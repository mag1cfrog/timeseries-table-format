use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use clap::{Args as ClapArgs, ValueEnum};
use serde::Serialize;
use serde_json::{Value, json};

use super::{
    ArtifactReport, BenchmarkReport, ColumnChecksums, Mode, RequiredNullable,
    TableDefinitionReport, TimingReport, ValidationReport, Workload, WriterPropertiesReport,
    benchmark_table_definition_report, invalid_data, writer_properties_report,
};

const GNU_TIME: &str = "/usr/bin/time";
const WARMUP_COUNT_PER_MODE: usize = 1;

#[derive(Clone, Copy, Debug, Serialize, ValueEnum)]
#[serde(rename_all = "kebab-case")]
enum WorkloadName {
    Smoke,
    LargeScale,
}

impl WorkloadName {
    fn name(self) -> &'static str {
        match self {
            Self::Smoke => "smoke",
            Self::LargeScale => "large-scale",
        }
    }

    fn workload(self) -> Workload {
        match self {
            Self::Smoke => Workload {
                row_count: 1_048_577,
                batch_rows: 262_144,
                row_group_rows: 1_048_576,
                payload_bytes: 1,
                seed: 20_260_821,
            },
            Self::LargeScale => Workload {
                row_count: 3_466_797,
                batch_rows: 8_192,
                row_group_rows: 1_048_576,
                payload_bytes: 1_024,
                seed: 20_260_821,
            },
        }
    }
}

#[derive(Debug, ClapArgs)]
pub(super) struct CompareArgs {
    #[arg(long, value_enum, default_value = "smoke")]
    workload: WorkloadName,

    #[arg(
        long,
        default_value_t = 3,
        value_parser = parse_sample_count,
        help = "Measured samples per mode; use 1 only for CI smoke coverage"
    )]
    samples: usize,

    /// Also write the combined JSON report to this path.
    #[arg(long)]
    json_out: Option<PathBuf>,

    /// Retain every generated table and external normalized Parquet file.
    #[arg(long)]
    keep_data: bool,
}

#[derive(Debug, Serialize)]
struct InvocationReport {
    mode: Mode,
    command: Vec<String>,
    peak_rss_bytes: RequiredNullable<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    repetition: Option<usize>,
    driver: BenchmarkReport,
}

#[derive(Debug, Serialize)]
struct MedianSummary {
    append_and_commit_wall_time_ns: f64,
    end_to_end_pipeline_wall_time_ns: f64,
    peak_rss_bytes: f64,
    table_managed_committed_parquet_bytes: f64,
    total_retained_ingestion_parquet_bytes: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    external_normalization_wall_time_ns: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    external_normalized_parquet_bytes: Option<f64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct RepositoryReport {
    commit_sha: String,
    worktree_dirty: bool,
}

#[derive(Debug, PartialEq, Eq)]
struct RepositorySnapshot {
    report: RepositoryReport,
    porcelain_status: String,
}

#[derive(Debug, PartialEq, Serialize)]
struct ComparableValidation<'a> {
    boundary_index_values_round_trip_as_uint64: bool,
    column_checksums: &'a ColumnChecksums,
    committed_parquet_schema_matches_registered: bool,
    coverage_ratio: f64,
    full_scan_matches_generated: bool,
    index_max: u64,
    index_min: u64,
    row_count: u64,
    schema_matches_registered: bool,
    segment_row_group_count: usize,
    table_managed_parquet_file_count: usize,
}

#[derive(Debug, PartialEq, Serialize)]
struct ComparableLogicalResult<'a> {
    committed_version: u64,
    workload: Workload,
    table_definition: &'a TableDefinitionReport,
    writer_properties: &'a WriterPropertiesReport,
    validation: ComparableValidation<'a>,
}

pub(super) fn run_comparison(args: &CompareArgs) -> Result<Value, Box<dyn std::error::Error>> {
    require_supported_environment()?;
    let binary = env::current_exe()?;
    let initial_repository = repository_snapshot()?;
    let workload = args.workload.workload();
    let temporary = tempfile::Builder::new()
        .prefix("tstable-append-widening-")
        .tempdir()?;
    let work_directory = temporary.path().to_path_buf();
    let cleanup_guard = if args.keep_data {
        let retained_directory = temporary.keep();
        debug_assert_eq!(retained_directory, work_directory);
        None
    } else {
        Some(temporary)
    };

    eprintln!("Running one untimed warm-up per mode");
    let mut warmups = Vec::with_capacity(2);
    for mode in [Mode::ExternalNormalization, Mode::DirectWidening] {
        let directory = work_directory.join("warmups").join(mode.name());
        warmups.push(run_invocation(
            &binary, mode, &directory, workload, false, None,
        )?);
        remove_completed_invocation(&cleanup_guard, &directory)?;
    }
    let reference = &warmups[0].driver;
    require_same_logical_result(reference, &warmups[1].driver)?;

    let execution_order = measured_execution_order(args.samples)?;
    let mut measured_samples = Vec::with_capacity(args.samples * 2);
    for (offset, modes) in execution_order.iter().enumerate() {
        let repetition = offset + 1;
        for &mode in modes {
            eprintln!("Running measured repetition {repetition}: {}", mode.name());
            let directory = work_directory
                .join("measured")
                .join(repetition.to_string())
                .join(mode.name());
            let sample =
                run_invocation(&binary, mode, &directory, workload, true, Some(repetition))?;
            remove_completed_invocation(&cleanup_guard, &directory)?;
            require_same_logical_result(reference, &sample.driver)?;
            measured_samples.push(sample);
        }
    }

    if repository_snapshot()? != initial_repository {
        return Err(invalid_data("Git repository changed during the comparison").into());
    }

    let medians = BTreeMap::from([
        (
            Mode::ExternalNormalization.name(),
            median_summary(&measured_samples, Mode::ExternalNormalization)?,
        ),
        (
            Mode::DirectWidening.name(),
            median_summary(&measured_samples, Mode::DirectWidening)?,
        ),
    ]);
    let logical_result = serde_json::to_value(comparable_logical_result(reference))?;
    let artifacts_directory = args.keep_data.then(|| work_directory.clone());
    let report = json!({
        "schema_version": 1,
        "repository": initial_repository.report,
        "benchmark": {
            "binary": binary,
            "build_profile": "release",
            "runner_command": process_arguments()?,
        },
        "environment": environment_metadata()?,
        "workload": {
            "name": args.workload.name(),
            "generated_payload_bytes": workload
                .row_count
                .checked_mul(u64::try_from(workload.payload_bytes)?)
                .ok_or_else(|| invalid_data("generated payload byte count overflow"))?,
            "generation": workload,
            "table_definition": reference.table_definition,
            "writer_properties": reference.writer_properties,
        },
        "sampling": {
            "warmup_count_per_mode": WARMUP_COUNT_PER_MODE,
            "measured_sample_count_per_mode": args.samples,
            "execution_order": {
                "warmups": [Mode::ExternalNormalization, Mode::DirectWidening],
                "measured": execution_order,
            },
        },
        "warmups": warmups,
        "measured_samples": measured_samples,
        "medians": medians,
        "validation": {
            "all_invocations_have_same_logical_result": true,
            "logical_result": logical_result,
        },
        "artifacts_directory": artifacts_directory,
    });

    if let Some(path) = &args.json_out {
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            fs::create_dir_all(parent)?;
        }
        fs::write(path, serde_json::to_vec_pretty(&report)?)?;
    }
    if args.keep_data {
        eprintln!("Kept benchmark data at {}", work_directory.display());
    }
    Ok(report)
}

fn parse_sample_count(raw: &str) -> Result<usize, String> {
    let count = raw
        .parse::<usize>()
        .map_err(|error| format!("invalid sample count: {error}"))?;
    match count {
        0 => Err("sample count must be positive".to_string()),
        2 => Err("use one sample for CI smoke coverage, or at least three samples".to_string()),
        _ => Ok(count),
    }
}

fn measured_execution_order(sample_count: usize) -> Result<Vec<[Mode; 2]>, io::Error> {
    if sample_count == 0 {
        return Err(invalid_data("sample count must be positive"));
    }
    Ok((0..sample_count)
        .map(|repetition| {
            if repetition.is_multiple_of(2) {
                [Mode::ExternalNormalization, Mode::DirectWidening]
            } else {
                [Mode::DirectWidening, Mode::ExternalNormalization]
            }
        })
        .collect())
}

fn remove_completed_invocation(
    cleanup_guard: &Option<tempfile::TempDir>,
    directory: &Path,
) -> Result<(), io::Error> {
    if cleanup_guard.is_some() {
        fs::remove_dir_all(directory)?;
    }
    Ok(())
}

fn run_invocation(
    binary: &Path,
    mode: Mode,
    directory: &Path,
    workload: Workload,
    measured: bool,
    repetition: Option<usize>,
) -> Result<InvocationReport, Box<dyn std::error::Error>> {
    fs::create_dir_all(directory)?;
    let table = directory.join("table");
    let external = (mode == Mode::ExternalNormalization)
        .then(|| directory.join("external").join("normalized.parquet"));
    let child_arguments = child_arguments(mode, &table, external.as_deref(), workload)?;
    let timing_path = directory.join("gnu-time.txt");
    let mut command = if measured {
        vec![
            GNU_TIME.to_string(),
            "-v".to_string(),
            "-o".to_string(),
            path_as_utf8(&timing_path)?,
            path_as_utf8(binary)?,
        ]
    } else {
        vec![path_as_utf8(binary)?]
    };
    command.extend(child_arguments);

    let output = execute_command(&command, &[("LC_ALL", "C")])?;
    let report: BenchmarkReport = serde_json::from_slice(&output.stdout).map_err(|error| {
        invalid_data(format!(
            "malformed benchmark JSON from {}: {error}",
            command.join(" ")
        ))
    })?;
    validate_benchmark_report(&report, mode, workload, &table, external.as_deref())?;
    let peak_rss_bytes = if measured {
        RequiredNullable::Value(parse_peak_rss_bytes(&fs::read_to_string(&timing_path)?)?)
    } else {
        RequiredNullable::Null
    };

    Ok(InvocationReport {
        mode,
        command,
        peak_rss_bytes,
        repetition,
        driver: report,
    })
}

fn child_arguments(
    mode: Mode,
    table: &Path,
    external: Option<&Path>,
    workload: Workload,
) -> Result<Vec<String>, io::Error> {
    let mut arguments = vec![
        "run".to_string(),
        "--mode".to_string(),
        mode.name().to_string(),
        "--table".to_string(),
        path_as_utf8(table)?,
        "--row-count".to_string(),
        workload.row_count.to_string(),
        "--batch-rows".to_string(),
        workload.batch_rows.to_string(),
        "--row-group-rows".to_string(),
        workload.row_group_rows.to_string(),
        "--payload-bytes".to_string(),
        workload.payload_bytes.to_string(),
        "--seed".to_string(),
        workload.seed.to_string(),
    ];
    if let Some(path) = external {
        arguments.extend([
            "--external-normalized-parquet".to_string(),
            path_as_utf8(path)?,
        ]);
    }
    Ok(arguments)
}

fn execute_command(
    command: &[String],
    environment: &[(&str, &str)],
) -> Result<Output, Box<dyn std::error::Error>> {
    let (program, arguments) = command
        .split_first()
        .ok_or_else(|| invalid_data("cannot execute an empty command"))?;
    let mut process = Command::new(program);
    process.args(arguments);
    for &(key, value) in environment {
        process.env(key, value);
    }
    let output = process.output()?;
    io::stderr().write_all(&output.stderr)?;
    if !output.status.success() {
        let detail = if output.stderr.is_empty() {
            String::from_utf8_lossy(&output.stdout)
        } else {
            String::from_utf8_lossy(&output.stderr)
        };
        return Err(invalid_data(format!(
            "command failed ({}): {}",
            command.join(" "),
            detail.trim()
        ))
        .into());
    }
    Ok(output)
}

pub(super) fn validate_benchmark_report(
    report: &BenchmarkReport,
    mode: Mode,
    workload: Workload,
    table: &Path,
    external: Option<&Path>,
) -> Result<(), io::Error> {
    if report.schema_version != 1
        || report.mode != mode
        || report.process_id == 0
        || report.committed_version != 2
        || report.workload != workload
        || report.table_definition != benchmark_table_definition_report()
        || report.table_path != table
    {
        return Err(invalid_data(
            "benchmark report identity does not match the requested invocation",
        ));
    }
    if report.writer_properties != writer_properties_report(workload.row_group_rows)? {
        return Err(invalid_data(
            "benchmark writer properties do not match the workload",
        ));
    }
    if report
        .external_normalized_parquet_path
        .as_ref()
        .map(PathBuf::as_path)
        != external
    {
        return Err(invalid_data(
            "benchmark external Parquet path does not match the invocation",
        ));
    }

    validate_timing(&report.timing, mode)?;
    validate_artifacts(&report.artifacts, &report.validation, mode)?;
    validate_reported_table_result(&report.validation, workload, mode)?;
    Ok(())
}

fn validate_timing(timing: &TimingReport, mode: Mode) -> Result<(), io::Error> {
    if timing.append_and_commit_wall_time_ns == 0 || timing.end_to_end_pipeline_wall_time_ns == 0 {
        return Err(invalid_data("benchmark reported a zero duration"));
    }
    let normalization_ns = match (mode, timing.external_normalization_wall_time_ns.copied()) {
        (Mode::ExternalNormalization, Some(value)) if value > 0 => value,
        (Mode::DirectWidening, None) => 0,
        _ => return Err(invalid_data("benchmark reported invalid phase timing")),
    };
    let measured_ns = normalization_ns
        .checked_add(timing.append_and_commit_wall_time_ns)
        .ok_or_else(|| invalid_data("benchmark phase timing overflow"))?;
    if timing.end_to_end_pipeline_wall_time_ns < measured_ns {
        return Err(invalid_data(
            "end-to-end timing is shorter than its measured phases",
        ));
    }
    Ok(())
}

fn validate_artifacts(
    artifacts: &ArtifactReport,
    validation: &ValidationReport,
    mode: Mode,
) -> Result<(), io::Error> {
    if artifacts.table_managed_committed_parquet_bytes == 0
        || artifacts.table_managed_committed_parquet_bytes != validation.segment_file_bytes
    {
        return Err(invalid_data(
            "table-managed artifact bytes do not match validation",
        ));
    }
    let expected_retained = match (mode, artifacts.external_normalized_parquet_bytes.copied()) {
        (Mode::ExternalNormalization, Some(value)) if value > 0 => artifacts
            .table_managed_committed_parquet_bytes
            .checked_add(value)
            .ok_or_else(|| invalid_data("retained artifact byte count overflow"))?,
        (Mode::DirectWidening, None) => artifacts.table_managed_committed_parquet_bytes,
        _ => {
            return Err(invalid_data(
                "benchmark reported invalid external artifact bytes",
            ));
        }
    };
    if artifacts.total_retained_ingestion_parquet_bytes != expected_retained {
        return Err(invalid_data(
            "total retained artifact bytes do not match their components",
        ));
    }
    Ok(())
}

fn validate_reported_table_result(
    validation: &ValidationReport,
    workload: Workload,
    mode: Mode,
) -> Result<(), io::Error> {
    let expected_row_groups = workload.row_count.div_ceil(workload.row_group_rows as u64) as usize;
    if validation.coverage_ratio != 1.0
        || validation.index_min != 0
        || validation.index_max != u64::from(u32::MAX)
        || validation.row_count != workload.row_count
        || validation.segment_row_group_count != expected_row_groups
        || expected_row_groups <= 1
        || validation.table_managed_parquet_file_count != 1
        || validation.segment_path.is_empty()
        || !validation.boundary_index_values_round_trip_as_uint64
        || !validation.committed_parquet_schema_matches_registered
        || !validation.full_scan_matches_generated
        || !validation.schema_matches_registered
    {
        return Err(invalid_data("benchmark table validation is incomplete"));
    }
    match mode {
        Mode::ExternalNormalization
            if !validation.direct_pipeline_has_no_external_normalized_parquet
                && validation
                    .external_normalized_parquet_schema_matches_registered
                    .copied()
                    == Some(true) => {}
        Mode::DirectWidening
            if validation.direct_pipeline_has_no_external_normalized_parquet
                && validation
                    .external_normalized_parquet_schema_matches_registered
                    .as_ref()
                    .is_none() => {}
        _ => return Err(invalid_data("benchmark mode validation is inconsistent")),
    }
    if ![
        &validation.column_checksums.ordered_index,
        &validation.column_checksums.sequence,
        &validation.column_checksums.payload,
    ]
    .into_iter()
    .all(|checksum| {
        checksum.len() == 64
            && checksum
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    }) {
        return Err(invalid_data("benchmark column checksum is invalid"));
    }
    Ok(())
}

fn comparable_logical_result(report: &BenchmarkReport) -> ComparableLogicalResult<'_> {
    ComparableLogicalResult {
        committed_version: report.committed_version,
        workload: report.workload,
        table_definition: &report.table_definition,
        writer_properties: &report.writer_properties,
        validation: ComparableValidation {
            boundary_index_values_round_trip_as_uint64: report
                .validation
                .boundary_index_values_round_trip_as_uint64,
            column_checksums: &report.validation.column_checksums,
            committed_parquet_schema_matches_registered: report
                .validation
                .committed_parquet_schema_matches_registered,
            coverage_ratio: report.validation.coverage_ratio,
            full_scan_matches_generated: report.validation.full_scan_matches_generated,
            index_max: report.validation.index_max,
            index_min: report.validation.index_min,
            row_count: report.validation.row_count,
            schema_matches_registered: report.validation.schema_matches_registered,
            segment_row_group_count: report.validation.segment_row_group_count,
            table_managed_parquet_file_count: report.validation.table_managed_parquet_file_count,
        },
    }
}

pub(super) fn require_same_logical_result(
    reference: &BenchmarkReport,
    candidate: &BenchmarkReport,
) -> Result<(), io::Error> {
    if comparable_logical_result(reference) != comparable_logical_result(candidate) {
        return Err(invalid_data(
            "benchmark invocations produced different logical results",
        ));
    }
    Ok(())
}

fn median_summary(samples: &[InvocationReport], mode: Mode) -> Result<MedianSummary, io::Error> {
    let selected = samples
        .iter()
        .filter(|sample| sample.mode == mode)
        .collect::<Vec<_>>();
    let peak_rss = selected
        .iter()
        .map(|sample| sample.peak_rss_bytes.copied())
        .collect::<Option<Vec<_>>>()
        .ok_or_else(|| invalid_data("measured sample omitted peak RSS"))?;
    let external_normalization = (mode == Mode::ExternalNormalization)
        .then(|| {
            selected
                .iter()
                .map(|sample| {
                    sample
                        .driver
                        .timing
                        .external_normalization_wall_time_ns
                        .copied()
                })
                .collect::<Option<Vec<_>>>()
                .ok_or_else(|| invalid_data("external sample omitted normalization timing"))
                .and_then(median)
        })
        .transpose()?;
    let external_bytes = (mode == Mode::ExternalNormalization)
        .then(|| {
            selected
                .iter()
                .map(|sample| {
                    sample
                        .driver
                        .artifacts
                        .external_normalized_parquet_bytes
                        .copied()
                })
                .collect::<Option<Vec<_>>>()
                .ok_or_else(|| invalid_data("external sample omitted normalized Parquet bytes"))
                .and_then(median)
        })
        .transpose()?;

    Ok(MedianSummary {
        append_and_commit_wall_time_ns: median(
            selected
                .iter()
                .map(|sample| sample.driver.timing.append_and_commit_wall_time_ns)
                .collect(),
        )?,
        end_to_end_pipeline_wall_time_ns: median(
            selected
                .iter()
                .map(|sample| sample.driver.timing.end_to_end_pipeline_wall_time_ns)
                .collect(),
        )?,
        peak_rss_bytes: median(peak_rss)?,
        table_managed_committed_parquet_bytes: median(
            selected
                .iter()
                .map(|sample| {
                    sample
                        .driver
                        .artifacts
                        .table_managed_committed_parquet_bytes
                })
                .collect(),
        )?,
        total_retained_ingestion_parquet_bytes: median(
            selected
                .iter()
                .map(|sample| {
                    sample
                        .driver
                        .artifacts
                        .total_retained_ingestion_parquet_bytes
                })
                .collect(),
        )?,
        external_normalization_wall_time_ns: external_normalization,
        external_normalized_parquet_bytes: external_bytes,
    })
}

fn median(mut values: Vec<u64>) -> Result<f64, io::Error> {
    if values.is_empty() {
        return Err(invalid_data("median requires at least one value"));
    }
    values.sort_unstable();
    let midpoint = values.len() / 2;
    if values.len().is_multiple_of(2) {
        Ok(values[midpoint - 1] as f64 / 2.0 + values[midpoint] as f64 / 2.0)
    } else {
        Ok(values[midpoint] as f64)
    }
}

fn parse_peak_rss_bytes(time_output: &str) -> Result<u64, io::Error> {
    const LABEL: &str = "Maximum resident set size (kbytes)";
    for line in time_output.lines() {
        let Some((label, raw_value)) = line.split_once(':') else {
            continue;
        };
        if label.trim() != LABEL {
            continue;
        }
        let kib = raw_value
            .trim()
            .parse::<u64>()
            .map_err(|error| invalid_data(format!("invalid {LABEL}: {error}")))?;
        if kib == 0 {
            return Err(invalid_data(format!("invalid {LABEL}: 0")));
        }
        return kib
            .checked_mul(1_024)
            .ok_or_else(|| invalid_data("peak RSS byte count overflow"));
    }
    Err(invalid_data(format!("missing {LABEL}")))
}

fn require_supported_environment() -> Result<(), io::Error> {
    if env::consts::OS != "linux" {
        return Err(invalid_data("append widening comparison requires Linux"));
    }
    if !Path::new(GNU_TIME).is_file() {
        return Err(invalid_data(
            "append widening comparison requires /usr/bin/time",
        ));
    }
    if cfg!(debug_assertions) {
        return Err(invalid_data(
            "append widening comparison requires a release-mode executable",
        ));
    }
    Ok(())
}

fn repository_snapshot() -> Result<RepositorySnapshot, Box<dyn std::error::Error>> {
    let root = PathBuf::from(
        checked_stdout("git", &["rev-parse", "--show-toplevel"], None)?
            .trim()
            .to_string(),
    );
    let commit_sha = checked_stdout("git", &["rev-parse", "HEAD"], Some(&root))?;
    let porcelain_status = checked_stdout(
        "git",
        &["status", "--porcelain=v1", "--untracked-files=all"],
        Some(&root),
    )?;
    Ok(RepositorySnapshot {
        report: RepositoryReport {
            commit_sha: commit_sha.trim().to_string(),
            worktree_dirty: !porcelain_status.trim().is_empty(),
        },
        porcelain_status,
    })
}

fn environment_metadata() -> Result<Value, Box<dyn std::error::Error>> {
    Ok(json!({
        "operating_system": env::consts::OS,
        "kernel": checked_stdout("uname", &["-r"], None)?.trim(),
        "architecture": env::consts::ARCH,
        "cpu_count": std::thread::available_parallelism()?.get(),
        "available_memory_bytes": available_memory_bytes()?,
        "benchmark_process_environment": {"LC_ALL": "C"},
        "rustc_version": checked_stdout("rustc", &["--version"], None)?.trim(),
    }))
}

fn available_memory_bytes() -> Result<u64, io::Error> {
    for line in fs::read_to_string("/proc/meminfo")?.lines() {
        let Some(value) = line.strip_prefix("MemAvailable:") else {
            continue;
        };
        let mut parts = value.split_whitespace();
        let kib = parts
            .next()
            .ok_or_else(|| invalid_data("MemAvailable omitted its value"))?
            .parse::<u64>()
            .map_err(|error| invalid_data(format!("invalid MemAvailable: {error}")))?;
        if parts.next() != Some("kB") || parts.next().is_some() {
            return Err(invalid_data("MemAvailable has an unexpected unit"));
        }
        return kib
            .checked_mul(1_024)
            .ok_or_else(|| invalid_data("available memory byte count overflow"));
    }
    Err(invalid_data("missing MemAvailable in /proc/meminfo"))
}

fn checked_stdout(
    program: &str,
    arguments: &[&str],
    directory: Option<&Path>,
) -> Result<String, Box<dyn std::error::Error>> {
    let mut command = Command::new(program);
    command.args(arguments);
    if let Some(path) = directory {
        command.current_dir(path);
    }
    let output = command.output()?;
    if !output.status.success() {
        return Err(invalid_data(format!(
            "command failed ({program} {}): {}",
            arguments.join(" "),
            String::from_utf8_lossy(&output.stderr).trim()
        ))
        .into());
    }
    Ok(String::from_utf8(output.stdout)?)
}

fn process_arguments() -> Result<Vec<String>, io::Error> {
    env::args_os()
        .map(|argument| {
            argument
                .into_string()
                .map_err(|_| invalid_data("benchmark argument is not valid UTF-8"))
        })
        .collect()
}

fn path_as_utf8(path: &Path) -> Result<String, io::Error> {
    path.to_str()
        .map(str::to_string)
        .ok_or_else(|| invalid_data("benchmark path is not valid UTF-8"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_peak_rss_and_rejects_malformed_output() {
        assert_eq!(
            parse_peak_rss_bytes("Maximum resident set size (kbytes): 2048").unwrap(),
            2 * 1_024 * 1_024
        );
        for output in [
            "no RSS here",
            "Maximum resident set size (kbytes): nope",
            "Maximum resident set size (kbytes): 0",
        ] {
            assert!(parse_peak_rss_bytes(output).is_err());
        }
    }

    #[test]
    fn calculates_medians_and_alternates_execution_order() {
        assert_eq!(median(vec![9, 1, 5]).unwrap(), 5.0);
        assert_eq!(median(vec![9, 1, 5, 3]).unwrap(), 4.0);
        assert!(median(Vec::new()).is_err());
        assert_eq!(
            measured_execution_order(3).unwrap(),
            vec![
                [Mode::ExternalNormalization, Mode::DirectWidening],
                [Mode::DirectWidening, Mode::ExternalNormalization],
                [Mode::ExternalNormalization, Mode::DirectWidening],
            ]
        );
        assert_eq!(parse_sample_count("1").unwrap(), 1);
        assert_eq!(parse_sample_count("3").unwrap(), 3);
        assert!(parse_sample_count("2").is_err());
    }

    #[test]
    fn propagates_subprocess_failures() {
        let command = vec![
            "rustc".to_string(),
            "--definitely-invalid-option".to_string(),
        ];
        let error = execute_command(&command, &[]).unwrap_err();
        assert!(error.to_string().contains("command failed"));
    }

    #[test]
    fn required_nullable_fields_cannot_be_omitted() {
        #[derive(Debug, serde::Deserialize)]
        struct Contract {
            value: RequiredNullable<u64>,
        }

        assert_eq!(
            serde_json::from_str::<Contract>(r#"{"value":null}"#)
                .unwrap()
                .value,
            RequiredNullable::Null
        );
        assert!(serde_json::from_str::<Contract>("{}").is_err());
    }
}
