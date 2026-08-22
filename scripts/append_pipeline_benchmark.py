#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import platform
import shlex
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from statistics import median as stdlib_median

from scan_range_rss_regression import (
    GNU_TIME,
    REPO_ROOT,
    parse_max_rss_bytes,
    positive_int,
    repository_state,
    write_summary,
)

MODES = ("path-first", "streaming")
WARMUP_COUNT_PER_MODE = 1
EXPECTED_TABLE_DEFINITION = {
    "index_column": "ts",
    "index_type": "int64",
    "bucket_width": 1,
    "entity_columns": [],
}
WORKLOADS: dict[str, dict[str, int]] = {
    "smoke": {
        "row_count": 1_048_577,
        "batch_rows": 262_144,
        "payload_bytes_per_row": 1,
        "seed": 20_260_821,
    },
    "cs2-scale": {
        "row_count": 3_466_797,
        "batch_rows": 8_192,
        "payload_bytes_per_row": 1_024,
        "seed": 20_260_821,
    },
}


def calculate_median(values: list[int]) -> int | float:
    if not values or any(isinstance(value, bool) or value < 0 for value in values):
        raise ValueError("median requires non-negative integer values")
    return stdlib_median(values)


def measured_execution_order(sample_count: int) -> list[list[str]]:
    if sample_count <= 0:
        raise ValueError("sample count must be positive")
    return [
        list(MODES if repetition % 2 == 0 else reversed(MODES))
        for repetition in range(sample_count)
    ]


def sample_count(raw: str) -> int:
    value = positive_int(raw)
    if value == 2:
        raise argparse.ArgumentTypeError(
            "use one sample only for CI smoke coverage, or at least three samples"
        )
    return value


def required_mapping(report: dict[str, object], key: str) -> dict[str, object]:
    value = report.get(key)
    if not isinstance(value, dict):
        raise ValueError(f"missing or invalid {key}")
    return value


def required_integer(
    report: dict[str, object], key: str, *, positive: bool = False
) -> int:
    value = report.get(key)
    minimum = 1 if positive else 0
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ValueError(f"missing or invalid {key}: {value!r}")
    return value


def require_duration_or_null(
    timing: dict[str, object], key: str, *, present: bool
) -> int | None:
    value = timing.get(key)
    if not present:
        if value is not None:
            raise ValueError(f"expected {key} to be null, found {value!r}")
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"missing or invalid {key}: {value!r}")
    return value


def validate_driver_report(
    report: dict[str, object],
    mode: str,
    workload: dict[str, int],
    *,
    expected_table: Path | None = None,
    expected_external_parquet: Path | None = None,
) -> None:
    if (
        isinstance(report.get("schema_version"), bool)
        or report.get("schema_version") != 1
    ):
        raise ValueError("unsupported or missing driver schema_version")
    if report.get("mode") != mode:
        raise ValueError(f"expected {mode} report, found {report.get('mode')!r}")
    required_integer(report, "process_id", positive=True)
    required_integer(report, "committed_version", positive=True)

    if required_mapping(report, "workload") != workload:
        raise ValueError("driver workload does not match the requested parameters")
    if required_mapping(report, "table_definition") != EXPECTED_TABLE_DEFINITION:
        raise ValueError(
            "driver table_definition does not match the benchmark contract"
        )
    writer_properties = required_mapping(report, "writer_properties")
    for key in ("compression", "statistics", "writer_version"):
        if (
            not isinstance(writer_properties.get(key), str)
            or not writer_properties[key]
        ):
            raise ValueError(f"driver writer_properties has invalid {key}")
    for key in ("data_page_size_bytes", "max_row_group_rows", "write_batch_rows"):
        required_integer(writer_properties, key, positive=True)
    if not isinstance(writer_properties.get("dictionary_enabled"), bool):
        raise ValueError("driver writer_properties has invalid dictionary_enabled")

    table_path = report.get("table_path")
    if not isinstance(table_path, str) or not table_path:
        raise ValueError("missing or invalid table_path")
    if expected_table is not None and Path(table_path) != expected_table:
        raise ValueError("driver table_path does not match the requested path")
    external_path = report.get("external_parquet_path")
    if mode == "path-first":
        if not isinstance(external_path, str) or not external_path:
            raise ValueError("path-first report is missing external_parquet_path")
        if (
            expected_external_parquet is not None
            and Path(external_path) != expected_external_parquet
        ):
            raise ValueError(
                "driver external_parquet_path does not match the requested path"
            )
    elif external_path is not None:
        raise ValueError("streaming report must not contain external_parquet_path")

    timing = required_mapping(report, "timing")
    pipeline_ns = required_integer(timing, "end_to_end_pipeline_ns", positive=True)
    generation_ns = require_duration_or_null(
        timing, "external_parquet_generation_ns", present=mode == "path-first"
    )
    path_append_ns = require_duration_or_null(
        timing, "path_append_copy_commit_ns", present=mode == "path-first"
    )
    streaming_append_ns = require_duration_or_null(
        timing, "streaming_append_ns", present=mode == "streaming"
    )
    measured_ns = (
        generation_ns + path_append_ns
        if generation_ns is not None and path_append_ns is not None
        else streaming_append_ns
    )
    if measured_ns is None or pipeline_ns < measured_ns:
        raise ValueError("end-to-end timing is shorter than its measured phases")

    artifacts = required_mapping(report, "artifacts")
    segment_bytes = required_integer(
        artifacts, "table_owned_segment_bytes", positive=True
    )
    retained_bytes = required_integer(
        artifacts, "total_retained_ingestion_bytes", positive=True
    )
    external_bytes = artifacts.get("external_source_parquet_bytes")
    if mode == "path-first":
        if (
            isinstance(external_bytes, bool)
            or not isinstance(external_bytes, int)
            or external_bytes <= 0
        ):
            raise ValueError("path-first report has invalid external source bytes")
        if retained_bytes != segment_bytes + external_bytes:
            raise ValueError("path-first retained bytes do not equal both artifacts")
    elif external_bytes is not None or retained_bytes != segment_bytes:
        raise ValueError("streaming retained bytes do not equal its table segment")

    validation = required_mapping(report, "validation")
    if validation.get("schema_matches_generated") is not True:
        raise ValueError("driver did not validate the generated schema")
    if validation.get("ordered_full_scan_matches_generated") is not True:
        raise ValueError("driver did not validate the ordered full scan")
    if (
        isinstance(validation.get("coverage_ratio"), bool)
        or validation.get("coverage_ratio") != 1.0
    ):
        raise ValueError("driver did not validate complete table coverage")
    if required_integer(validation, "row_count") != workload["row_count"]:
        raise ValueError("committed row count does not match the workload")
    if required_integer(validation, "index_min") != 0:
        raise ValueError("committed index_min is not zero")
    if required_integer(validation, "index_max") != workload["row_count"] - 1:
        raise ValueError("committed index_max does not match the workload")
    if (
        required_integer(validation, "segment_file_bytes", positive=True)
        != segment_bytes
    ):
        raise ValueError("validation and artifact segment byte counts differ")
    if required_integer(validation, "segment_row_group_count", positive=True) <= 1:
        raise ValueError("benchmark workload did not produce multiple row groups")
    if (
        not isinstance(validation.get("segment_path"), str)
        or not validation["segment_path"]
    ):
        raise ValueError("validation is missing segment_path")

    checksums = required_mapping(validation, "column_checksums")
    if set(checksums) != {"ts", "sequence", "payload"} or any(
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
        for value in checksums.values()
    ):
        raise ValueError("validation has missing or invalid column checksums")


def logical_result(report: dict[str, object]) -> dict[str, object]:
    validation = required_mapping(report, "validation")
    return {
        "committed_version": report["committed_version"],
        "workload": report["workload"],
        "table_definition": report["table_definition"],
        "writer_properties": report["writer_properties"],
        "validation": {
            key: validation[key]
            for key in (
                "column_checksums",
                "coverage_ratio",
                "index_max",
                "index_min",
                "ordered_full_scan_matches_generated",
                "row_count",
                "schema_matches_generated",
                "segment_row_group_count",
            )
        },
    }


def require_equivalent_results(
    path_first: dict[str, object], streaming: dict[str, object]
) -> None:
    if logical_result(path_first) != logical_result(streaming):
        raise RuntimeError("path-first and streaming logical results differ")


def parse_driver_stdout(stdout: str, command: list[str]) -> dict[str, object]:
    try:
        report = json.loads(stdout)
    except json.JSONDecodeError as error:
        raise ValueError(f"malformed driver JSON from {shlex.join(command)}") from error
    if not isinstance(report, dict):
        raise ValueError(f"expected driver JSON object from {shlex.join(command)}")
    return report


def run_process(command: list[str], env: dict[str, str]) -> dict[str, object]:
    result = subprocess.run(command, text=True, capture_output=True, env=env)
    if result.stderr:
        sys.stderr.write(result.stderr)
    if result.returncode:
        detail = result.stderr.strip() or result.stdout.strip() or "no command output"
        raise RuntimeError(f"command failed ({shlex.join(command)}): {detail}")
    return parse_driver_stdout(result.stdout, command)


def driver_command(
    binary: Path, mode: str, directory: Path, workload: dict[str, int]
) -> tuple[list[str], Path, Path | None]:
    table = directory / "table"
    external = (
        directory / "external" / "source.parquet" if mode == "path-first" else None
    )
    command = [
        str(binary),
        "--mode",
        mode,
        "--table",
        str(table),
        "--row-count",
        str(workload["row_count"]),
        "--batch-rows",
        str(workload["batch_rows"]),
        "--payload-bytes",
        str(workload["payload_bytes_per_row"]),
        "--seed",
        str(workload["seed"]),
    ]
    if external is not None:
        command.extend(["--external-parquet", str(external)])
    return command, table, external


def run_invocation(
    binary: Path,
    mode: str,
    directory: Path,
    workload: dict[str, int],
    *,
    measured: bool,
) -> dict[str, object]:
    directory.mkdir(parents=True)
    command, table, external = driver_command(binary, mode, directory, workload)
    env = os.environ.copy()
    env["LC_ALL"] = "C"
    timing_path = directory / "gnu-time.txt"
    executed_command = (
        [str(GNU_TIME), "-v", "-o", str(timing_path), *command] if measured else command
    )
    report = run_process(executed_command, env)
    validate_driver_report(
        report,
        mode,
        workload,
        expected_table=table,
        expected_external_parquet=external,
    )
    return {
        "mode": mode,
        "command": shlex.join(executed_command),
        "peak_rss_bytes": (
            parse_max_rss_bytes(timing_path.read_text(encoding="utf-8"))
            if measured
            else None
        ),
        "driver": report,
    }


def median_summary(
    samples: list[dict[str, object]], mode: str
) -> dict[str, int | float]:
    selected = [sample for sample in samples if sample["mode"] == mode]

    def values(section: str, field: str) -> list[int]:
        result: list[int] = []
        for sample in selected:
            if section == "sample":
                value = sample[field]
            else:
                driver = required_mapping(sample, "driver")
                value = required_mapping(driver, section)[field]
            if isinstance(value, bool) or not isinstance(value, int):
                raise ValueError(f"invalid measured {field}: {value!r}")
            result.append(value)
        return result

    summary = {
        "end_to_end_pipeline_ns": calculate_median(
            values("timing", "end_to_end_pipeline_ns")
        ),
        "peak_rss_bytes": calculate_median(values("sample", "peak_rss_bytes")),
        "table_owned_segment_bytes": calculate_median(
            values("artifacts", "table_owned_segment_bytes")
        ),
        "total_retained_ingestion_bytes": calculate_median(
            values("artifacts", "total_retained_ingestion_bytes")
        ),
    }
    if mode == "path-first":
        summary.update(
            {
                "external_parquet_generation_ns": calculate_median(
                    values("timing", "external_parquet_generation_ns")
                ),
                "path_append_copy_commit_ns": calculate_median(
                    values("timing", "path_append_copy_commit_ns")
                ),
                "external_source_parquet_bytes": calculate_median(
                    values("artifacts", "external_source_parquet_bytes")
                ),
            }
        )
    else:
        summary["streaming_append_ns"] = calculate_median(
            values("timing", "streaming_append_ns")
        )
    return summary


def require_linux_dependencies() -> None:
    if not sys.platform.startswith("linux"):
        raise RuntimeError("append pipeline benchmark requires Linux")
    if not GNU_TIME.is_file():
        raise RuntimeError("append pipeline benchmark requires /usr/bin/time")


def resolve_benchmark(explicit_path: Path | None) -> tuple[Path, str, str]:
    if explicit_path is None:
        command = [
            "cargo",
            "build",
            "--locked",
            "--release",
            "-p",
            "timeseries-table-format",
            "--features",
            "cli",
            "--example",
            "append_pipeline_bench",
        ]
        subprocess.run(command, cwd=REPO_ROOT, check=True)
        metadata = json.loads(
            subprocess.check_output(
                ["cargo", "metadata", "--format-version", "1", "--no-deps"],
                cwd=REPO_ROOT,
                text=True,
            )
        )
        binary = (
            Path(metadata["target_directory"])
            / "release"
            / "examples"
            / "append_pipeline_bench"
        )
        profile = "release"
        build_command = shlex.join(command)
    else:
        binary = explicit_path.expanduser().resolve()
        parent = binary.parent
        profile = parent.parent.name if parent.name == "examples" else "external"
        if profile == "debug":
            raise ValueError("--benchmark must be a release-mode binary")
        build_command = f"prebuilt binary: {binary}"

    if not binary.is_file() or not os.access(binary, os.X_OK):
        raise RuntimeError(f"benchmark binary is not executable: {binary}")
    return binary, profile, build_command


def available_memory_bytes() -> int:
    for line in Path("/proc/meminfo").read_text(encoding="utf-8").splitlines():
        if line.startswith("MemAvailable:"):
            parts = line.split()
            if len(parts) == 3 and parts[2] == "kB":
                return int(parts[1]) * 1024
    raise RuntimeError("could not read MemAvailable from /proc/meminfo")


def environment_metadata() -> dict[str, object]:
    cpu_count = (
        len(os.sched_getaffinity(0))
        if hasattr(os, "sched_getaffinity")
        else os.cpu_count() or 1
    )
    return {
        "operating_system": platform.platform(),
        "kernel": platform.release(),
        "architecture": platform.machine(),
        "cpu_count": cpu_count,
        "available_memory_bytes": available_memory_bytes(),
        "benchmark_process_environment": {"LC_ALL": "C"},
        "rustc_version": subprocess.check_output(
            ["rustc", "--version"], text=True
        ).strip(),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare path-first and streaming append pipelines."
    )
    parser.add_argument(
        "--benchmark", type=Path, help="existing release append_pipeline_bench binary"
    )
    parser.add_argument("--workload", choices=WORKLOADS, default="smoke")
    parser.add_argument(
        "--samples",
        type=sample_count,
        default=3,
        help="measured samples per mode (use 1 only for CI smoke coverage)",
    )
    parser.add_argument("--json-out", type=Path, help="also write the JSON report here")
    parser.add_argument(
        "--keep-data", action="store_true", help="retain generated benchmark artifacts"
    )
    return parser.parse_args()


def run_benchmark(args: argparse.Namespace) -> None:
    require_linux_dependencies()
    commit_sha, worktree_dirty = repository_state()
    provenance = (
        "prebuilt" if args.benchmark is not None else "repository_release_build"
    )
    if provenance == "repository_release_build" and worktree_dirty:
        raise RuntimeError("default recorded comparison requires a clean Git worktree")

    binary, build_profile, build_command = resolve_benchmark(args.benchmark)
    workload = dict(WORKLOADS[args.workload])
    work_dir = Path(tempfile.mkdtemp(prefix="tstable-append-pipeline-"))
    try:
        warmups: list[dict[str, object]] = []
        print("Running one untimed warm-up per mode", file=sys.stderr)
        for mode in MODES:
            warmups.append(
                run_invocation(
                    binary,
                    mode,
                    work_dir / "warmups" / mode,
                    workload,
                    measured=False,
                )
            )
        require_equivalent_results(
            required_mapping(warmups[0], "driver"),
            required_mapping(warmups[1], "driver"),
        )

        measured_samples: list[dict[str, object]] = []
        order = measured_execution_order(args.samples)
        for repetition, modes in enumerate(order, start=1):
            pair: dict[str, dict[str, object]] = {}
            for mode in modes:
                print(
                    f"Running measured repetition {repetition}: {mode}", file=sys.stderr
                )
                sample = run_invocation(
                    binary,
                    mode,
                    work_dir / "measured" / str(repetition) / mode,
                    workload,
                    measured=True,
                )
                sample["repetition"] = repetition
                measured_samples.append(sample)
                pair[mode] = required_mapping(sample, "driver")
            require_equivalent_results(pair["path-first"], pair["streaming"])

        final_commit_sha, final_worktree_dirty = repository_state()
        if (final_commit_sha, final_worktree_dirty) != (commit_sha, worktree_dirty):
            raise RuntimeError("Git worktree changed during the recorded comparison")

        reference = required_mapping(measured_samples[0], "driver")
        report: dict[str, object] = {
            "schema_version": 1,
            "repository": {
                "commit_sha": commit_sha,
                "worktree_dirty": worktree_dirty,
            },
            "benchmark": {
                "binary": str(binary),
                "build_profile": build_profile,
                "provenance": provenance,
                "build_command": build_command,
            },
            "environment": environment_metadata(),
            "workload": {
                "name": args.workload,
                "generated_payload_bytes": (
                    workload["row_count"] * workload["payload_bytes_per_row"]
                ),
                "generation": workload,
                "table_definition": reference["table_definition"],
                "writer_properties": reference["writer_properties"],
            },
            "sampling": {
                "warmup_count_per_mode": WARMUP_COUNT_PER_MODE,
                "measured_sample_count_per_mode": args.samples,
                "execution_order": {
                    "warmups": list(MODES),
                    "measured": order,
                },
            },
            "warmups": warmups,
            "measured_samples": measured_samples,
            "medians": {mode: median_summary(measured_samples, mode) for mode in MODES},
            "validation": {
                "all_mode_pairs_equivalent": True,
                "logical_result": logical_result(reference),
            },
            "artifacts_directory": str(work_dir) if args.keep_data else None,
        }
        write_summary(report, args.json_out)
    finally:
        if args.keep_data:
            print(f"Kept benchmark data at {work_dir}", file=sys.stderr)
        else:
            shutil.rmtree(work_dir)


def main() -> int:
    args = parse_args()
    try:
        run_benchmark(args)
    except (
        KeyError,
        OSError,
        RuntimeError,
        TypeError,
        ValueError,
        subprocess.CalledProcessError,
    ) as error:
        sys.stderr.write(f"append pipeline benchmark failed: {error}\n")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
