# Benchmark helpers

These helpers run reproducible process checks and common `coverage_bench`
parameter sweeps.

## Compare path-first and streaming append

Use `scripts/append_pipeline_benchmark.py` to compare the complete legacy
path-first ingestion pipeline with `TimeSeriesTable::append`. Both modes
generate the same deterministic Arrow batches and commit them to fresh local
tables in separate processes.

The path-first pipeline generates an external Parquet file and then appends
it. The streaming pipeline passes the same lazy batches directly to the table.
The comparison therefore includes external Parquet generation in the
path-first end-to-end time. It also reports Parquet generation and
append/copy/commit as separate phases.

### Requirements

The runner requires Linux, GNU `/usr/bin/time`, Python 3, Rust, Cargo, and
temporary space for every warm-up and measured invocation. The default path
builds the benchmark example with the release profile and requires a clean Git
worktree so the report can identify the measured commit.

### Run the smoke workload

From the repository root, run:

```bash
python3 scripts/append_pipeline_benchmark.py \
  --workload smoke \
  --samples 3 \
  --json-out /tmp/tstable-append-pipeline-smoke.json
```

The smoke workload is the normal validation workload. It contains 1,048,577
rows, uses 262,144-row batches, stores one payload byte per row, and uses seed
20,260,821. It produces five Arrow batches and two Parquet row groups with the
current writer settings.

CI uses `--samples 1` to verify the complete harness without making a
performance assertion. Recorded local comparisons should use the default of
three samples or a larger count. A sample count of two is rejected because it
is neither the CI exception nor a sufficient recorded comparison.

### Run the large-scale workload

The large-scale workload is manual and does not run in CI. It generates
3,466,797 rows with 8,192-row batches and a 1,024-byte payload per row. This is
3,550,000,128 bytes of logical payload, approximately 3.55 GB in decimal units.
It uses seed 20,260,821.

Confirm that the machine has sufficient memory and temporary storage, then
run:

```bash
python3 scripts/append_pipeline_benchmark.py \
  --workload large-scale \
  --samples 3 \
  --json-out /tmp/tstable-append-pipeline-large.json
```

Unless `--keep-data` is set, the runner removes each invocation's generated
data after collecting its metrics and validation result. Add `--keep-data` to
retain every source Parquet file, table, and GNU time output. The JSON field
`artifacts_directory` gives the retained root path.

To use a release binary that was built separately:

```bash
cargo build --locked --release \
  -p timeseries-table-format \
  --features cli \
  --example append_pipeline_bench

python3 scripts/append_pipeline_benchmark.py \
  --benchmark target/release/examples/append_pipeline_bench \
  --workload smoke \
  --samples 3 \
  --json-out /tmp/tstable-append-pipeline-smoke.json
```

An explicitly supplied binary is reported as prebuilt. The Git fields still
describe the current checkout and do not prove the source revision of an
external binary.

### Measurement method

The runner executes one untimed warm-up per mode. Measured repetitions
alternate mode order:

1. path-first, then streaming
2. streaming, then path-first
3. path-first, then streaming

Additional repetitions continue the same pattern. All invocations are
sequential and use fresh directories. Measured processes run under GNU
`/usr/bin/time -v` with `LC_ALL=C`; GNU time supplies peak resident set size.
The Rust driver uses a monotonic clock for pipeline and phase durations.

The fixed table definition uses the non-null `ts` Int64 column as its index,
bucket width 1, and no entity columns. Generated columns are non-null `ts`
Int64, `sequence` UInt64, and `payload` Binary. The current Parquet defaults
reported by the driver are Snappy compression, dictionary encoding enabled,
1,048,576 maximum rows per row group, a 1,048,576-byte data-page limit,
1,024 write-batch rows, page-level statistics, and Parquet writer version 1.0.
The report records these settings for every run instead of assuming they remain
unchanged across dependency updates.

Every invocation validates its committed table before the runner accepts it.
Validation covers the returned version, schema, requested row count, index
bounds, complete coverage, a full ordered scan, and BLAKE3 checksums for every
generated column. Each path-first result is then compared with the streaming
result from the same repetition. A generation, append, commit, parsing, or
validation error fails the complete run and no sample is silently omitted.

The benchmark has no speed, RSS, or storage threshold. Its output is evidence
from one environment, not a universal performance guarantee.

### Metric definitions

Raw samples and medians use these exact names:

| JSON field | Meaning |
| --- | --- |
| `end_to_end_pipeline_ns` | Time from source generation through the committed append. |
| `peak_rss_bytes` | Maximum resident set size reported by GNU time for the complete measured process. |
| `table_owned_segment_bytes` | Size of the committed segment owned by the table. |
| `total_retained_ingestion_bytes` | Source plus table segment for path-first, or the table segment alone for streaming. |
| `external_parquet_generation_ns` | Path-first time spent generating the external Parquet source. |
| `path_append_copy_commit_ns` | Path-first time spent appending, copying, and committing that source. |
| `external_source_parquet_bytes` | Size of the retained path-first source Parquet file. |
| `streaming_append_ns` | Streaming time from the start of source consumption through the returned committed version. |

Artifact byte fields are file sizes. They are not kernel block-I/O counters,
physical device reads, or physical device writes.

### JSON report reference

The runner writes the same JSON object to stdout and to `--json-out` when that
option is present. Its top-level fields are:

| Field | Contents |
| --- | --- |
| `schema_version` | Version of the combined report contract. |
| `repository` | Commit SHA and dirty-worktree status. |
| `benchmark` | Binary path, build profile, provenance, and exact build command. |
| `environment` | OS, kernel, architecture, effective CPU count, available memory, Rust compiler version, and benchmark locale. |
| `workload` | Workload name, generated payload bytes, generation parameters, table definition, and writer properties. |
| `sampling` | Warm-up count, measured count, and actual execution order. |
| `warmups` | Raw untimed invocation records. |
| `measured_samples` | Raw measured invocation records in execution order. |
| `medians` | Required medians grouped by `path-first` and `streaming`. |
| `validation` | Cross-mode equivalence result and the common logical result. |
| `artifacts_directory` | Retained root path when `--keep-data` is set; otherwise `null`. |

Each item in `warmups` and `measured_samples` contains `mode`, the exact
`command`, `peak_rss_bytes`, and the Rust `driver` result. Warm-up RSS is
`null` because warm-ups do not run under GNU time. Measured items also contain
their one-based `repetition` number.

Each `driver` result contains:

- `schema_version`, `mode`, and `process_id`
- `table_path` and, for path-first, `external_parquet_path`
- `workload`, `table_definition`, and `writer_properties`
- `timing` with the applicable phase fields and `end_to_end_pipeline_ns`
- `artifacts` with external, table-owned, and total retained byte fields
- `committed_version`
- `validation` with row count, schema and ordered-scan results, checksums,
  coverage, index bounds, segment path and bytes, and row-group count

Diagnostics and progress go to stderr. Driver stdout must contain exactly one
JSON object; malformed or incomplete output fails the runner.

## Verify bounded scan RSS and first-batch delivery

Use `scripts/scan_range_rss_regression.py` to measure the public core
`scan_range` path as one-segment tables grow from approximately 128 MiB to 1
GiB. The two tables have identical schemas, row-group shapes, payloads, and
timestamps; only the row-group count changes. The default workload uses 4,096
rows per group and a 1,024-byte payload per row, with 32 groups in the small
table and 256 in the large table. Actual segment sizes must remain within 10%
of those payload targets.

The check requires Linux, GNU `/usr/bin/time`, Cargo, and approximately 1.25
GiB of free temporary disk space. From the repository root, run:

```bash
python3 scripts/scan_range_rss_regression.py \
  --json-out /tmp/tstable-scan-range-rss.json
```

The script builds the release benchmark example, prepares each table in its own
process, then measures each scan in a fresh process. It checks that row groups
and returned batches stay at or below 8 MiB, that the first batch arrives before
scan completion, and that peak RSS grows by no more than the fixed 64 MiB
allowance. An equal or negative RSS delta passes.

The JSON summary records the Git commit, platform, build command and profile,
workload, actual segment sizes, row and row-group counts, batch sizes,
first-batch and total durations, exact scan commands, peak RSS values, RSS
delta, allowance, and pass result. The default recorded comparison requires a
clean Git worktree. The summary also records worktree state and whether the
binary was built by the runner or supplied with `--benchmark`. For a prebuilt
binary, the Git fields describe the repository checkout and do not identify the
binary's source revision.

Generated tables and timing files are removed on success and ordinary failure.
Add `--keep-data` to retain them. `--benchmark PATH` uses an existing executable
for diagnostics; its results are marked as prebuilt rather than attributed to
the repository build. A smaller diagnostic run can override the workload:

```bash
cargo build \
  -p timeseries-table-format \
  --features cli \
  --example scan_range_bench

python3 scripts/scan_range_rss_regression.py \
  --benchmark target/debug/examples/scan_range_bench \
  --small-row-groups 2 \
  --large-row-groups 4 \
  --rows-per-group 16 \
  --payload-bytes 32
```

## Thread sweep

```bash
./scripts/bench/bench_rg_threads.sh \
  data/fhvhv_2024-06.parquet \
  pickup_datetime \
  rg-parallel-threads.csv \
  1 \
  default \
  16
```

Arguments:
1) parquet file path (default: `data/fhvhv_2024-06.parquet`)
2) time column (default: `pickup_datetime`)
3) output csv (default: `rg-parallel-threads.csv`)
4) rg_chunk (default: `1`)
5) batch size (`default` to omit `--batch-size`)
6) max threads to test (default: `32`)

## Grid sweep (threads x rg_chunk x batch)

```bash
./scripts/bench/bench_rg_grid.sh \
  data/fhvhv_2024-06.parquet \
  pickup_datetime \
  rg-parallel-grid.csv
```

This produces a single consolidated CSV.
