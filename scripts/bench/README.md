# Benchmark helpers

These helpers run reproducible process checks and common `coverage_bench`
parameter sweeps.

## Verify bounded append RSS

Use `scripts/append_rss_regression.py` to check that appending a Parquet file does
not consume memory in proportion to an unprojected payload column. The script
generates approximately 128 MiB and 1 GiB uncompressed inputs, creates a fresh
table for each input, and measures the production `tstable append` process while
it copies the external file into the table.

This check requires:

- Linux.
- GNU `/usr/bin/time`.
- `uv` and the repository's locked Python dependencies.
- Approximately 2.5 GiB of free disk space for inputs and staged copies.

Install PyArrow without building the Python extension:

```bash
uv sync \
  --project crates/timeseries-table-python \
  --locked \
  --no-install-project
```

From the repository root, run the exact regression command:

```bash
crates/timeseries-table-python/.venv/bin/python \
  scripts/append_rss_regression.py \
  --json-out /tmp/tstable-append-rss.json
```

The script builds release `tstable` unless `--tstable PATH` supplies an existing
binary. Progress is written to standard error. The machine-readable summary is
written to standard output and, when `--json-out` is set, to that path.

The JSON contains:

- Actual file sizes, peak RSS values, and exact append commands under
  `measurements.small` and `measurements.large`.
- `rss_delta_bytes`, calculated as large-file RSS minus small-file RSS.
- `max_rss_delta_bytes` and the resulting `passed` value.
- The build profile, binary path, operating system, row count, target sizes, and
  worker configuration.
- `artifacts_directory`, which is `null` unless artifacts are retained.

The command exits nonzero when `rss_delta_bytes` is greater than 128 MiB. An
equal or negative delta passes.

Generated inputs, tables, and timing files are removed on success and ordinary
failure. Add `--keep-artifacts` to retain them; the script prints their directory
and records it in the JSON summary.

For a quick smoke run with an existing debug binary:

```bash
crates/timeseries-table-python/.venv/bin/python \
  scripts/append_rss_regression.py \
  --tstable target/debug/tstable \
  --small-mib 1 \
  --large-mib 2 \
  --row-count 64 \
  --row-groups 4
```

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
