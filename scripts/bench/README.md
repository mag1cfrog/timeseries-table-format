# Benchmark helpers

These scripts run the `coverage_bench` example with common parameter sweeps.

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
