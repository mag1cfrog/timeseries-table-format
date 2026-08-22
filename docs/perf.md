# Performance tooling

This repo includes small profiling utilities. These tools are opt-in and do
not affect normal CLI usage unless invoked.

## Append widening comparison

Use `append_widening_bench` on Linux with GNU `/usr/bin/time` to compare
external Arrow normalization with lossless widening during append. Each mode
runs in a fresh process and table, validates the committed result, and emits a
combined JSON report.

Run the one-sample smoke workload to validate the benchmark and its
multi-batch, multi-row-group append paths:

```bash
cargo run --locked --release \
  -p timeseries-table-format \
  --features cli \
  --example append_widening_bench -- \
  compare --workload smoke --samples 1
```

The smoke workload has no performance threshold and is not benchmark
evidence. For a recorded comparison, run the 3,550,000,128-byte generated
payload workload with three alternating measured samples per mode:

```bash
cargo run --locked --release \
  -p timeseries-table-format \
  --features cli \
  --example append_widening_bench -- \
  compare \
  --workload large-scale \
  --samples 3 \
  --json-out /tmp/append-widening-large-scale.json
```

Completed invocation data is removed by default. Use `--keep-data` only when
the retained tables and external normalized Parquet files are needed and the
temporary filesystem has enough space. Compare results only within the same
report because timings and peak RSS depend on the host environment.

## Segment meta benchmark (scan strategies)

Compares different min/max extraction strategies for the timestamp column,
including row-iterator baseline, direct column reader, and row-group parallel
variants. Optional CSV output is supported.

Example:
```
cargo run --release -p timeseries-table-format --features cli --example segment_meta_bench -- \
  --parquet ./data/fhvhv_2024-04.parquet \
  --time-column pickup_datetime \
  --warmup 1 --repeat 3 --threads 20 \
  --csv-out ./segment_meta_bench.csv
```

Notes:
- `--threads 0` means “auto” for parallel strategies.
- Benchmarks validate correctness by comparing min/max across strategies.

## Coverage bitmap benchmark
The canonical Rust crate includes `coverage_bench`, which compares coverage computation
strategies for Parquet time columns.

Example:
```
cargo run --release -p timeseries-table-format --example coverage_bench -- \
  --file ./data/fhvhv_2024-04.parquet \
  --time-column pickup_datetime \
  --bucket 1s \
  --engine all \
  --iters 5 --warmup 1 \
  --csv ./coverage_bench.csv
```

Key options:
- `--engine` = `baseline` | `rg-parallel` | `parquet-direct` | `all`
- `--threads` for RG-parallel
- `--batch-size` for Arrow/parquet-direct readers
- `--print-metadata` to inspect row groups
