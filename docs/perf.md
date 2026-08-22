# Performance tooling

This repo includes small profiling utilities. These tools are opt-in and do
not affect normal CLI usage unless invoked.

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
