# Performance tooling

This repo includes small profiling utilities for investigating append performance.
These tools are opt-in and do not affect normal CLI usage unless invoked.

## Append profile (end-to-end)
Runs a fresh table under `<table>/benchmark`, executes create + append, and prints
step timings. Optional CSV output is supported.

Example:
```
cargo run -p timeseries-table-cli --bin append_profile -- \
  --table ./data/nyc_hvfhv \
  --parquet ./data/fhvhv_2024-04.parquet \
  --time-column pickup_datetime \
  --bucket 1s \
  --csv-out ./append_profile.csv
```

Output includes per-step timings (read bytes, segment_meta, coverage, etc.) and
context fields like row groups and scanned rows.

## Segment meta benchmark (scan strategies)
Compares different min/max extraction strategies for the timestamp column,
including row-iterator baseline, direct column reader, and row-group parallel
variants. Optional CSV output is supported.

Example:
```
cargo run -p timeseries-table-cli --bin segment_meta_bench -- \
  --parquet ./data/fhvhv_2024-04.parquet \
  --time-column pickup_datetime \
  --warmup 1 --repeat 3 --threads 20 \
  --csv-out ./segment_meta_bench.csv
```

Notes:
- `--threads 0` means “auto” for parallel strategies.
- Benchmarks validate correctness by comparing min/max across strategies.
