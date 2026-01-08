# Benchmark helpers

These scripts run the `coverage_bench` binary with common parameter sweeps.

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
