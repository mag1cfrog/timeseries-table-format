#!/usr/bin/env bash
set -euo pipefail

FILE=${1:-data/fhvhv_2024-06.parquet}
TIME_COL=${2:-pickup_datetime}
CSV=${3:-rg-parallel-grid.csv}

THREADS_LIST=(2 4 8 12)
RG_CHUNK_LIST=(1 2 4 8)
BATCH_LIST=(8192 32768 65536 131072)

for t in "${THREADS_LIST[@]}"; do
  for c in "${RG_CHUNK_LIST[@]}"; do
    for b in "${BATCH_LIST[@]}"; do
      cargo run -p timeseries-table-core --bin coverage_bench -- \
        --file "$FILE" \
        --time-column "$TIME_COL" \
        --engine rg-parallel \
        --threads "$t" \
        --rg-chunk "$c" \
        --batch-size "$b" \
        --iters 5 \
        --csv "$CSV"
    done
  done
 done
