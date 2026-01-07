#!/usr/bin/env bash
set -euo pipefail

FILE=${1:-data/fhvhv_2024-06.parquet}
TIME_COL=${2:-pickup_datetime}
CSV=${3:-rg-parallel-threads.csv}

RG_CHUNK=${4:-1}
BATCH_SIZE=${5:-default}
MAX_THREADS=${6:-32}

for t in $(seq 1 "$MAX_THREADS"); do
  if [[ "$BATCH_SIZE" == "default" ]]; then
    cargo run -p timeseries-table-core --bin coverage_bench -- \
      --file "$FILE" \
      --time-column "$TIME_COL" \
      --engine rg-parallel \
      --threads "$t" \
      --rg-chunk "$RG_CHUNK" \
      --iters 20 \
      --csv "$CSV"
  else
    cargo run -p timeseries-table-core --bin coverage_bench -- \
      --file "$FILE" \
      --time-column "$TIME_COL" \
      --engine rg-parallel \
      --threads "$t" \
      --rg-chunk "$RG_CHUNK" \
      --batch-size "$BATCH_SIZE" \
      --iters 20 \
      --csv "$CSV"
  fi
 done
