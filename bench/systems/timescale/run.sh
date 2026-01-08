#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)

# shellcheck source=/dev/null
source "${ROOT_DIR}/bench/config.env"
# shellcheck source=/dev/null
source "${ROOT_DIR}/bench/scripts/bench_lib.sh"

COMPOSE="docker compose -f ${ROOT_DIR}/bench/compose.yml --project-directory ${ROOT_DIR}/bench"
CSV_OUT="${RESULTS_RUN_DIR}/timescale.csv"
ensure_csv_header "$CSV_OUT"

for _ in {1..30}; do
  if ${COMPOSE} exec -T timescaledb pg_isready -U bench >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

${COMPOSE} exec -T timescaledb psql -U bench -d bench -f /workspace/bench/systems/timescale/schema.sql

bulk_rel="csv/raw/${TLC_FILE_PREFIX}${START_MONTH}.csv"
bulk_path="/workspace/${DATASET_DIR}/${bulk_rel}"
read -r bulk_rows bulk_bytes < <(manifest_lookup "${ROOT_DIR}/${DATASET_DIR}" "$bulk_rel")

start=$(now_ms)
${COMPOSE} exec -T timescaledb psql -U bench -d bench -c "\\copy trips FROM '${bulk_path}' WITH (FORMAT csv, HEADER true)"
elapsed=$(( $(now_ms) - start ))
emit_row "$CSV_OUT" "timescale" "bulk_ingest" "$bulk_rel" "$bulk_rows" "$bulk_bytes" "$elapsed" "$CPU_LIMIT" "$MEM_LIMIT" ""

mapfile -t daily_files < <(ls -1 "${ROOT_DIR}/${DATASET_DIR}/csv/daily"/fhvhv_*.csv | sort)
for file in "${daily_files[@]}"; do
  rel="csv/daily/$(basename "$file")"
  path="/workspace/${DATASET_DIR}/${rel}"
  read -r rows bytes < <(manifest_lookup "${ROOT_DIR}/${DATASET_DIR}" "$rel")
  start=$(now_ms)
  ${COMPOSE} exec -T timescaledb psql -U bench -d bench -c "\\copy trips FROM '${path}' WITH (FORMAT csv, HEADER true)"
  elapsed=$(( $(now_ms) - start ))
  emit_row "$CSV_OUT" "timescale" "daily_append" "$rel" "$rows" "$bytes" "$elapsed" "$CPU_LIMIT" "$MEM_LIMIT" ""
done

for q in q1_time_range q2_agg q3_filter_agg q4_groupby q5_date_trunc q6_date_bin; do
  sql=$(sed -e "s/{START}/${QUERY_START}/g" \
            -e "s/{END}/${QUERY_END}/g" \
            -e "s/{MIN_MILES}/${MIN_MILES}/g" \
            "${ROOT_DIR}/bench/queries/${q}.sql")
  sql_compact=$(echo "$sql" | tr '\n' ' ')
  start=$(now_ms)
  ${COMPOSE} exec -T timescaledb psql -U bench -d bench -c "$sql_compact" >/dev/null
  elapsed=$(( $(now_ms) - start ))
  emit_row "$CSV_OUT" "timescale" "${q}" "" "" "" "$elapsed" "$CPU_LIMIT" "$MEM_LIMIT" ""
done
