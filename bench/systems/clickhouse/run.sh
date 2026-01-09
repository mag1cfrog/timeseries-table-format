#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)

# shellcheck source=/dev/null
source "${ROOT_DIR}/bench/config.env"
# shellcheck source=/dev/null
source "${ROOT_DIR}/bench/scripts/bench_lib.sh"

COMPOSE="docker compose -f ${ROOT_DIR}/bench/compose.yml --project-directory ${ROOT_DIR}/bench"
CSV_OUT="${RESULTS_RUN_DIR}/clickhouse.csv"
ensure_csv_header "$CSV_OUT"

# Normalize ISO 8601 timestamps ("2024-05-01T00:00:00Z") to a ClickHouse-friendly
# format ("2024-05-01 00:00:00") using the server's timezone (expected UTC).
CH_START=${QUERY_START/T/ }
CH_START=${CH_START/Z/}
CH_END=${QUERY_END/T/ }
CH_END=${CH_END/Z/}

for _ in {1..30}; do
  if ${COMPOSE} exec -T clickhouse clickhouse-client --query "SELECT 1" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

${COMPOSE} exec -T clickhouse clickhouse-client --multiquery < "${ROOT_DIR}/bench/systems/clickhouse/schema.sql"

bulk_rel="csv/raw/${TLC_FILE_PREFIX}${START_MONTH}.csv"
bulk_path="/workspace/${DATASET_DIR}/${bulk_rel}"
if [[ ! -f "${ROOT_DIR}/${DATASET_DIR}/${bulk_rel}" ]]; then
  emit_row "$CSV_OUT" "clickhouse" "skipped" "" "" "" "" "$CPU_LIMIT" "$MEM_LIMIT" "missing_csv"
  exit 0
fi
read -r bulk_rows bulk_bytes < <(manifest_lookup "${ROOT_DIR}/${DATASET_DIR}" "$bulk_rel")

start=$(now_ms)
${COMPOSE} exec -T clickhouse clickhouse-client \
  --database bench \
  --query "INSERT INTO trips FORMAT CSVWithNames" \
  --date_time_input_format best_effort \
  --input_format_null_as_default 1 \
  < "${ROOT_DIR}/${DATASET_DIR}/${bulk_rel}"
elapsed=$(( $(now_ms) - start ))
emit_row "$CSV_OUT" "clickhouse" "bulk_ingest" "$bulk_rel" "$bulk_rows" "$bulk_bytes" "$elapsed" "$CPU_LIMIT" "$MEM_LIMIT" ""

${COMPOSE} exec -T clickhouse clickhouse-client --multiquery < "${ROOT_DIR}/bench/systems/clickhouse/schema.sql"

mapfile -t daily_files < <(ls -1 "${ROOT_DIR}/${DATASET_DIR}/csv/daily"/fhvhv_*.csv | sort)
for file in "${daily_files[@]}"; do
  rel="csv/daily/$(basename "$file")"
  path="/workspace/${DATASET_DIR}/${rel}"
  read -r rows bytes < <(manifest_lookup "${ROOT_DIR}/${DATASET_DIR}" "$rel")
  start=$(now_ms)
  ${COMPOSE} exec -T clickhouse clickhouse-client \
    --database bench \
    --query "INSERT INTO trips FORMAT CSVWithNames" \
    --date_time_input_format best_effort \
    --input_format_null_as_default 1 \
    < "${ROOT_DIR}/${DATASET_DIR}/${rel}"
  elapsed=$(( $(now_ms) - start ))
  emit_row "$CSV_OUT" "clickhouse" "daily_append" "$rel" "$rows" "$bytes" "$elapsed" "$CPU_LIMIT" "$MEM_LIMIT" ""
done

for q in q1_time_range q2_agg q3_filter_agg q4_groupby q5_date_trunc; do
  sql=$(sed -e "s/{START}/${CH_START}/g" \
            -e "s/{END}/${CH_END}/g" \
            -e "s/{MIN_MILES}/${MIN_MILES}/g" \
            "${ROOT_DIR}/bench/queries/${q}.sql")
  if [[ "${q}" == "q5_date_trunc" ]]; then
    sql=${sql//date_trunc('hour', pickup_datetime)/toStartOfHour(pickup_datetime)}
  fi
  start=$(now_ms)
  ${COMPOSE} exec -T clickhouse clickhouse-client \
    --database bench \
    --query "${sql}" >/dev/null
  elapsed=$(( $(now_ms) - start ))
  emit_row "$CSV_OUT" "clickhouse" "${q}" "" "" "" "$elapsed" "$CPU_LIMIT" "$MEM_LIMIT" ""
done
