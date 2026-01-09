#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)

# shellcheck source=/dev/null
source "${ROOT_DIR}/bench/config.env"
# shellcheck source=/dev/null
source "${ROOT_DIR}/bench/scripts/bench_lib.sh"

COMPOSE="docker compose -f ${ROOT_DIR}/bench/compose.yml --project-directory ${ROOT_DIR}/bench"
CSV_OUT="${RESULTS_RUN_DIR}/timeseries_table.csv"
ensure_csv_header "$CSV_OUT"

TABLE_DIR="/workspace/${WORK_DIR}/timeseries_table/${TABLE_NAME}"
TABLE_DIR_BULK="/workspace/${WORK_DIR}/timeseries_table/${TABLE_NAME}_bulk"

${COMPOSE} exec -T timeseries_table bash -lc "/usr/local/cargo/bin/cargo build -p timeseries-table-cli --release"
${COMPOSE} exec -T timeseries_table bash -lc "rm -rf '${TABLE_DIR_BULK}' && mkdir -p '${TABLE_DIR_BULK}'"
${COMPOSE} exec -T timeseries_table bash -lc "target/release/timeseries-table-cli create --table '${TABLE_DIR_BULK}' --time-column '${TIME_COLUMN}' --bucket 1s"

bulk_rel="raw/${TLC_FILE_PREFIX}${START_MONTH}.parquet"
bulk_path="/workspace/${DATASET_DIR}/${bulk_rel}"
read -r bulk_rows bulk_bytes < <(manifest_lookup "${ROOT_DIR}/${DATASET_DIR}" "$bulk_rel")

start=$(now_ms)
${COMPOSE} exec -T timeseries_table bash -lc "target/release/timeseries-table-cli append --table '${TABLE_DIR_BULK}' --parquet '${bulk_path}'"
elapsed=$(( $(now_ms) - start ))
emit_row "$CSV_OUT" "timeseries_table" "bulk_ingest" "$bulk_rel" "$bulk_rows" "$bulk_bytes" "$elapsed" "$CPU_LIMIT" "$MEM_LIMIT" ""

${COMPOSE} exec -T timeseries_table bash -lc "rm -rf '${TABLE_DIR}' && mkdir -p '${TABLE_DIR}'"
${COMPOSE} exec -T timeseries_table bash -lc "target/release/timeseries-table-cli create --table '${TABLE_DIR}' --time-column '${TIME_COLUMN}' --bucket 1s"

mapfile -t daily_files < <(ls -1 "${ROOT_DIR}/${DATASET_DIR}/daily"/fhvhv_*.parquet | sort)
for file in "${daily_files[@]}"; do
  rel="daily/$(basename "$file")"
  path="/workspace/${DATASET_DIR}/${rel}"
  read -r rows bytes < <(manifest_lookup "${ROOT_DIR}/${DATASET_DIR}" "$rel")
  start=$(now_ms)
  ${COMPOSE} exec -T timeseries_table bash -lc "target/release/timeseries-table-cli append --table '${TABLE_DIR}' --parquet '${path}'"
  elapsed=$(( $(now_ms) - start ))
  emit_row "$CSV_OUT" "timeseries_table" "daily_append" "$rel" "$rows" "$bytes" "$elapsed" "$CPU_LIMIT" "$MEM_LIMIT" ""
done

for q in q1_time_range q2_agg q3_filter_agg q4_groupby q5_date_trunc; do
  sql=$(sed -e "s/{START}/${QUERY_START}/g" \
            -e "s/{END}/${QUERY_END}/g" \
            -e "s/{MIN_MILES}/${MIN_MILES}/g" \
            "${ROOT_DIR}/bench/queries/${q}.sql")
  sql_compact=$(echo "$sql" | tr '\n' ' ')
  sql_escaped=${sql_compact//\"/\\\"}
  start=$(now_ms)
  ${COMPOSE} exec -T timeseries_table bash -lc "target/release/timeseries-table-cli query --table '${TABLE_DIR}' --sql \"${sql_escaped}\" --max-rows 0 >/dev/null"
  elapsed=$(( $(now_ms) - start ))
  emit_row "$CSV_OUT" "timeseries_table" "${q}" "" "" "" "$elapsed" "$CPU_LIMIT" "$MEM_LIMIT" ""
done
