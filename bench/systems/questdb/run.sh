#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)

# shellcheck source=/dev/null
source "${ROOT_DIR}/bench/config.env"
# shellcheck source=/dev/null
source "${ROOT_DIR}/bench/scripts/bench_lib.sh"

COMPOSE="docker compose -f ${ROOT_DIR}/bench/compose.yml --project-directory ${ROOT_DIR}/bench"
CSV_OUT="${RESULTS_RUN_DIR}/questdb.csv"
ensure_csv_header "$CSV_OUT"

questdb_http="http://localhost:9000"

for _ in {1..30}; do
  if curl -fsS "${questdb_http}/exec?query=select%201" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

bulk_rel="csv/raw/${TLC_FILE_PREFIX}${START_MONTH}.csv"
bulk_path="${ROOT_DIR}/${DATASET_DIR}/${bulk_rel}"
if [[ ! -f "${bulk_path}" ]]; then
  emit_row "$CSV_OUT" "questdb" "skipped" "" "" "" "" "$CPU_LIMIT" "$MEM_LIMIT" "missing_csv"
  exit 0
fi
read -r bulk_rows bulk_bytes < <(manifest_lookup "${ROOT_DIR}/${DATASET_DIR}" "$bulk_rel")

start=$(now_ms)
curl -fsS -F "data=@${bulk_path}" \
  "${questdb_http}/imp?name=trips&timestamp=${TIME_COLUMN}&overwrite=true" >/dev/null
elapsed=$(( $(now_ms) - start ))
emit_row "$CSV_OUT" "questdb" "bulk_ingest" "$bulk_rel" "$bulk_rows" "$bulk_bytes" "$elapsed" "$CPU_LIMIT" "$MEM_LIMIT" ""

mapfile -t daily_files < <(ls -1 "${ROOT_DIR}/${DATASET_DIR}/csv/daily"/fhvhv_*.csv | sort)
for file in "${daily_files[@]}"; do
  rel="csv/daily/$(basename "$file")"
  path="${ROOT_DIR}/${DATASET_DIR}/${rel}"
  read -r rows bytes < <(manifest_lookup "${ROOT_DIR}/${DATASET_DIR}" "$rel")
  start=$(now_ms)
  curl -fsS -F "data=@${path}" \
    "${questdb_http}/imp?name=trips&timestamp=${TIME_COLUMN}&overwrite=false" >/dev/null
  elapsed=$(( $(now_ms) - start ))
  emit_row "$CSV_OUT" "questdb" "daily_append" "$rel" "$rows" "$bytes" "$elapsed" "$CPU_LIMIT" "$MEM_LIMIT" ""
done

for q in q1_time_range q2_agg q3_filter_agg q4_groupby q5_date_trunc; do
  sql=$(sed -e "s/{START}/${QUERY_START}/g" \
            -e "s/{END}/${QUERY_END}/g" \
            -e "s/{MIN_MILES}/${MIN_MILES}/g" \
            "${ROOT_DIR}/bench/queries/${q}.sql")
  start=$(now_ms)
  curl -fsS -G --data-urlencode "query=${sql}" "${questdb_http}/exec" >/dev/null
  elapsed=$(( $(now_ms) - start ))
  emit_row "$CSV_OUT" "questdb" "${q}" "" "" "" "$elapsed" "$CPU_LIMIT" "$MEM_LIMIT" ""
done
