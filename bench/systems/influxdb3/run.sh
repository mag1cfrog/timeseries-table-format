#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)

# shellcheck source=/dev/null
source "${ROOT_DIR}/bench/config.env"
# shellcheck source=/dev/null
source "${ROOT_DIR}/bench/scripts/bench_lib.sh"

COMPOSE="docker compose -f ${ROOT_DIR}/bench/compose.yml --project-directory ${ROOT_DIR}/bench"
CSV_OUT="${RESULTS_RUN_DIR}/influxdb3.csv"
ensure_csv_header "$CSV_OUT"

if [[ ! -d "${ROOT_DIR}/${INFLUX_LINE_PROTOCOL_DIR}" ]]; then
  emit_row "$CSV_OUT" "influxdb3" "skipped" "" "" "" "" "$CPU_LIMIT" "$MEM_LIMIT" "missing_line_protocol"
  exit 0
fi

bulk_rel="raw/${TLC_FILE_PREFIX}${START_MONTH}.lp"
bulk_path="/workspace/${INFLUX_LINE_PROTOCOL_DIR}/${bulk_rel}"
read -r bulk_rows bulk_bytes < <(manifest_lookup "${ROOT_DIR}/${DATASET_DIR}" "influx/${bulk_rel}")

start=$(now_ms)
${COMPOSE} exec -T influxdb3 influxdb3 write \
  --database "${INFLUX_DATABASE}" \
  --file "${bulk_path}"
elapsed=$(( $(now_ms) - start ))
emit_row "$CSV_OUT" "influxdb3" "bulk_ingest" "influx/${bulk_rel}" "$bulk_rows" "$bulk_bytes" "$elapsed" "$CPU_LIMIT" "$MEM_LIMIT" ""

mapfile -t daily_files < <(ls -1 "${ROOT_DIR}/${INFLUX_LINE_PROTOCOL_DIR}/daily"/fhvhv_*.lp 2>/dev/null | sort)
for file in "${daily_files[@]}"; do
  rel="daily/$(basename "$file")"
  path="/workspace/${INFLUX_LINE_PROTOCOL_DIR}/${rel}"
  read -r rows bytes < <(manifest_lookup "${ROOT_DIR}/${DATASET_DIR}" "influx/${rel}")
  start=$(now_ms)
  ${COMPOSE} exec -T influxdb3 influxdb3 write \
    --database "${INFLUX_DATABASE}" \
    --file "${path}"
  elapsed=$(( $(now_ms) - start ))
  emit_row "$CSV_OUT" "influxdb3" "daily_append" "influx/${rel}" "$rows" "$bytes" "$elapsed" "$CPU_LIMIT" "$MEM_LIMIT" ""
done

# Queries for InfluxDB 3 are intentionally left as a follow-up once the SQL/Flight API is configured.
for q in q1_time_range q2_agg q3_filter_agg q4_groupby q5_date_trunc q6_date_bin; do
  emit_row "$CSV_OUT" "influxdb3" "${q}" "" "" "" "" "$CPU_LIMIT" "$MEM_LIMIT" "not_implemented"
done
