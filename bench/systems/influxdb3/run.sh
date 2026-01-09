#!/usr/bin/env bash
set -euo pipefail
trap 'echo "influxdb3: failed at line $LINENO" >&2' ERR

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

chunk_bytes="${INFLUX_WRITE_CHUNK_BYTES:-8000000}"
chunk_dir="/workspace/${WORK_DIR}/influxdb3_chunks"

split_and_write() {
  local lp_path="$1"
  local db_name="$2"
  local test_name="$3"
  local file_label="$4"

  start=$(now_ms)
  ${COMPOSE} exec -T influxdb3 bash -lc "set -euo pipefail; mkdir -p '${chunk_dir}' && rm -f '${chunk_dir}'/part_*; split -C ${chunk_bytes} -d -a 4 '${lp_path}' '${chunk_dir}/part_'; max_bytes=\$(stat -c%s ${chunk_dir}/part_* | sort -n | tail -1); total=\$(ls -1 ${chunk_dir}/part_* | wc -l); echo \"influxdb3: ${file_label} chunks=\${total} max_bytes=\${max_bytes}\"; for part in ${chunk_dir}/part_*; do if [[ \"${INFLUXDB3_NO_SYNC:-}\" == \"1\" ]]; then influxdb3 write --no-sync --database '${db_name}' --file \"\${part}\" >/dev/null; else influxdb3 write --database '${db_name}' --file \"\${part}\" >/dev/null; fi; done; echo \"influxdb3: ${file_label} write complete\""
  elapsed=$(( $(now_ms) - start ))
  emit_row "$CSV_OUT" "influxdb3" "${test_name}" "${file_label}" "$bulk_rows" "$bulk_bytes" "$elapsed" "$CPU_LIMIT" "$MEM_LIMIT" "chunk_bytes=${chunk_bytes}"
  echo "influxdb3: ${file_label} recorded"
}

split_and_write "${bulk_path}" "${INFLUX_DATABASE}_bulk" "bulk_ingest" "influx/${bulk_rel}"

mapfile -t daily_files < <(ls -1 "${ROOT_DIR}/${INFLUX_LINE_PROTOCOL_DIR}/daily"/fhvhv_*.lp 2>/dev/null | sort)
for file in "${daily_files[@]}"; do
  rel="daily/$(basename "$file")"
  path="/workspace/${INFLUX_LINE_PROTOCOL_DIR}/${rel}"
  read -r rows bytes < <(manifest_lookup "${ROOT_DIR}/${DATASET_DIR}" "influx/${rel}")
  start=$(now_ms)
  ${COMPOSE} exec -T influxdb3 bash -lc "set -euo pipefail; mkdir -p '${chunk_dir}' && rm -f '${chunk_dir}'/part_*; split -C ${chunk_bytes} -d -a 4 '${path}' '${chunk_dir}/part_'; max_bytes=\$(stat -c%s ${chunk_dir}/part_* | sort -n | tail -1); total=\$(ls -1 ${chunk_dir}/part_* | wc -l); echo \"influxdb3: influx/${rel} chunks=\${total} max_bytes=\${max_bytes}\"; for part in ${chunk_dir}/part_*; do if [[ \"${INFLUXDB3_NO_SYNC:-}\" == \"1\" ]]; then influxdb3 write --no-sync --database '${INFLUX_DATABASE}_daily' --file \"\${part}\" >/dev/null; else influxdb3 write --database '${INFLUX_DATABASE}_daily' --file \"\${part}\" >/dev/null; fi; done; echo \"influxdb3: influx/${rel} write complete\""
  elapsed=$(( $(now_ms) - start ))
  emit_row "$CSV_OUT" "influxdb3" "daily_append" "influx/${rel}" "$rows" "$bytes" "$elapsed" "$CPU_LIMIT" "$MEM_LIMIT" "chunk_bytes=${chunk_bytes}"
  echo "influxdb3: influx/${rel} recorded"
done

# Queries for InfluxDB 3 are intentionally left as a follow-up once the SQL/Flight API is configured.
for q in q1_time_range q2_agg q3_filter_agg q4_groupby q5_date_trunc; do
  emit_row "$CSV_OUT" "influxdb3" "${q}" "" "" "" "" "$CPU_LIMIT" "$MEM_LIMIT" "not_implemented"
done
