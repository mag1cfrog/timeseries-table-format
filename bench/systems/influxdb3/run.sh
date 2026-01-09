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

ensure_influx_running() {
  local cid
  cid="$(${COMPOSE} ps -q influxdb3 2>/dev/null || true)"
  if [[ -z "${cid}" ]]; then
    ${COMPOSE} up -d influxdb3
    sleep 3
    cid="$(${COMPOSE} ps -q influxdb3 2>/dev/null || true)"
  fi
  if [[ -n "${cid}" ]]; then
    local status
    status="$(docker inspect -f '{{.State.Status}}' "${cid}" 2>/dev/null || true)"
    if [[ "${status}" != "running" ]]; then
      ${COMPOSE} up -d influxdb3
      sleep 3
    fi
    local oom
    oom="$(docker inspect -f '{{.State.OOMKilled}}' "${cid}" 2>/dev/null || true)"
    if [[ "${oom}" == "true" ]]; then
      echo "influxdb3: container OOMKilled; restart and reduce load before retrying" >&2
      exit 1
    fi
  fi
}

ensure_influx_running

startup_wait="${INFLUX_STARTUP_WAIT_SEC:-5}"
if [[ "${startup_wait}" != "0" && "${startup_wait}" != "0.0" ]]; then
  echo "influxdb3: waiting ${startup_wait}s for server startup"
  sleep "${startup_wait}"
fi

if [[ ! -d "${ROOT_DIR}/${INFLUX_LINE_PROTOCOL_DIR}" ]]; then
  emit_row "$CSV_OUT" "influxdb3" "skipped" "" "" "" "" "$CPU_LIMIT" "$MEM_LIMIT" "missing_line_protocol"
  exit 0
fi

bulk_rel="raw/${TLC_FILE_PREFIX}${START_MONTH}.lp"
bulk_path="/workspace/${INFLUX_LINE_PROTOCOL_DIR}/${bulk_rel}"
read -r bulk_rows bulk_bytes < <(manifest_lookup "${ROOT_DIR}/${DATASET_DIR}" "influx/${bulk_rel}")

chunk_bytes="${INFLUX_WRITE_CHUNK_BYTES:-8000000}"
sleep_sec="${INFLUX_WRITE_SLEEP_SEC:-0}"
chunk_dir="/workspace/${WORK_DIR}/influxdb3_chunks"

split_and_write() {
  local lp_path="$1"
  local db_name="$2"
  local test_name="$3"
  local file_label="$4"

  start=$(now_ms)
  ${COMPOSE} exec -T influxdb3 bash -lc "set -euo pipefail; mkdir -p '${chunk_dir}' && rm -f '${chunk_dir}'/part_*; split -C ${chunk_bytes} -d -a 4 '${lp_path}' '${chunk_dir}/part_'; max_bytes=\$(stat -c%s ${chunk_dir}/part_* | sort -n | tail -1); total=\$(ls -1 ${chunk_dir}/part_* | wc -l); echo \"influxdb3: ${file_label} chunks=\${total} max_bytes=\${max_bytes}\"; for part in ${chunk_dir}/part_*; do if [[ \"${INFLUXDB3_NO_SYNC:-}\" == \"1\" ]]; then influxdb3 write --no-sync --database '${db_name}' --file \"\${part}\" >/dev/null || { echo \"influxdb3: write failed for \${part}\"; influxdb3 write --no-sync --database '${db_name}' --file \"\${part}\"; exit 1; }; else influxdb3 write --database '${db_name}' --file \"\${part}\" >/dev/null || { echo \"influxdb3: write failed for \${part}\"; influxdb3 write --database '${db_name}' --file \"\${part}\"; exit 1; }; fi; if [[ \"${sleep_sec}\" != \"0\" && \"${sleep_sec}\" != \"0.0\" ]]; then sleep ${sleep_sec}; fi; done; echo \"influxdb3: ${file_label} write complete\""
  elapsed=$(( $(now_ms) - start ))
  emit_row "$CSV_OUT" "influxdb3" "${test_name}" "${file_label}" "$bulk_rows" "$bulk_bytes" "$elapsed" "$CPU_LIMIT" "$MEM_LIMIT" "chunk_bytes=${chunk_bytes} sleep=${sleep_sec} no_sync=${INFLUXDB3_NO_SYNC:-0}"
  echo "influxdb3: ${file_label} recorded"
}

split_and_write "${bulk_path}" "${INFLUX_DATABASE}_bulk" "bulk_ingest" "influx/${bulk_rel}"

mapfile -t daily_files < <(ls -1 "${ROOT_DIR}/${INFLUX_LINE_PROTOCOL_DIR}/daily"/fhvhv_*.lp 2>/dev/null | sort)
for file in "${daily_files[@]}"; do
  rel="daily/$(basename "$file")"
  path="/workspace/${INFLUX_LINE_PROTOCOL_DIR}/${rel}"
  read -r rows bytes < <(manifest_lookup "${ROOT_DIR}/${DATASET_DIR}" "influx/${rel}")
  start=$(now_ms)
  ${COMPOSE} exec -T influxdb3 bash -lc "set -euo pipefail; mkdir -p '${chunk_dir}' && rm -f '${chunk_dir}'/part_*; split -C ${chunk_bytes} -d -a 4 '${path}' '${chunk_dir}/part_'; max_bytes=\$(stat -c%s ${chunk_dir}/part_* | sort -n | tail -1); total=\$(ls -1 ${chunk_dir}/part_* | wc -l); echo \"influxdb3: influx/${rel} chunks=\${total} max_bytes=\${max_bytes}\"; for part in ${chunk_dir}/part_*; do if [[ \"${INFLUXDB3_NO_SYNC:-}\" == \"1\" ]]; then influxdb3 write --no-sync --database '${INFLUX_DATABASE}_daily' --file \"\${part}\" >/dev/null || { echo \"influxdb3: write failed for \${part}\"; influxdb3 write --no-sync --database '${INFLUX_DATABASE}_daily' --file \"\${part}\"; exit 1; }; else influxdb3 write --database '${INFLUX_DATABASE}_daily' --file \"\${part}\" >/dev/null || { echo \"influxdb3: write failed for \${part}\"; influxdb3 write --database '${INFLUX_DATABASE}_daily' --file \"\${part}\"; exit 1; }; fi; if [[ \"${sleep_sec}\" != \"0\" && \"${sleep_sec}\" != \"0.0\" ]]; then sleep ${sleep_sec}; fi; done; echo \"influxdb3: influx/${rel} write complete\""
  elapsed=$(( $(now_ms) - start ))
  emit_row "$CSV_OUT" "influxdb3" "daily_append" "influx/${rel}" "$rows" "$bytes" "$elapsed" "$CPU_LIMIT" "$MEM_LIMIT" "chunk_bytes=${chunk_bytes} sleep=${sleep_sec} no_sync=${INFLUXDB3_NO_SYNC:-0}"
  echo "influxdb3: influx/${rel} recorded"
done

# InfluxDB 3 Core enforces a file-scan limit that blocks wide-range queries on large ingest.
for q in q1_time_range q2_agg q3_filter_agg q4_groupby q5_date_trunc; do
  emit_row "$CSV_OUT" "influxdb3" "${q}" "" "" "" "" "$CPU_LIMIT" "$MEM_LIMIT" "not_supported_core_file_limit"
done
