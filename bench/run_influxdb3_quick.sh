#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)

# shellcheck source=/dev/null
set -a
source "${ROOT_DIR}/bench/config.env"
set +a

if [[ -z "${RESULTS_RUN_DIR:-}" ]]; then
  TIMESTAMP=$(date +%Y%m%d_%H%M%S)
  RESULTS_RUN_DIR="${ROOT_DIR}/${RESULTS_DIR}/${TIMESTAMP}"
  mkdir -p "${RESULTS_RUN_DIR}"
  export RESULTS_RUN_DIR
fi

export MANIFEST_CSV="${ROOT_DIR}/${DATASET_DIR}/manifest.csv"
export DATASET_ROOT="${ROOT_DIR}/${DATASET_DIR}"
export QUERY_START QUERY_END MIN_MILES CPU_LIMIT MEM_LIMIT TIME_COLUMN TABLE_NAME

COMPOSE="docker compose -f ${ROOT_DIR}/bench/compose.yml --project-directory ${ROOT_DIR}/bench"

echo "==> InfluxDB3 quick: start services"
pushd "${ROOT_DIR}/bench" >/dev/null
  docker compose -f compose.yml up -d --build influxdb3
popd >/dev/null
echo "==> InfluxDB3 quick: services up"

startup_wait="${INFLUX_STARTUP_WAIT_SEC:-5}"
if [[ "${startup_wait}" != "0" && "${startup_wait}" != "0.0" ]]; then
  echo "==> InfluxDB3 quick: waiting ${startup_wait}s"
  sleep "${startup_wait}"
fi

quick_lines="${INFLUX_QUICK_LINES:-200000}"
quick_tmp="/workspace/${WORK_DIR}/influxdb3_quick.lp"
quick_chunk_bytes="${INFLUX_QUICK_CHUNK_BYTES:-20000000}"
quick_chunk_dir="/workspace/${WORK_DIR}/influxdb3_quick_chunks"

echo "==> InfluxDB3 quick: prepare sample"
sample_file=$(ls -1 "${ROOT_DIR}/${INFLUX_LINE_PROTOCOL_DIR}/daily"/fhvhv_*.lp 2>/dev/null | sort | head -n 1 || true)
if [[ -z "${sample_file}" ]]; then
  echo "No daily line protocol files found in ${INFLUX_LINE_PROTOCOL_DIR}/daily" >&2
  exit 1
fi

${COMPOSE} exec -T influxdb3 bash -lc "head -n ${quick_lines} '/workspace/${INFLUX_LINE_PROTOCOL_DIR}/daily/$(basename "${sample_file}")' > '${quick_tmp}'"
echo "==> InfluxDB3 quick: write sample (chunked)"
${COMPOSE} exec -T influxdb3 bash -lc "set -euo pipefail; mkdir -p '${quick_chunk_dir}' && rm -f '${quick_chunk_dir}'/part_*; split -C ${quick_chunk_bytes} -d -a 4 '${quick_tmp}' '${quick_chunk_dir}/part_'; total=\$(ls -1 ${quick_chunk_dir}/part_* | wc -l); echo \"influxdb3 quick: chunks=\${total}\"; for part in ${quick_chunk_dir}/part_*; do influxdb3 write --database '${INFLUX_DATABASE}_daily' --file \"\${part}\" >/dev/null; done"

echo "==> InfluxDB3 quick: run queries"
# Narrow the query window to the sample file's date to avoid file-scan limits.
sample_date=$(basename "${sample_file}" | sed -E 's/^fhvhv_([0-9]{4}-[0-9]{2}-[0-9]{2})\.lp$/\1/')
query_start="${sample_date}T00:00:00Z"
query_end="$(date -u -d "${sample_date} + 1 day" +%Y-%m-%d)T00:00:00Z"
for q in q1_time_range q2_agg q3_filter_agg q4_groupby q5_date_trunc; do
  sql=$(sed -e "s/{START}/${QUERY_START}/g" \
            -e "s/{END}/${QUERY_END}/g" \
            -e "s/{MIN_MILES}/${MIN_MILES}/g" \
            "${ROOT_DIR}/bench/queries/${q}.sql")
  sql=${sql//pickup_datetime/time}
  sql=${sql//FROM trips/FROM ${INFLUX_MEASUREMENT}}
  sql=${sql//${QUERY_START}/${query_start}}
  sql=${sql//${QUERY_END}/${query_end}}
  ${COMPOSE} exec -T influxdb3 influxdb3 query \
    --database "${INFLUX_DATABASE}_daily" \
    --language sql \
    --format jsonl \
    "${sql}" >/dev/null
  echo "==> InfluxDB3 quick: ${q} ok"
done

echo "==> InfluxDB3 quick: done"

echo "==> InfluxDB3 quick: stop services"
pushd "${ROOT_DIR}/bench" >/dev/null
  docker compose -f compose.yml stop influxdb3
popd >/dev/null
