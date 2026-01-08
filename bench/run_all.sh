#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)

# shellcheck source=/dev/null
set -a
source "${ROOT_DIR}/bench/config.env"
set +a

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RESULTS_RUN_DIR="${ROOT_DIR}/${RESULTS_DIR}/${TIMESTAMP}"
mkdir -p "${RESULTS_RUN_DIR}"

export RESULTS_RUN_DIR
export MANIFEST_CSV="${ROOT_DIR}/${DATASET_DIR}/manifest.csv"
export DATASET_ROOT="${ROOT_DIR}/${DATASET_DIR}"
export QUERY_START QUERY_END MIN_MILES CPU_LIMIT MEM_LIMIT TIME_COLUMN TABLE_NAME

# 1) Data prep (download + split + csv)
"${ROOT_DIR}/bench/scripts/run_data_prep.sh"

# 2) Start services
pushd "${ROOT_DIR}/bench" >/dev/null
  docker compose -f compose.yml up -d --build postgres timescaledb influxdb3 spark timeseries_table
popd >/dev/null

# 3) Run per-system benchmarks
"${ROOT_DIR}/bench/systems/timeseries_table/run.sh"
"${ROOT_DIR}/bench/systems/postgres/run.sh"
"${ROOT_DIR}/bench/systems/timescale/run.sh"
"${ROOT_DIR}/bench/systems/influxdb3/run.sh"
"${ROOT_DIR}/bench/systems/delta_spark/run.sh"

# 4) Combine results
COMBINED="${RESULTS_RUN_DIR}/combined.csv"
first=1
for csv in "${RESULTS_RUN_DIR}"/*.csv; do
  if [[ "${first}" == "1" ]]; then
    cat "$csv" > "$COMBINED"
    first=0
  else
    tail -n +2 "$csv" >> "$COMBINED"
  fi
done

echo "Results: ${RESULTS_RUN_DIR}"
