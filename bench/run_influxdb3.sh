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

echo "==> InfluxDB3: data prep"
if [[ ! -f "${MANIFEST_CSV}" ]]; then
  "${ROOT_DIR}/bench/scripts/run_data_prep.sh"
else
  echo "==> InfluxDB3: data prep already complete"
fi

echo "==> InfluxDB3: start services"
pushd "${ROOT_DIR}/bench" >/dev/null
  docker compose -f compose.yml up -d --build influxdb3
popd >/dev/null
echo "==> InfluxDB3: services up"

echo "==> InfluxDB3: run"
"${ROOT_DIR}/bench/systems/influxdb3/run.sh"
echo "==> InfluxDB3: done"

echo "Results: ${RESULTS_RUN_DIR}"
