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

echo "==> ClickHouse: data prep"
if [[ ! -f "${MANIFEST_CSV}" ]]; then
  "${ROOT_DIR}/bench/scripts/run_data_prep.sh"
else
  echo "==> ClickHouse: data prep already complete"
fi

echo "==> ClickHouse: start services"
pushd "${ROOT_DIR}/bench" >/dev/null
  docker compose -f compose.yml up -d --build clickhouse
popd >/dev/null
echo "==> ClickHouse: services up"

echo "==> ClickHouse: run"
"${ROOT_DIR}/bench/systems/clickhouse/run.sh"
echo "==> ClickHouse: done"

echo "==> ClickHouse: stop services"
pushd "${ROOT_DIR}/bench" >/dev/null
  docker compose -f compose.yml stop clickhouse
popd >/dev/null

echo "Results: ${RESULTS_RUN_DIR}"
