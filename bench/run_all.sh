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
echo "==> Phase 1/4: data prep"
"${ROOT_DIR}/bench/scripts/run_data_prep.sh"
echo "==> Phase 1/4: data prep done"

# 2) Run per-system benchmarks (start/stop each service)
echo "==> Phase 2/4: per-system benchmarks"

run_system() {
  local name="$1"
  local service="$2"
  local script="$3"

  echo "==> ${name}: start service"
  pushd "${ROOT_DIR}/bench" >/dev/null
    docker compose -f compose.yml up -d --build "${service}"
  popd >/dev/null

  echo "==> ${name}: run"
  "${script}"
  echo "==> ${name}: done"

  echo "==> ${name}: stop service"
  pushd "${ROOT_DIR}/bench" >/dev/null
    docker compose -f compose.yml stop "${service}"
  popd >/dev/null
}

run_system "timeseries_table" "timeseries_table" "${ROOT_DIR}/bench/systems/timeseries_table/run.sh"
run_system "postgres" "postgres" "${ROOT_DIR}/bench/systems/postgres/run.sh"
run_system "timescale" "timescaledb" "${ROOT_DIR}/bench/systems/timescale/run.sh"
run_system "clickhouse" "clickhouse" "${ROOT_DIR}/bench/systems/clickhouse/run.sh"
run_system "influxdb3" "influxdb3" "${ROOT_DIR}/bench/systems/influxdb3/run.sh"
run_system "delta_spark" "spark" "${ROOT_DIR}/bench/systems/delta_spark/run.sh"

# 3) Combine results
echo "==> Phase 3/4: combine results"
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
echo "==> All done"
