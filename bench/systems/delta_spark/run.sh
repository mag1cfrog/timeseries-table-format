#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)

# shellcheck source=/dev/null
source "${ROOT_DIR}/bench/config.env"

COMPOSE="docker compose -f ${ROOT_DIR}/bench/compose.yml --project-directory ${ROOT_DIR}/bench"

RESULTS_CSV="${RESULTS_RUN_DIR}/delta_spark.csv"
BULK_REL="raw/${TLC_FILE_PREFIX}${START_MONTH}.parquet"

${COMPOSE} exec -T \
  -e RESULTS_CSV="/workspace/${RESULTS_DIR}/$(basename "${RESULTS_RUN_DIR}")/delta_spark.csv" \
  -e MANIFEST_CSV="/workspace/${DATASET_DIR}/manifest.csv" \
  -e DATASET_DIR="/workspace/${DATASET_DIR}" \
  -e BULK_REL="${BULK_REL}" \
  -e DAILY_DIR="/workspace/${DATASET_DIR}/daily" \
  -e QUERIES_DIR="/workspace/bench/queries" \
  -e QUERY_START="${QUERY_START}" \
  -e QUERY_END="${QUERY_END}" \
  -e MIN_MILES="${MIN_MILES}" \
  -e CPU_LIMIT="${CPU_LIMIT}" \
  -e MEM_LIMIT="${MEM_LIMIT}" \
  -e DELTA_TABLE_PATH_BULK="/workspace/${WORK_DIR}/delta_spark/trips_bulk" \
  -e DELTA_TABLE_PATH_DAILY="/workspace/${WORK_DIR}/delta_spark/trips_daily" \
  spark bash -lc "mkdir -p /tmp/.ivy2 /workspace/${RESULTS_DIR}/$(basename "${RESULTS_RUN_DIR}") && chmod -R a+rwX /workspace/${RESULTS_DIR}/$(basename "${RESULTS_RUN_DIR}") /workspace/${WORK_DIR}/delta_spark || true; /opt/spark/bin/spark-submit --conf spark.jars.ivy=/tmp/.ivy2 --driver-memory 4g --executor-memory 4g --conf spark.driver.maxResultSize=512m --conf spark.sql.shuffle.partitions=8 --packages io.delta:delta-spark_2.12:3.1.0 /workspace/bench/systems/delta_spark/run.py"
