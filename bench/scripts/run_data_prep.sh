#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)

# shellcheck source=/dev/null
source "${ROOT_DIR}/bench/config.env"

DATASET_PATH="${ROOT_DIR}/${DATASET_DIR}"
SCRIPT_PATH="${ROOT_DIR}/bench/scripts/prepare_data.py"
IMAGE_NAME="timeseries-table-data-prep:local"

mkdir -p "${DATASET_PATH}"

if [[ -s "${DATASET_PATH}/manifest.csv" ]]; then
  echo "Data prep already complete: ${DATASET_PATH}/manifest.csv"
  exit 0
fi

# Build a small helper image once to avoid reinstalling deps every run.
if ! docker image inspect "${IMAGE_NAME}" >/dev/null 2>&1; then
  docker build -f "${ROOT_DIR}/bench/scripts/data_prep.Dockerfile" -t "${IMAGE_NAME}" "${ROOT_DIR}"
fi

# Run in a Python container to avoid local deps.
docker run --rm \
  -v "${ROOT_DIR}:/workspace" \
  -w /workspace \
  "${IMAGE_NAME}" \
  bash -c "python /workspace/bench/scripts/prepare_data.py \
    --base-url '${TLC_BASE_URL}' \
    --file-prefix '${TLC_FILE_PREFIX}' \
    --start-month '${START_MONTH}' \
    --months '${MONTHS}' \
    --time-column '${TIME_COLUMN}' \
    --local-raw-dir '/workspace/${LOCAL_RAW_DIR}' \
    --dataset-dir '/workspace/${DATASET_DIR}' \
    $(if [[ "${GENERATE_CSV}" == "1" ]]; then echo --generate-csv; fi)"
