#!/usr/bin/env bash
set -euo pipefail

now_ms() {
  if command -v date >/dev/null 2>&1; then
    date +%s%3N
  else
    python - <<'PY'
import time
print(int(time.time() * 1000))
PY
  fi
}

ensure_csv_header() {
  local csv_path="$1"
  if [[ ! -f "$csv_path" ]]; then
    echo "system,test,file,rows,bytes,elapsed_ms,cpu_limit,mem_limit,notes" > "$csv_path"
  fi
}

manifest_lookup() {
  local dataset_dir="$1"
  local rel_path="$2"
  python3 - <<PY
import csv
import os
path = os.path.join("$dataset_dir", "manifest.csv")
rel = "$rel_path"
rows = ""
bytes_ = ""
with open(path, newline="") as f:
    reader = csv.DictReader(f)
    for row in reader:
        if row["path"] == rel:
            rows = row["rows"]
            bytes_ = row["bytes"]
            break
print(rows)
print(bytes_)
PY
}

emit_row() {
  local csv_path="$1"
  local system="$2"
  local test="$3"
  local file="$4"
  local rows="$5"
  local bytes="$6"
  local elapsed_ms="$7"
  local cpu_limit="$8"
  local mem_limit="$9"
  local notes="${10}"

  printf "%s,%s,%s,%s,%s,%s,%s,%s,%s\n" \
    "$system" "$test" "$file" "$rows" "$bytes" "$elapsed_ms" "$cpu_limit" "$mem_limit" "$notes" >> "$csv_path"
}
