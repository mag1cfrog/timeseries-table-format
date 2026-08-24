# Temporary format-v6 to protocol-v7 migration runbook

Delete this runbook, the migration script, and their migration-only tests after the
internal user accepts the migration and before the 0.5 release.

## Prerequisites

- Use a revision containing the merged protocol-v7 implementation from issue #399.
- Stop every reader and writer before the first checksum and keep them stopped
  through cutover.
- Keep the source directory untouched as the rollback artifact.
- Use a new destination name whose parent has enough free space for a full copy.
- Keep private paths, data, credentials, and validation results in private notes.

Set the private paths and record the source commit before migration:

```sh
export TTF_SOURCE_TABLE=/absolute/path/to/source-v6
export TTF_DESTINATION_TABLE=/absolute/path/to/destination-v7
export TTF_EXPECTED_CURRENT=$(tr -d '[:space:]' < "$TTF_SOURCE_TABLE/_timeseries_log/CURRENT")
git rev-parse HEAD
```

Record the resolved source path, `TTF_EXPECTED_CURRENT`, metadata, logical schema,
index specification, live segment paths, row counts, index bounds, and coverage
pointer in the private notes. In the old internal-user environment, record the full
row count and a deterministic digest produced by the same complete, explicitly
ordered SQL query that will be used after migration. Also record results for
representative range queries and a duplicate-per-entity-interval query expected to
return zero rows.

## Migrate

Run once from the repository root:

```sh
python scripts/migrate_table_v6_to_protocol_v7.py \
  "$TTF_SOURCE_TABLE" "$TTF_DESTINATION_TABLE"
```

The report must identify format v6 and protocol v7, show the expected `CURRENT`,
commit and transformed-action counts, byte totals for preserved files, identical
non-log SHA-256 manifests, successful atomic publication, and follow-up commands.
Any failure means no destination is usable; investigate before retrying with a new
destination path.

## Validate protocol v7

Run the Rust core check printed by the migration. Add a representative half-open
coverage range. For an entity table, provide the complete entity using the
`EntityValue` JSON wire shape, for example
`{"symbol":{"type":"utf8","value":"PRIVATE_VALUE"}}`.

```sh
TTF_MIGRATED_TABLE="$TTF_DESTINATION_TABLE" \
TTF_EXPECTED_CURRENT="$TTF_EXPECTED_CURRENT" \
TTF_COVERAGE_START='PRIVATE_START' \
TTF_COVERAGE_END='PRIVATE_END' \
TTF_COVERAGE_ENTITY_JSON='PRIVATE_ENTITY_JSON_OR_OMIT_FOR_NO_ENTITIES' \
cargo test -p timeseries-table-format \
  --test migrate_table_v6_to_protocol_v7 \
  validate_migrated_table_from_environment -- \
  --ignored --exact --nocapture
```

Compare the printed index, logical schema, segments, bounds, row counts, and
coverage pointer with the private source-state record.

Use the internal user's Python environment to open and query the destination. Set
`TTF_VALIDATION_SQL` to the same full, deterministic, explicitly ordered query used
before migration, and compare both outputs with the private expected values:

```sh
TTF_MIGRATED_TABLE="$TTF_DESTINATION_TABLE" \
TTF_EXPECTED_CURRENT="$TTF_EXPECTED_CURRENT" \
TTF_EXPECTED_ROWS='PRIVATE_ROW_COUNT' \
TTF_EXPECTED_DIGEST='PRIVATE_SHA256' \
TTF_VALIDATION_SQL='PRIVATE_SELECT_WITH_COMPLETE_ORDER_BY' \
python - <<'PY'
import hashlib
import os

import pyarrow as pa
import timeseries_table_format as ttf

root = os.environ["TTF_MIGRATED_TABLE"]
table = ttf.TimeSeriesTable.open(root)
assert table.version() == int(os.environ["TTF_EXPECTED_CURRENT"])
session = ttf.Session()
session.register_tstable("migrated", root)
result = session.sql(os.environ["TTF_VALIDATION_SQL"])
sink = pa.BufferOutputStream()
with pa.ipc.new_stream(sink, result.schema) as writer:
    writer.write_table(result)
digest = hashlib.sha256(sink.getvalue().to_pybytes()).hexdigest()
assert result.num_rows == int(os.environ["TTF_EXPECTED_ROWS"])
assert digest == os.environ["TTF_EXPECTED_DIGEST"]
print({"version": table.version(), "index": table.index_spec(), "rows": result.num_rows, "sha256": digest})
PY
```

Run the recorded representative range queries and the duplicate-per-entity-index-
interval query against `migrated` through the same Python session. All must match
the source record, and the duplicate query must still return zero rows.

Confirm the source stayed byte-for-byte unchanged by comparing a private SHA-256
manifest captured before downtime with a fresh manifest. Do not publish the
manifest because paths can be sensitive.

Finally test writes only on a disposable copy. Prepare a valid non-overlapping
Parquet append file for this table, then run:

```sh
export TTF_DISPOSABLE_TABLE="$TTF_DESTINATION_TABLE.append-test"
test ! -e "$TTF_DISPOSABLE_TABLE"
cp -a "$TTF_DESTINATION_TABLE" "$TTF_DISPOSABLE_TABLE"
cargo run -p timeseries-table-format --features cli --bin tstable -- \
  append --table "$TTF_DISPOSABLE_TABLE" --parquet PRIVATE_APPEND.parquet
TTF_MIGRATED_TABLE="$TTF_DISPOSABLE_TABLE" \
TTF_EXPECTED_CURRENT="$((TTF_EXPECTED_CURRENT + 1))" \
cargo test -p timeseries-table-format \
  --test migrate_table_v6_to_protocol_v7 \
  validate_migrated_table_from_environment -- \
  --ignored --exact --nocapture
```

Re-run the full scan against the disposable table and verify the expected appended
rows. Do not append to the validated migration destination.

## Cut over, rollback, and retire

Point the internal user's configuration to `TTF_DESTINATION_TABLE` only after every
check passes. If validation or initial use fails, stop the user and point it back to
`TTF_SOURCE_TABLE`; do not reverse-convert the destination.

Retain the source until the internal user explicitly accepts the migrated table and
the agreed backup-retention window passes. Record acceptance privately, then remove
the script, this runbook, `scripts/test_migrate_table_v6_to_protocol_v7.py`, and
`crates/timeseries-table-format/tests/migrate_table_v6_to_protocol_v7.rs` before
0.5. Preserve the implementation and execution record in Git and issue #400.
