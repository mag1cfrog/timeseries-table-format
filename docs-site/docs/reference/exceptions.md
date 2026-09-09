# Exceptions reference

All library-specific errors inherit from `TimeseriesTableError`, so you can catch any library
error with a single `except ttf.TimeseriesTableError`.

## Exception hierarchy

```
TimeseriesTableError
|-- StorageError                - filesystem or I/O problem
|   `-- VacuumApplyError        - vacuum stopped after apply began
|-- ConflictError               - concurrent table metadata modification
|-- IndexIntervalOverlapError   - incoming interval conflicts with committed data
|-- DuplicateIndexIntervalError - incoming rows duplicate an identity and interval
|-- SchemaMismatchError         - incoming Arrow schema does not match the table schema
`-- DataFusionError             - SQL query failed inside DataFusion
```

## When you'll see each error

**`StorageError`** - raised when the filesystem operation fails. Common causes: the table root
directory doesn't exist, a file is missing, or a permissions problem. The error message includes
the path that caused the problem.

**`VacuumApplyError`** - raised when vacuum apply mode cannot delete a selected file. It inherits
from `StorageError` and carries `path` for the failed table-relative path plus `partial_report` for
the state of every candidate when deletion stopped. Entries marked `deleted` completed before the
failure; entries still marked `removable` were not deleted.

**`IndexIntervalOverlapError`** - raised by `append(...)` when an incoming row uses an identity
and logical index interval already present in committed data. The exception carries:

- `segment_path` - the generated table-relative path for the rejected segment
- `conflict_count` - the number of conflicting intervals, or identity and interval pairs
- `example_identity` - one complete identity as a dictionary, or `None` for a table without
  entity columns
- `example_index_interval` - one conflicting logical interval, such as
  `[-20, -10)`, `[50460, 50470)`, or
  `[1970-01-01T00:00:00Z, 1970-01-01T01:00:00Z)`

**`DuplicateIndexIntervalError`** - raised when two rows in one incoming append use the same
complete identity and logical index interval. It carries `segment_path`, `example_identity`, and
`example_index_interval`. It does not expose `conflict_count`.

These attributes are stable diagnostics. Inspect them directly instead of parsing exception
messages. See [Index granularity and conflicts](../concepts/index_granularity_and_conflicts.md)
for the uniqueness rule.

**`SchemaMismatchError`** - raised when an Arrow source you try to append has a schema that
conflicts with the table's established schema (set on the first successful append), or when
`add_columns(...)` violates the nullable-addition contract. It also covers invalid destination
selections and source/schema incompatibilities reported by `update_rows(...)`.

**`ConflictError`** - raised when a concurrent modification to the table metadata is detected.
In typical single-process usage this is rare; it can happen if two processes are appending to the
same table root simultaneously or adding columns through a stale handle. The exception carries
`expected` and `found` versions. Reopen and reconcile before retrying.

For `update_rows`, `expected` is the explicit source version. `found` is the selected handle's
version when that differs, or the observed published version when the handle matches. A create-only
commit race that has no observed version remains `StorageError` with path context.

**`TimeseriesTableError`** - also preserves protocol incompatibility and ambiguous commit
diagnostics. An ambiguous outcome must not be treated as guaranteed rollback; reopen and reconcile
the log before retrying.

## Keyed update diagnostics

Update errors include `table_root`, including representation errors raised inside the operation.
Invalid Python column/version representations use `TypeError`; out-of-range integer versions use
`ValueError`. Arrow export/import failures follow append's existing boundary behavior and retain
available upstream causes. Reader failures during consumption preserve their source diagnostic.

When the core rejects a complete key, the exception is exactly `TimeseriesTableError`. It is
distinct from append's interval-overlap exceptions and carries these conditional attributes:

| Attribute | Meaning |
| --- | --- |
| `reason` | `duplicate_source_key`, `unmatched_source_key`, `ambiguous_target_key`, or `null_identity` |
| `input_rows_seen` | Source rows observed before rejection |
| `observed_violations` | Violations observed, not a total for unread input |
| `example_key` | A complete key dictionary using configured column names |

Key dictionary values are Python strings, integers, or `None`. Timestamp components are
`pyarrow.TimestampScalar` values in the canonical unit/timezone, preserving nanoseconds.
Unsigned values retain their full range. A table without entity columns includes only the
ordered-index entry. These attributes are present when the structured core key diagnostic is
available; an Arrow reader can reject malformed data before it reaches key validation.

Storage errors retain applicable paths. Cleanup failures preserve their diagnostics alongside
the primary failure. Ambiguous commits retain the base exception and full diagnostic, even when
they contain nested storage failures. See [Update values by row key](../guides/update_rows.md).

**`DataFusionError`** - raised when `Session.sql(...)` or `Session.sql_reader(...)` encounters a
SQL error (syntax error, type error, unknown column, etc.).

---

## API reference

::: timeseries_table_format.TimeseriesTableError
    options:
      show_source: false

::: timeseries_table_format.StorageError
    options:
      show_source: false

::: timeseries_table_format.VacuumApplyError
    options:
      show_source: false

::: timeseries_table_format.ConflictError
    options:
      show_source: false

::: timeseries_table_format.IndexIntervalOverlapError
    options:
      show_source: false

::: timeseries_table_format.DuplicateIndexIntervalError
    options:
      show_source: false

::: timeseries_table_format.SchemaMismatchError
    options:
      show_source: false

::: timeseries_table_format.DataFusionError
    options:
      show_source: false
