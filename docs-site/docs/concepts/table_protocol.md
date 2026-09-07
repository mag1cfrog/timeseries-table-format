# Table protocol compatibility

Protocol metadata lets the software evolve without tying every table to one package release.
A client can sometimes keep reading a table even when it can no longer write to it.

## Keep the versions separate

- The **package version** identifies a Rust crate, Python package, or CLI release.
- The **commit version** is the table's increasing transaction number in `CURRENT`.
- The **protocol version** identifies the foundational metadata and transaction-log grammar.
- The **required feature sets** describe optional behavior needed for reading or writing.

These values are independent. Installing package version 0.5 does not create protocol version 0.5,
and adding an optional feature does not normally change the protocol version.

## Protocol 7 metadata

Every `UpdateTableMeta` replacement contains this compatibility header:

```json
{
  "protocol_version": 7,
  "required_reader_features": [],
  "required_writer_features": []
}
```

All three fields are required. Feature lists cannot be null, and an empty list means that only
baseline protocol-7 behavior is required. New tables start with both lists empty.

## Read and write checks

A read is allowed when the table uses protocol 7 and every required reader feature is supported.
A write must pass the read check and support every required writer feature.

| Table requirements | Read | Write |
|---|---|---|
| No unknown features | Allowed | Allowed |
| Unknown reader feature | Rejected | Rejected |
| Unknown writer feature only | Allowed | Rejected |
| Unknown reader and writer features | Rejected | Rejected |

Opening, refreshing, scanning, and querying use the reader check. Append, add-columns, optimize, and other
mutations use the writer check before inspecting input or creating artifacts. A reader feature
does not need to be repeated in the writer list because every write performs both checks.

Rust callers can inspect `protocol_version()`, `required_reader_features()`, and
`required_writer_features()` through `table.state().table_meta`. Each mutation checks compatibility
before preparing its input or creating artifacts.

## Nullable-column addition

The `schema_add_columns` **reader feature** permits adding nullable top-level fields to an
established canonical schema. Existing rows return null for those fields. The first successful
addition declares the feature atomically in its `UpdateTableMeta` replacement; protocol version
7 and existing reader/writer requirements remain unchanged. No writer-feature entry is needed.
Installing a supporting client, creating a table, and ordinary appends do not activate it.

Rust callers use `TimeSeriesTable::add_columns(Vec<LogicalField>)`:

```rust
use timeseries_table_format::metadata::logical_schema::{LogicalDataType, LogicalField};

let version = table.add_columns(vec![LogicalField {
    name: "score".to_string(),
    data_type: LogicalDataType::Float64,
    nullable: true,
}]).await?;
```

The request must contain at least one field. An explicitly schematized empty table is eligible;
a schemaless table must establish its schema first. Names are exact and case-sensitive: blank
names, duplicates, collisions with existing fields, and non-nullable additions are rejected.
Valid names retain their whitespace, and dots are literal characters rather than nested paths.
Use SQL identifier quoting for names that need it.

Types must round-trip exactly through the existing logical, Arrow, and Parquet schema model.
This includes complete supported structs, lists, and maps as new nullable top-level fields;
it excludes legacy `Int96`, placeholder `Other` types, invalid parameters, and definitions whose
names or types would change during conversion. Existing fields keep their order, names, types,
and nullability. Index and entity-key definitions cannot change. Replay rejects undeclared
additions, removed schemas, and non-additive changes, including invalid intermediate metadata
replacements in a commit.

An addition publishes one metadata-only commit. It does not read or rewrite historical Parquet
data or coverage objects, and leaves segment metadata and coverage pointers intact. Appends to
an evolved table may supply the new fields or omit nullable payload fields; the writer fills
omissions with null and writes the complete canonical schema. Keys remain required. Optimization
also writes the canonical schema while preserving historical nulls.

The operation uses the handle's selected version without refreshing or retrying. On success it
returns the committed version and updates only that handle. Concurrent additions, appends, or
optimization may conflict, even when their changes seem disjoint. `AddColumnsError::Commit`
preserves `CommitError::Conflict` for an observed version mismatch and
`CommitError::Storage` with `StorageError::AlreadyExists` for a create-only race. Definite
prepublication failures leave the handle and existing objects unchanged. On
`CommitError::AmbiguousOutcome`, reopen and reconcile the log before retrying; do not assume
success or rollback.

DataFusion captures the schema when `TsTableProvider` is registered. If the selected scan
snapshot has a different schema, planning fails with a schema-changed diagnostic directing the
caller to re-register the table. A reference to a newly added column may fail earlier during
normal name resolution. Refresh or reopen the Rust handle, construct a new provider, and replace
the registration. Ordinary appends and optimization with the same schema still refresh normally.
Existing Rust handles, native scans, and already built physical plans retain their snapshots.

This feature does not permit dropping, renaming, reordering, or retyping columns, nested-field
edits, defaults, automatic schema merging, or backfilling values. Its meaning will not be expanded
to cover those operations.

## Adding a feature

Feature identifiers are stable ASCII snake-case names matching `[a-z][a-z0-9_]*`. Lists reject
duplicates and serialize in sorted order.

Requirements are monotonic across table history: metadata may add reader or writer features, but
it may not remove them or decrease the protocol version. The first commit that uses a feature must
also declare it. Declaration and first use are atomic even when the metadata action appears after
the dependent action in the JSON array.

## Parsing before decoding

The log loader inspects raw commit JSON for protocol metadata before decoding typed actions. This
ensures an unknown reader feature is rejected before its dependent payload is interpreted.

When no unsupported reader feature is declared, read paths may ignore unknown writer-only fields
or actions. Known actions with malformed data remain corruption errors. A payload declared as
required for reading is never silently ignored.

## Feature or protocol bump?

Add a feature when protocol 7 can still locate and read the compatibility header and safely skip
unsupported behavior. Optional actions, metadata, indexes, checkpoints, compression choices, and
storage-specific write rules normally fit this model.

Bump the protocol only when an older client cannot reliably find or interpret the compatibility
header itself. Examples include incompatible changes to log framing, commit discovery, or the
location or type of the protocol fields. Package releases and ordinary feature work do not require
a protocol bump.
