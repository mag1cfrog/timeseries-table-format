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

Opening, refreshing, scanning, and querying use the reader check. Append, optimize, and other
mutations use the writer check before inspecting input or creating artifacts. A reader feature
does not need to be repeated in the writer list because every write performs both checks.

Rust callers can inspect `protocol_version()`, `required_reader_features()`, and
`required_writer_features()` through `table.state().table_meta`. Higher-level integrations can
call `TimeSeriesTable::ensure_write_compatible()` before preparing mutation input; the mutation
itself checks again.

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
