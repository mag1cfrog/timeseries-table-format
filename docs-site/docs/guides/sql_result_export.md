# Configure SQL result export

`Session.sql(...)` uses the Arrow C Data Interface to return a `pyarrow.Table`
by default. Most applications should keep this default.

Use `TTF_SQL_EXPORT_MODE` to change the export path before calling
`Session.sql(...)`:

| Value | Behavior |
|---|---|
| `c_stream` | Use Arrow C Stream and report unsupported types as errors. This is the default. |
| `auto` | Try Arrow C Stream, then fall back to Arrow IPC. |
| `ipc` | Always use Arrow IPC serialization. |

For example:

```bash
export TTF_SQL_EXPORT_MODE=auto
```

Arrow C Stream supports common nested types such as `List`, `Struct`, and
`Map`. Types such as `Union` and `ListView` currently require `auto` or `ipc`
mode.

## Diagnose a fallback

Enable a Python warning when `auto` mode falls back to IPC:

```bash
export TTF_SQL_EXPORT_DEBUG=1
```

By default, `auto` preserves the collected batches for a possible fallback. To
avoid that clone, allow the query to run again if Arrow C Stream conversion
fails:

```bash
export TTF_SQL_EXPORT_AUTO_RERUN_FALLBACK=1
```

!!! warning
    Rerunning can produce different results for nondeterministic queries.

These settings do not change `Session.sql_reader(...)`, which returns an Arrow
C Stream-backed `pyarrow.RecordBatchReader`.
