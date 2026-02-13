# Troubleshooting

## `pip install` tries to build from source

If pip can’t find a compatible wheel for your platform/Python version, it may try to build from
source, which requires a Rust toolchain.

- Prefer a supported Python version (project requires Python 3.10+)
- If you must build from source, install Rust (stable) and retry

## SQL placeholder type errors

If you use placeholders in a `SELECT` projection, DataFusion might not infer the type without an
explicit cast.

Example:

```python
sess.sql("select cast($1 as bigint) as x", params=[1])
```

## Overlap errors on append

If `append_parquet(...)` raises `CoverageOverlapError`, your segment overlaps existing coverage at
the configured bucket granularity.

- Use a finer `bucket=...` if appropriate for your data
- Ensure you aren’t accidentally re-ingesting the same time window

