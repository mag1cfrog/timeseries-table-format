# `timeseries-table-format` (Python)

Python bindings for the `timeseries-table-format` Rust project.

- PyPI package name: `timeseries-table-format`
- Python import name: `timeseries_table_format`

This is early-stage and currently focused on local development and dogfooding.

## Development install (editable)

Prereqs:
- Rust toolchain installed
- Python 3.12+
- `uv` installed

From the repo root:

```bash
cd python
uv venv -p 3.12 .venv
uv pip install -p .venv/bin/python -e .
```

## Smoke test

```bash
.venv/bin/python -c "import timeseries_table_format as m; print(m.__version__); print(m.Session); print(m.TimeSeriesTable)"
```

## SQL queries (Session)

Run SQL with DataFusion and get a `pyarrow.Table` back:

```python
import timeseries_table_format as ttf

sess = ttf.Session()
out = sess.sql("select 1 as x")
```

### Parameterized queries

Use DataFusion placeholders:

- Positional placeholders: `$1`, `$2`, ...
- Named placeholders: `$name`

Examples:

```python
sess.sql("select 1 as x where 1 = $1", params=[1])
sess.sql("select 1 as x where 1 = $a", params={"a": 1})
```

If a placeholder appears in a `SELECT` projection without type context, you may need an explicit cast:

```python
sess.sql("select cast($1 as bigint) as x", params=[1])
```

## Troubleshooting

- If you want to rebuild the Rust extension after changing Rust code, re-run:
  - `uv pip install -p .venv/bin/python -e .`
