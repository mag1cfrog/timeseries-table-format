# `timeseries-table-format` (Python)

Python bindings for the `timeseries-table-format` Rust project.

- PyPI package name: `timeseries-table-format`
- Python import name: `timeseries_table_format`

This is early-stage and currently focused on local development and dogfooding.
v0 is local-filesystem-only (no S3/object store backend yet).

## Quick start (from the repo root)

Prereqs:
- Rust toolchain installed
- Python 3.10+ (CI targets 3.10–3.14; examples below use 3.12)
- `uv` installed

```bash
uv venv -p 3.12 python/.venv
uv pip install -p python/.venv/bin/python -e python --group dev
```

Smoke test:

```bash
python/.venv/bin/python -c "import timeseries_table_format as m; print(m.__version__); print(m.Session); print(m.TimeSeriesTable)"
```

Run tests (pytest):

Tests are end-to-end and generate tiny Parquet files on the fly via `pyarrow` (no fixtures, no network at runtime).

```bash
python/.venv/bin/python -m pytest
```

Run the example script:

The repo includes a copy-pastable end-to-end script under `python/examples/`:

```bash
python/.venv/bin/python python/examples/create_append_sql.py
```

### Alternative: build with `maturin` directly

If you prefer calling `maturin` yourself (instead of via `uv pip install`), run:

```bash
cd python
uv venv -p 3.12 .venv
uv pip install -p .venv/bin/python pyarrow --group dev
uv run -p .venv/bin/python maturin develop -m pyproject.toml
.venv/bin/python -m pytest
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

Supported Python parameter value types: `None`, `bool`, `int` (i64 range), `float`, `str`, `bytes`.

If a placeholder appears in a `SELECT` projection without type context, you may need an explicit cast:

```python
sess.sql("select cast($1 as bigint) as x", params=[1])
```

## Troubleshooting

- If you want to rebuild the Rust extension after changing Rust code, re-run:
  - `uv pip install -p python/.venv/bin/python -e python`
