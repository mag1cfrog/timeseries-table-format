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

## Troubleshooting

- If you want to rebuild the Rust extension after changing Rust code, re-run:
  - `uv pip install -p .venv/bin/python -e .`
