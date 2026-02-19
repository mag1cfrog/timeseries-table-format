# timeseries-table-format (Python)

Python-first workflow for managing local, append-only time-series tables stored as Parquet
segments on disk, with SQL querying (DataFusion) that returns `pyarrow.Table`.

- PyPI: `timeseries-table-format`
- Import: `timeseries_table_format`
- Docs: https://mag1cfrog.github.io/timeseries-table-format/

v0 is local-filesystem-only (no S3/object storage backend yet).

## Install

```bash
pip install timeseries-table-format
```

Requires: Python 3.10+. `pyarrow` is installed automatically (dependency: `pyarrow>=23.0.0`).
If `pip` tries to build from source (Rust errors), see Troubleshooting below.

## Verify installation

```python
import timeseries_table_format as ttf

out = ttf.Session().sql("select 1 as x")
print(type(out))  # pyarrow.Table
```

## Return type and interop

`Session.sql(...)` returns a `pyarrow.Table`.

- Polars: `pip install polars`, then `polars.from_arrow(out)`

## Notebook display (Jupyter/IPython)

In IPython/Jupyter (including VS Code notebooks), `pyarrow.Table` results will display as a bounded HTML preview by default (the return type is still a real `pyarrow.Table`).

- Defaults: `max_rows=20` (head/tail), `max_cols=50` (left/right), `max_cell_chars=2000`
- Opt-out: set `TTF_NOTEBOOK_DISPLAY=0` before importing `timeseries_table_format`, or call `timeseries_table_format.disable_notebook_display()`
- Configure: call `timeseries_table_format.enable_notebook_display(max_rows=..., max_cols=..., max_cell_chars=..., align=...)`
- Config file (TOML): set `TTF_NOTEBOOK_CONFIG=path/to/ttf.toml` before importing `timeseries_table_format` (or call `timeseries_table_format.load_notebook_display_config("path/to/ttf.toml")`)
  (On Python 3.10, install `tomli` to enable TOML parsing.)
- Alignment: `align="right"` (default) or `align="auto"` (strings left, numbers right); auto-enable can be configured with `TTF_NOTEBOOK_ALIGN=auto|left|right`
- Cells are visually clipped to a bounded column width with an ellipsis indicator; copying a cell copies the underlying value (up to `max_cell_chars`).

Example `ttf.toml`:

```toml
[notebook_display]
max_rows = 20
max_cols = 50
max_cell_chars = 2000
align = "auto"
```

## Maintainers: releasing the Python package

The PyPI package version is derived from `crates/timeseries-table-python/Cargo.toml` (via maturin).
If you change the pure-Python sources under `python/src/` (or `python/pyproject.toml` / `python/README.md`),
CI will automatically update `crates/timeseries-table-python/python-src.stamp` on PRs from branches in
this repository.

If you need to update it locally (e.g. working on a fork, or before pushing), run:

```bash
python3 scripts/update_python_wheel_stamp.py
```

If your development environment uses the repo venv, you can also run:

```bash
python/.venv/bin/python scripts/update_python_wheel_stamp.py
```

CI enforces the stamp, and it helps the release automation notice python-only changes for version bumps.

## Quickstart: create → append → query

```python
import tempfile
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

import timeseries_table_format as ttf

with tempfile.TemporaryDirectory() as d:
    table_root = Path(d) / "my_table"

    tbl = ttf.TimeSeriesTable.create(
        table_root=str(table_root),
        time_column="ts",
        bucket="1h",
        entity_columns=["symbol"],
        timezone=None,
    )

    seg_path = table_root / "incoming" / "prices.parquet"
    seg_path.parent.mkdir(parents=True, exist_ok=True)

    pq.write_table(
        pa.table(
            {
                "ts": pa.array([0, 3_600 * 1_000_000, 7_200 * 1_000_000], type=pa.timestamp("us")),
                "symbol": pa.array(["NVDA", "NVDA", "NVDA"], type=pa.string()),
                "close": pa.array([10.0, 20.0, 30.0], type=pa.float64()),
            }
        ),
        str(seg_path),
    )

    tbl.append_parquet(str(seg_path))

    sess = ttf.Session()
    sess.register_tstable("prices", str(table_root))

    out = sess.sql("select ts, symbol, close from prices order by ts")
    print(out)  # pyarrow.Table
```

> **Bucket size (important):** `bucket=1h` does **not** resample your data. It defines the time grid used for overlap detection and coverage tracking.
> Example: with `bucket=1h`, timestamps `10:05` and `10:55` fall into the same bucket (10:00–11:00).
> See https://mag1cfrog.github.io/timeseries-table-format/concepts/bucketing_and_overlap/

## Join multiple tables

```python
# Aligned with python/examples/register_and_join_two_tables.py
import tempfile
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

import timeseries_table_format as ttf

with tempfile.TemporaryDirectory() as d:
    base_dir = Path(d)

    prices_root = base_dir / "prices_tbl"
    prices = ttf.TimeSeriesTable.create(
        table_root=str(prices_root),
        time_column="ts",
        bucket="1h",
        entity_columns=["symbol"],
        timezone=None,
    )
    prices_seg = base_dir / "prices.parquet"
    pq.write_table(
        pa.table(
            {
                "ts": pa.array([0, 3_600 * 1_000_000], type=pa.timestamp("us")),
                "symbol": pa.array(["NVDA", "NVDA"], type=pa.string()),
                "close": pa.array([1.0, 2.0], type=pa.float64()),
            }
        ),
        str(prices_seg),
    )
    prices.append_parquet(str(prices_seg))

    volumes_root = base_dir / "volumes_tbl"
    volumes = ttf.TimeSeriesTable.create(
        table_root=str(volumes_root),
        time_column="ts",
        bucket="1h",
        entity_columns=["symbol"],
        timezone=None,
    )
    volumes_seg = base_dir / "volumes.parquet"
    pq.write_table(
        pa.table(
            {
                "ts": pa.array([0, 3_600 * 1_000_000], type=pa.timestamp("us")),
                "symbol": pa.array(["NVDA", "NVDA"], type=pa.string()),
                "volume": pa.array([10, 20], type=pa.int64()),
            }
        ),
        str(volumes_seg),
    )
    volumes.append_parquet(str(volumes_seg))

    sess = ttf.Session()
    sess.register_tstable("prices", str(prices_root))
    sess.register_tstable("volumes", str(volumes_root))

    out = sess.sql(
        """
        select p.ts as ts, p.symbol as symbol, p.close as close, v.volume as volume
        from prices p
        join volumes v
        on p.ts = v.ts and p.symbol = v.symbol
        order by p.ts
        """
    )
    print(out)  # pyarrow.Table
```

## Parameterized queries

DataFusion infers placeholder types from context when possible (e.g. in `WHERE` clauses).
If you use placeholders in a `SELECT` projection without type context, you may need an explicit cast.

```python
# Aligned with python/examples/parameterized_queries.py
import timeseries_table_format as ttf

sess = ttf.Session()

out_positional = sess.sql(
    "select cast($1 as bigint) as x, cast($2 as varchar) as y",
    params=[1, "hello"],
)
out_named = sess.sql(
    "select cast($a as bigint) as x, cast($b as varchar) as y",
    params={"a": 2, "b": "world"},
)

print(out_positional)
print(out_named)
```

## Building from source (contributors)

Prereqs:
- Rust toolchain installed
- Python 3.10+ (CI targets 3.10–3.14; examples below use 3.12)
- `uv` installed

From the repo root:

```bash
uv venv -p 3.12 python/.venv
uv pip install -p python/.venv/bin/python -e python --group dev
python/.venv/bin/python -m pytest
```

Type checking (ty):

```bash
uvx ty check --project python
```

Alternative (uses the `python/` dev environment):

```bash
cd python
uv run ty check
```

Alternative: build with `maturin` directly:

```bash
cd python
uv venv -p 3.12 .venv
uv pip install -p .venv/bin/python pyarrow --group dev
uv run -p .venv/bin/python maturin develop
.venv/bin/python -m pytest
```

## Benchmark: SQL conversion (IPC vs C Stream)

`Session.sql(...)` returns results as a `pyarrow.Table`.

By default, results are exported via the Arrow C Data Interface (C Stream) when supported, and
fall back to an in-memory Arrow IPC stream otherwise. To compare the two paths and estimate the
conversion overhead, run:

```bash
cd python
uv pip install -p .venv/bin/python numpy
uv run -p .venv/bin/python maturin develop --features test-utils
.venv/bin/python bench/sql_conversion.py --target-ipc-gb 2
```

Environment variables (useful for debugging and benchmarks):
- `TTF_SQL_EXPORT_MODE=auto|ipc|c_stream` (default: `c_stream`)
- `TTF_SQL_EXPORT_DEBUG=1` to emit a debug warning when `auto` falls back from C Stream → IPC
- `TTF_SQL_EXPORT_AUTO_RERUN_FALLBACK=1` to re-run the SQL query on C Stream failure in `auto` mode (avoids cloning batches on the hot path, but may change results for non-deterministic queries)

Optional: benchmark IPC ZSTD compression (requires building with `ipc-zstd`):

```bash
uv run -p .venv/bin/python maturin develop --features test-utils,ipc-zstd
.venv/bin/python bench/sql_conversion.py --target-ipc-gb 2 --ipc-compression zstd
```

The script can print a human-friendly terminal summary (`--summary`) and/or write a JSON payload
to a file (`--json path`). It reports separate timings for:
- end-to-end `Session.sql(...)`
- Rust-side query+IPC encode (`_native._testing._bench_sql_ipc`)
- Rust-side query+C Stream export (`_native._testing._bench_sql_c_stream`)
- Python-side decode/import

Large targets can require high peak RAM (IPC bytes + decoded Table + intermediate buffers). Start with
`--target-ipc-gb 2` and scale up to `3` or `6` on a machine with plenty of memory.

If you hit `Disk quota exceeded`, pass `--tmpdir /path/with/more/space` (the bench uses a temporary
directory and cleans it up on exit).

## Troubleshooting

- `pip` is building from source / fails with Rust errors: no wheel is available for your platform/Python; install Rust and retry, or use a supported Python/platform combination.
- `DataFusionError` about an unknown table name: call `sess.register_tstable("name", "/path/to/table")` first; use `sess.tables()` to list registrations.
- Append fails with a time column error: the timestamp column must be an Arrow `timestamp(...)`, and the unit should remain consistent across segments (e.g. `timestamp("us")`).
- `SchemaMismatchError` on append: the new Parquet segment schema must match the table's adopted schema (column names and types).
- SQL errors / parameter placeholders: try an explicit `CAST(...)` for placeholders used in `SELECT` projections.
