# timeseries-table-format (Python)

Python-first workflow for managing local, append-only time-series tables stored as Parquet
segments on disk, with SQL querying (DataFusion) that returns `pyarrow.Table`.

Each table has one ascending chronological index: a physical Timestamp or an Int64/UInt64
logical clock with application-defined units.

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

## Development: build and test locally

To run the Python test suite against the local editable Rust extension, build the extension into
the repo virtualenv first, then run `pytest` with the same interpreter.

```bash
cd crates/timeseries-table-python
uv sync --group dev
uv run maturin develop --features test-utils
uv run pytest -q
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

The PyPI package version is derived from the local `Cargo.toml` via maturin.

Published wheels use CPython's stable ABI with a Python 3.10 minimum and carry the
`cp310-abi3` tag. Release CI builds one wheel for Linux x86_64, macOS arm64, macOS
x86_64, and Windows x86_64, then tests the same Linux wheel on CPython 3.10 through
3.14. These wheels target ordinary GIL-enabled CPython. Free-threaded CPython, PyPy,
and other Python implementations are not supported by this wheel policy.

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
        index_column="ts",
        index_type="timestamp",
        bucket="1h",
        entity_columns=["exchange_id", "symbol"],
        timezone=None,
    )

    seg_path = table_root / "incoming" / "prices.parquet"
    seg_path.parent.mkdir(parents=True, exist_ok=True)

    pq.write_table(
        pa.table(
            {
                "ts": pa.array([0, 3_600 * 1_000_000, 7_200 * 1_000_000], type=pa.timestamp("us")),
                "exchange_id": pa.array([1, 1, 1], type=pa.int32()),
                "symbol": pa.array(["NVDA", "NVDA", "NVDA"], type=pa.string()),
                "close": pa.array([10.0, 20.0, 30.0], type=pa.float64()),
            }
        ),
        str(seg_path),
    )

    tbl.append_parquet(str(seg_path))

    sess = ttf.Session()
    sess.register_tstable("prices", str(table_root))

    out = sess.sql(
        "select ts, exchange_id, symbol, close "
        "from prices order by exchange_id, symbol, ts"
    )
    print(out)  # pyarrow.Table
```

`entity_columns` defines independent identities within one logical table. Different identities may
reuse the same timestamp bucket, and one Parquet file may contain multiple identities. An append
is rejected only when the same complete identity already covers a bucket. In SQL, entity columns
are ordinary columns that can be filtered or grouped.

Entity columns support Arrow `string`, `large_string`, `int32`, `int64`, and `uint64`. Actual
entity values must be non-null. Composite identity components follow the configured
`entity_columns` order. Signed and unsigned integers keep their exact types and are not compared
as strings. Entity column names must be unique and cannot also be the ordered index column.
Unsupported domains and mismatched types are rejected rather than cast.

Parquet rows need not be sorted by the ordered index. SQL query results are unordered unless the
query uses `ORDER BY`.

## Optimize mixed-entity segments

Mixed-entity segments are valid input. Entity-layout optimization is explicit and optional:

```python
report = tbl.optimize()
print(report.source_segments_replaced, report.replacement_segments_written)
```

For each mixed source segment, optimization writes one replacement segment per complete entity
identity. It preserves logical rows, schema, and per-entity coverage. If no mixed live segments
need rewriting, `report.no_op` is `True`, `starting_version` equals `committed_version`, and no new
table version is created.

Optimization may change physical row order.

Optimization does not combine small files or accept a target file size. Replaced source files may
remain on disk until a future vacuum operation removes unreferenced files.

> **Bucket size (important):** `bucket=1h` does **not** resample your data. It defines the time grid used for overlap detection and coverage tracking.
> Example: with `bucket=1h`, timestamps `10:05` and `10:55` fall into the same bucket (10:00–11:00).
> See https://mag1cfrog.github.io/timeseries-table-format/concepts/bucketing_and_overlap/

## Integer chronological indexes

Integer Parquet columns must be Arrow `int64` or `uint64` exactly; signedness is not converted.

```python
import timeseries_table_format as ttf

signed = ttf.TimeSeriesTable.create(
    table_root="signed_ticks",
    index_column="tick",
    index_type="int64",
    bucket_width=10,
)
signed.append_parquet("signed.parquet")

unsigned = ttf.TimeSeriesTable.create(
    table_root="unsigned_counters",
    index_column="counter",
    index_type="uint64",
    bucket_width=100,
)
unsigned.append_parquet("unsigned.parquet")
assert unsigned.index_spec() == {
    "column": "counter",
    "entity_columns": [],
    "kind": "uint64",
    "bucket_width": 100,
}

session = ttf.Session()
session.register_tstable("signed_ticks", "signed_ticks")
session.register_tstable("unsigned_counters", "unsigned_counters")
negative = session.sql(
    "SELECT tick FROM signed_ticks WHERE tick >= -20 AND tick < 0 ORDER BY tick"
)
large = session.sql(
    "SELECT counter FROM unsigned_counters "
    "WHERE counter >= CAST('9223372036854775808' AS BIGINT UNSIGNED) "
    "ORDER BY counter"
)
```

See the [ordered-index migration table](https://github.com/mag1cfrog/timeseries-table-format#migrating-timestamp-only-callers)
when upgrading timestamp-only code.

## Join multiple tables

```python
# Aligned with examples/register_and_join_two_tables.py
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
        index_column="ts",
        index_type="timestamp",
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
        index_column="ts",
        index_type="timestamp",
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
# Aligned with examples/parameterized_queries.py
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
cd crates/timeseries-table-python
uv sync --group dev
uv run maturin develop --features test-utils
uv run pytest -q
```

Type checking (ty):

```bash
uv run ty check python tests
```

## Benchmark: SQL conversion and streaming SQL

`Session.sql(...)` returns results as a `pyarrow.Table`.

By default, results are exported via the Arrow C Data Interface (C Stream) when supported, and
fall back to an in-memory Arrow IPC stream otherwise. To compare the two paths and estimate the
conversion overhead, run:

```bash
cd crates/timeseries-table-python
uv pip install -p .venv/bin/python numpy
uv run -p .venv/bin/python maturin develop --features test-utils
.venv/bin/python bench/sql_conversion.py --target-ipc-gb 2
```

To also benchmark the streaming SQL API (`Session.sql_reader(...)`), include the streaming mode:

```bash
cd crates/timeseries-table-python
uv pip install -p .venv/bin/python numpy
uv run -p .venv/bin/python maturin develop --features test-utils
.venv/bin/python bench/sql_conversion.py --target-ipc-gb 2 --include-streaming --summary
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

With `--include-streaming`, it also reports:
- time to first batch for `Session.sql_reader(...)`
- total incremental iteration time and batch counts
- process-as-you-go comparison:
  `Session.sql_reader(...)` batch iteration vs `Session.sql(...)` materialize-then-process
- `Session.sql_reader(...).read_all()` vs `Session.sql(...)` parity
- best-effort process peak RSS (`ru_maxrss`) after each run
- isolated child-process peak RSS for the process-as-you-go comparison
  (sampled from Linux `/proc/<pid>/status`)

### Sample streaming benchmark results

One local Linux run over a generated dataset of about 10.5M rows:

| Query | Metric | `Session.sql_reader(...)` | `Session.sql(...)` materialize-then-process | Improvement |
|---|---|---:|---:|---:|
| `select * from prices` | First batch available | `370.7ms` | `2.312s` | `84.0%` earlier |
| `select * from prices` | Peak RSS | `2.30 GiB` | `3.60 GiB` | `36.1%` lower |
| `select * from prices order by ts` | First batch available | `2.489s` | `13.182s` | `81.1%` earlier |
| `select * from prices order by ts` | Peak RSS | `3.66 GiB` | `4.84 GiB` | `24.4%` lower |

`sql_reader(...).read_all()` stayed in the same general performance range as `Session.sql(...)`.

Large targets can require high peak RAM (IPC bytes + decoded Table + intermediate buffers). Start with
`--target-ipc-gb 2` and scale up to `3` or `6` on a machine with plenty of memory.

If you hit `Disk quota exceeded`, pass `--tmpdir /path/with/more/space` (the bench uses a temporary
directory and cleans it up on exit).

## Troubleshooting

- `pip` is building from source / fails with Rust errors: no wheel is available for your platform/Python; install Rust and retry, or use a supported Python/platform combination.
- `DataFusionError` about an unknown table name: call `sess.register_tstable("name", "/path/to/table")` first; use `sess.tables()` to list registrations.
- Append fails with an ordered-index error: the column must exactly match the configured Arrow `timestamp(...)`, `int64`, or `uint64` type; Timestamp units must also remain consistent across segments.
- `SchemaMismatchError` on append: the new Parquet segment schema must match the table's adopted schema (column names and types).
- SQL errors / parameter placeholders: try an explicit `CAST(...)` for placeholders used in `SELECT` projections.
