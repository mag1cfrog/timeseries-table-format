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

Requires: Python 3.10+.

## Verify installation

```python
import timeseries_table_format as ttf

out = ttf.Session().sql("select 1 as x")
print(type(out))  # pyarrow.Table
```

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

See the tutorial: https://mag1cfrog.github.io/timeseries-table-format/tutorials/register_and_join/

## Parameterized queries

See the tutorial: https://mag1cfrog.github.io/timeseries-table-format/tutorials/parameterized_queries/

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

Alternative: build with `maturin` directly:

```bash
cd python
uv venv -p 3.12 .venv
uv pip install -p .venv/bin/python pyarrow --group dev
uv run -p .venv/bin/python maturin develop -m pyproject.toml
.venv/bin/python -m pytest
```

## Troubleshooting

- Rebuild the Rust extension after changing Rust code:
  - `uv pip install -p python/.venv/bin/python -e python`
