import pytest
from lib2to3.fixes.fix_print import parend_expr
from fileinput import close
import pyarrow as pa
import pyarrow.parquet as pq

import timeseries_table_format as ttf

def _write_parquet(path: str, ts_us: list[int], symbol: list[str], close: list[float]) -> None:
    tbl = pa.Table(
        {
            "ts": pa.array(ts_us, type=pa.timestamp("us")),
            "symbol": pa.array(symbol, type=pa.string()),
            "close": pa.array(close, type=pa.float64()),
        }
    )
    pq.write_table(tbl, path)

def test_append_parquet_outside_and_inside_root(tmp_path):
    root = tmp_path / "table"

    tbl = ttf.TimeSeriesTable.create(
        table_root=str(root),
        time_column="ts",
        bucket="1h",
        entity_columns=["symbol"],
        timezone=None,
    )

    outside = tmp_path / "outside.parquet"
    _write_parquet(
        str(outside),
        ts_us=[0, 3_600 * 1_000_000],
        symbol=["NVDA", "NVDA"],
        close=[1.0, 2.0],
    )

    v1 = tbl.append_parquet(str(outside))
    assert isinstance(v1, int)
    assert v1 >= 2

    (root / "data").mkdir(parents=True, exist_ok=True)
    inside = root / "data" / "inside.parquet"
    _write_parquet(
        str(inside),
        ts_us=[7_200 * 1_000_000, 10_800 * 1_000_000],
        symbol=["NVDA", "NVDA"],
        close=[3.0, 4.0],
    )

    v2 = tbl.append_parquet(str(inside), copy_if_outside=False)
    assert v2 > v1

    with pytest.raises(ValueError):
        tbl.append_parquet(str(outside), copy_if_outside=False)

def test_append_parquet_overlap_raises_coverage_overlap(tmp_path):
    root = tmp_path / "table"

    tbl = ttf.TimeSeriesTable.create(
        table_root=str(root),
        time_column="ts",
        bucket="1h",
        entity_columns=["symbol"],
        timezone=None,
    )

    (root / "data").mkdir(parents=True, exist_ok=True)
    seg_a = root / "data" / "a.parquet"
    seg_b = root / "data" / "b.parquet"

    _write_parquet(
        str(seg_a),
        ts_us=[0, 3_600 * 1_000_000],
        symbol=["NVDA", "NVDA"],
        close=[1.0, 2.0],
    )
    _write_parquet(
        str(seg_b),
        ts_us=[1_800 * 1_000_000, 5_400 * 1_000_000],  # overlaps seg_a
        symbol=["NVDA", "NVDA"],
        close=[10.0, 20.0],
    )

    tbl.append_parquet(str(seg_a), copy_if_outside=False)

    with pytest.raises(ttf.CoverageOverlapError) as excinfo:
        tbl.append_parquet(str(seg_b), copy_if_outside=False)

    e = excinfo.value
    assert isinstance(e.segment_path, str)
    assert isinstance(e.overlap_count, int)
    assert e.overlap_count > 0