import os

import pytest
import pyarrow as pa
import pyarrow.parquet as pq

import timeseries_table_format as ttf


def _write_parquet(
    path: str, ts_us: list[int], symbol: list[str], close: list[float]
) -> None:
    tbl = pa.table(
        {
            "ts": pa.array(ts_us, type=pa.timestamp("us")),
            "symbol": pa.array(symbol, type=pa.string()),
            "close": pa.array(close, type=pa.float64()),
        }
    )
    pq.write_table(tbl, path)


def _write_parquet_with_ts2(
    path: str, ts2_us: list[int], symbol: list[str], close: list[float]
) -> None:
    tbl = pa.table(
        {
            "ts2": pa.array(ts2_us, type=pa.timestamp("us")),
            "symbol": pa.array(symbol, type=pa.string()),
            "close": pa.array(close, type=pa.float64()),
        }
    )
    pq.write_table(tbl, path)


def _write_parquet_close_int(
    path: str, ts_us: list[int], symbol: list[str], close: list[int]
) -> None:
    tbl = pa.table(
        {
            "ts": pa.array(ts_us, type=pa.timestamp("us")),
            "symbol": pa.array(symbol, type=pa.string()),
            "close": pa.array(close, type=pa.int64()),
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
    assert (root / "data" / "outside.parquet").exists()

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


def test_append_parquet_copy_if_outside_true_when_already_under_root_does_not_copy(
    tmp_path,
):
    root = tmp_path / "table"

    tbl = ttf.TimeSeriesTable.create(
        table_root=str(root),
        time_column="ts",
        bucket="1h",
        entity_columns=["symbol"],
        timezone=None,
    )

    (root / "custom").mkdir(parents=True, exist_ok=True)
    seg = root / "custom" / "in_root.parquet"
    _write_parquet(
        str(seg),
        ts_us=[0, 3_600 * 1_000_000],
        symbol=["NVDA", "NVDA"],
        close=[1.0, 2.0],
    )

    v = tbl.append_parquet(str(seg), copy_if_outside=True)
    assert isinstance(v, int)
    assert (root / "custom" / "in_root.parquet").exists()
    assert not (root / "data" / "in_root.parquet").exists()


def test_append_parquet_copy_collision_raises_storage_error(tmp_path):
    root = tmp_path / "table"

    tbl = ttf.TimeSeriesTable.create(
        table_root=str(root),
        time_column="ts",
        bucket="1h",
        entity_columns=["symbol"],
        timezone=None,
    )

    (root / "data").mkdir(parents=True, exist_ok=True)
    existing = root / "data" / "outside.parquet"
    _write_parquet(
        str(existing),
        ts_us=[0, 3_600 * 1_000_000],
        symbol=["NVDA", "NVDA"],
        close=[1.0, 2.0],
    )

    outside = tmp_path / "outside.parquet"
    _write_parquet(
        str(outside),
        ts_us=[7_200 * 1_000_000, 10_800 * 1_000_000],
        symbol=["NVDA", "NVDA"],
        close=[3.0, 4.0],
    )

    with pytest.raises(ttf.StorageError) as excinfo:
        tbl.append_parquet(str(outside), copy_if_outside=True)

    e = excinfo.value
    assert getattr(e, "table_root", None) == str(root)
    path = getattr(e, "path", "")
    assert isinstance(path, str)
    assert path.replace("\\", "/").endswith("/data/outside.parquet")


def test_append_parquet_copy_if_outside_false_relative_path_and_traversal(tmp_path):
    root = tmp_path / "table"

    tbl = ttf.TimeSeriesTable.create(
        table_root=str(root),
        time_column="ts",
        bucket="1h",
        entity_columns=["symbol"],
        timezone=None,
    )

    (root / "data").mkdir(parents=True, exist_ok=True)
    rel = root / "data" / "rel.parquet"
    _write_parquet(
        str(rel),
        ts_us=[0, 3_600 * 1_000_000],
        symbol=["NVDA", "NVDA"],
        close=[1.0, 2.0],
    )

    v = tbl.append_parquet("data/rel.parquet", copy_if_outside=False)
    assert isinstance(v, int)

    with pytest.raises(ValueError) as excinfo:
        tbl.append_parquet("../x.parquet", copy_if_outside=False)
    assert str(root) in str(excinfo.value)


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
    assert getattr(e, "table_root", None) == str(root)


def test_append_parquet_time_column_override(tmp_path):
    root = tmp_path / "table"

    tbl = ttf.TimeSeriesTable.create(
        table_root=str(root),
        time_column="ts",
        bucket="1h",
        entity_columns=["symbol"],
        timezone=None,
    )

    (root / "data").mkdir(parents=True, exist_ok=True)
    seg = root / "data" / "ts2.parquet"
    _write_parquet_with_ts2(
        str(seg),
        ts2_us=[0, 3_600 * 1_000_000],
        symbol=["NVDA", "NVDA"],
        close=[1.0, 2.0],
    )

    with pytest.raises(ttf.TimeseriesTableError):
        tbl.append_parquet(str(seg), copy_if_outside=False)

    v = tbl.append_parquet(str(seg), time_column="ts2", copy_if_outside=False)
    assert isinstance(v, int)


def test_append_parquet_schema_mismatch_raises_schema_mismatch_error(tmp_path):
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
    _write_parquet_close_int(
        str(seg_b),
        ts_us=[7_200 * 1_000_000, 10_800 * 1_000_000],
        symbol=["NVDA", "NVDA"],
        close=[3, 4],
    )

    tbl.append_parquet(str(seg_a), copy_if_outside=False)

    with pytest.raises(ttf.SchemaMismatchError) as excinfo:
        tbl.append_parquet(str(seg_b), copy_if_outside=False)
    assert getattr(excinfo.value, "table_root", None) == str(root)


def test_append_parquet_windows_backslash_paths(tmp_path):
    if os.name != "nt":
        pytest.skip("Windows-only path separator behavior")
        return

    root = tmp_path / "table"

    tbl = ttf.TimeSeriesTable.create(
        table_root=str(root),
        time_column="ts",
        bucket="1h",
        entity_columns=["symbol"],
        timezone=None,
    )

    (root / "data").mkdir(parents=True, exist_ok=True)
    seg = root / "data" / "win.parquet"
    _write_parquet(
        str(seg),
        ts_us=[0, 3_600 * 1_000_000],
        symbol=["NVDA", "NVDA"],
        close=[1.0, 2.0],
    )

    win_path = str(seg).replace("/", "\\")
    v = tbl.append_parquet(win_path, copy_if_outside=False)
    assert isinstance(v, int)
