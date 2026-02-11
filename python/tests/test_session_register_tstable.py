import threading

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import timeseries_table_format as ttf


def _write_parquet(path: str) -> None:
    tbl = pa.table(
        {
            "ts": pa.array([0, 3_600 * 1_000_000], type=pa.timestamp("us")),
            "symbol": pa.array(["NVDA", "NVDA"], type=pa.string()),
            "close": pa.array([1.0, 2.0], type=pa.float64()),
        }
    )
    pq.write_table(tbl, path)


def _make_table(root) -> ttf.TimeSeriesTable:
    return ttf.TimeSeriesTable.create(
        table_root=str(root),
        time_column="ts",
        bucket="1h",
        entity_columns=["symbol"],
        timezone=None,
    )

def _make_table_with_schema(root, seg_path) -> ttf.TimeSeriesTable:
    tbl = ttf.TimeSeriesTable.create(
        table_root=str(root),
        time_column="ts",
        bucket="1h",
        entity_columns=["symbol"],
        timezone=None,
    )
    _write_parquet(str(seg_path))
    tbl.append_parquet(str(seg_path))  # this should cause schema adoption
    return tbl

def test_register_tstable_succeeds_and_replaces(tmp_path):
    root = tmp_path / "table"

    tbl = ttf.TimeSeriesTable.create(
        table_root=str(root),
        time_column="ts",
        bucket="1h",
        entity_columns=["symbol"],
        timezone=None,
    )

    seg = tmp_path / "seg.parquet"
    _write_parquet(str(seg))
    tbl.append_parquet(str(seg))

    sess = ttf.Session()
    sess.register_tstable("prices", str(root))
    sess.register_tstable("prices", str(root))  # replace existing registration


def test_register_tstable_missing_root_raises(tmp_path):
    sess = ttf.Session()
    root = tmp_path / "nope"

    with pytest.raises(ttf.TimeseriesTableError) as excinfo:
        sess.register_tstable("missing", str(root))

    e = excinfo.value
    assert getattr(e, "table_root", None) == str(root)


def test_register_tstable_empty_name_rejected(tmp_path):
    root = tmp_path / "table"
    ttf.TimeSeriesTable.create(
        table_root=str(root),
        time_column="ts",
        bucket="1h",
        entity_columns=["symbol"],
        timezone=None,
    )

    sess = ttf.Session()
    with pytest.raises(ValueError):
        sess.register_tstable("", str(root))


@pytest.mark.parametrize("make_root", ["missing", "empty_dir"])
def test_register_tstable_invalid_root_raises(tmp_path, make_root):
    sess = ttf.Session()
    root = tmp_path / "bad"

    if make_root == "empty_dir":
        root.mkdir()

    with pytest.raises(ttf.TimeseriesTableError) as excinfo:
        sess.register_tstable("x", str(root))

    assert getattr(excinfo.value, "table_root", None) == str(root)


def test_register_tstable_succeeds_and_replaces_with_different_root(tmp_path):
    root1 = tmp_path / "table1"
    root2 = tmp_path / "table2"

    _make_table_with_schema(root1, tmp_path / "seg1.parquet")
    _make_table_with_schema(root2, tmp_path / "seg2.parquet")

    sess = ttf.Session()
    sess.register_tstable("prices", str(root1))
    sess.register_tstable("prices", str(root2))  # replacement should succeed

def test_register_tstable_concurrent_replace_does_not_crash(tmp_path):
    root1 = tmp_path / "t1"
    root2 = tmp_path / "t2"

    _make_table_with_schema(root1, tmp_path / "seg1.parquet")
    _make_table_with_schema(root2, tmp_path / "seg2.parquet")

    sess = ttf.Session()
    errors: list[Exception] = []

    def w(root):
        try:
            sess.register_tstable("prices", str(root))
        except Exception as e:
            errors.append(e)

    a = threading.Thread(target=w, args=(root1,))
    b = threading.Thread(target=w, args=(root2,))
    a.start()
    b.start()
    a.join(timeout=5.0)
    b.join(timeout=5.0)

    assert not a.is_alive() and not b.is_alive()
    assert not errors, errors[0]

def test_register_tstable_never_appended_table_raises_then_succeeds_after_append(tmp_path):
    root = tmp_path / "table"

    # Create table but do NOT append -> no canonical logical schema yet.
    tbl = _make_table(root)

    sess = ttf.Session()
    with pytest.raises(ttf.DataFusionError):
        sess.register_tstable("prices", str(root))

    # After first append, the table should adopt a canonical schema.
    seg = tmp_path / "seg.parquet"
    _write_parquet(str(seg))
    tbl.append_parquet(str(seg))

    # Now registration should succeed.
    sess.register_tstable("prices", str(root))