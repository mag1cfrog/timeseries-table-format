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

def test_register_tstable_succeeds_and_replaces(tmp_path):
    root = tmp_path / "table"

    tbl = ttf.TimeSeriesTable.create(table_root=str(root), time_column="ts", bucket="1h", entity_columns=["symbol"], timezone=None,)

    seg = tmp_path / "seg.parquet"
    _write_parquet(str(seg))
    tbl.append_parquet(str(seg))

    sess = ttf.Session()
    sess.register_tstable("prices", str(root))
    sess.register_tstable("prices", str(root)) # replace existing registration

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