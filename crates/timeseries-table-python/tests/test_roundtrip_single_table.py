import datetime

import pytest
import pyarrow as pa
import pyarrow.parquet as pq

import timeseries_table_format as ttf


def _write_prices_parquet(path: str, *, ts_type: pa.DataType) -> None:
    if pa.types.is_timestamp(ts_type):
        unit = ts_type.unit
        scale = {"s": 1, "ms": 1_000, "us": 1_000_000, "ns": 1_000_000_000}[unit]
        ts_values: list[object] = [0, 3_600 * scale, 7_200 * scale]
    elif pa.types.is_int64(ts_type):
        # Common real-world mistake: epoch values stored as plain integers.
        ts_values = [0, 3_600 * 1_000_000, 7_200 * 1_000_000]
    elif pa.types.is_string(ts_type):
        # Common real-world mistake: timestamps stored as strings.
        ts_values = ["0", "3600", "7200"]
    else:
        raise AssertionError(f"unexpected ts_type for test: {ts_type}")

    tbl = pa.table(
        {
            "ts": pa.array(
                ts_values,
                type=ts_type,
            ),
            "symbol": pa.array(["NVDA", "NVDA", "NVDA"], type=pa.string()),
            "close": pa.array([10, 20, 30], type=pa.int64()),
        }
    )
    pq.write_table(tbl, path)


def test_create_append_register_sql_roundtrip(tmp_path):
    table_root = tmp_path / "prices_tbl"
    tstable = ttf.TimeSeriesTable.create(
        table_root=str(table_root),
        time_column="ts",
        bucket="1h",
        entity_columns=["symbol"],
        timezone=None,
    )

    seg_path = tmp_path / "seg.parquet"
    _write_prices_parquet(str(seg_path), ts_type=pa.timestamp("us"))
    tstable.append_parquet(str(seg_path))

    sess = ttf.Session()
    sess.register_tstable("prices", str(table_root))

    out = sess.sql("select count(*) as n, sum(close) as s from prices")
    assert isinstance(out, pa.Table)
    assert out.column_names == ["n", "s"]
    assert out.schema.field("n").type == pa.int64()
    assert out.schema.field("s").type == pa.int64()
    assert out["n"].to_pylist() == [3]
    assert out["s"].to_pylist() == [60]

    out2 = sess.sql("select ts, symbol, close from prices order by ts")
    assert out2.column_names == ["ts", "symbol", "close"]
    assert out2.schema.field("ts").type == pa.timestamp("us")
    assert out2.schema.field("symbol").type == pa.string()
    assert out2.schema.field("close").type == pa.int64()
    assert out2["symbol"].to_pylist() == ["NVDA", "NVDA", "NVDA"]
    assert out2["close"].to_pylist() == [10, 20, 30]
    assert out2["ts"].to_pylist() == [
        datetime.datetime(1970, 1, 1, 0, 0, 0),
        datetime.datetime(1970, 1, 1, 1, 0, 0),
        datetime.datetime(1970, 1, 1, 2, 0, 0),
    ]

    out3 = sess.sql(
        "select ts, symbol, close from prices where close > 1000000 order by ts"
    )
    assert out3.num_rows == 0
    assert out3.column_names == ["ts", "symbol", "close"]
    assert out3.schema == out2.schema


def test_register_tstable_before_first_append_fails_then_succeeds(tmp_path):
    table_root = tmp_path / "prices_tbl"
    tstable = ttf.TimeSeriesTable.create(
        table_root=str(table_root),
        time_column="ts",
        bucket="1h",
        entity_columns=["symbol"],
        timezone=None,
    )

    sess = ttf.Session()
    with pytest.raises(ttf.DataFusionError):
        sess.register_tstable("prices", str(table_root))

    seg_path = tmp_path / "seg.parquet"
    _write_prices_parquet(str(seg_path), ts_type=pa.timestamp("us"))
    tstable.append_parquet(str(seg_path))

    sess.register_tstable("prices", str(table_root))
    out = sess.sql("select count(*) as n from prices")
    assert out["n"].to_pylist() == [3]


@pytest.mark.parametrize("bad_ts_type", [pa.int64(), pa.string()])
def test_append_rejects_time_column_wrong_type(tmp_path, bad_ts_type: pa.DataType):
    table_root = tmp_path / "prices_tbl"
    tstable = ttf.TimeSeriesTable.create(
        table_root=str(table_root),
        time_column="ts",
        bucket="1h",
        entity_columns=["symbol"],
        timezone=None,
    )

    seg_path = tmp_path / "seg.parquet"
    _write_prices_parquet(str(seg_path), ts_type=bad_ts_type)
    with pytest.raises(ttf.TimeseriesTableError) as excinfo:
        tstable.append_parquet(str(seg_path))

    # Realistic pain point: upstream produced epoch values or strings for `ts`,
    # which would silently break time pruning/coverage unless rejected.
    assert getattr(excinfo.value, "table_root", None) == str(table_root)
    msg = str(excinfo.value).lower()
    assert "time column" in msg or "timestamp" in msg


@pytest.mark.parametrize("mismatch_unit", ["ms", "ns"])
def test_append_rejects_time_column_unit_mismatch_after_schema_adoption(
    tmp_path, mismatch_unit: str
):
    table_root = tmp_path / "prices_tbl"
    tstable = ttf.TimeSeriesTable.create(
        table_root=str(table_root),
        time_column="ts",
        bucket="1h",
        entity_columns=["symbol"],
        timezone=None,
    )

    # First append adopts the canonical schema with microsecond timestamps.
    seg1 = tmp_path / "seg1.parquet"
    _write_prices_parquet(str(seg1), ts_type=pa.timestamp("us"))
    tstable.append_parquet(str(seg1))

    # Second append uses a different timestamp unit, which is a common real-world
    # pitfall when mixing pandas/pyarrow writers (e.g. default ns) with other tools.
    seg2 = tmp_path / "seg2.parquet"
    tbl2 = pa.table(
        {
            "ts": pa.array(
                [10_800 * 1_000, 14_400 * 1_000]  # 3h, 4h
                if mismatch_unit == "ms"
                else [
                    10_800 * 1_000_000_000,
                    14_400 * 1_000_000_000,
                ],
                type=pa.timestamp(mismatch_unit),
            ),
            "symbol": pa.array(["NVDA", "NVDA"], type=pa.string()),
            "close": pa.array([40, 50], type=pa.int64()),
        }
    )
    pq.write_table(tbl2, str(seg2))

    with pytest.raises(ttf.SchemaMismatchError) as excinfo:
        tstable.append_parquet(str(seg2))

    assert getattr(excinfo.value, "table_root", None) == str(table_root)
    msg = str(excinfo.value).lower()
    assert "ts" in msg and "time" in msg
