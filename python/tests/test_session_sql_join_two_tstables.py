import pyarrow as pa
import pyarrow.parquet as pq

import timeseries_table_format as ttf


def _write_prices_parquet(path: str) -> None:
    tbl = pa.table(
        {
            "ts": pa.array([0, 3_600 * 1_000_000], type=pa.timestamp("us")),
            "symbol": pa.array(["NVDA", "NVDA"], type=pa.string()),
            "close": pa.array([1.0, 2.0], type=pa.float64()),
        }
    )
    pq.write_table(tbl, path)


def _write_volumes_parquet(path: str) -> None:
    tbl = pa.table(
        {
            "ts": pa.array([0, 3_600 * 1_000_000], type=pa.timestamp("us")),
            "symbol": pa.array(["NVDA", "NVDA"], type=pa.string()),
            "volume": pa.array([10, 20], type=pa.int64()),
        }
    )
    pq.write_table(tbl, path)


def test_session_sql_join_two_tstables(tmp_path):
    prices_root = tmp_path / "prices_tbl"
    prices = ttf.TimeSeriesTable.create(
        table_root=str(prices_root),
        time_column="ts",
        bucket="1h",
        entity_columns=["symbol"],
        timezone=None,
    )
    prices_seg = tmp_path / "prices.parquet"
    _write_prices_parquet(str(prices_seg))
    prices.append_parquet(str(prices_seg))

    volumes_root = tmp_path / "volumes_tbl"
    volumes = ttf.TimeSeriesTable.create(
        table_root=str(volumes_root),
        time_column="ts",
        bucket="1h",
        entity_columns=["symbol"],
        timezone=None,
    )
    volumes_seg = tmp_path / "volumes.parquet"
    _write_volumes_parquet(str(volumes_seg))
    volumes.append_parquet(str(volumes_seg))

    sess = ttf.Session()
    sess.register_tstable("prices", str(prices_root))
    sess.register_tstable("volumes", str(volumes_root))

    out = sess.sql(
        """
        select count(*) as n
        from prices p
        join volumes v
        on p.ts = v.ts and p.symbol = v.symbol
        """
    )
    assert out.column_names == ["n"]
    assert out["n"].to_pylist() == [2]

    out2 = sess.sql(
        """
        select p.ts as ts, p.symbol as symbol, p.close as close, v.volume as volume
        from prices p
        join volumes v
        on p.ts = v.ts and p.symbol = v.symbol
        order by p.ts
        """
    )
    assert isinstance(out2, pa.Table)
    assert out2.column_names == ["ts", "symbol", "close", "volume"]
    assert out2.schema.field("ts").type == pa.timestamp("us")
    assert out2.schema.field("symbol").type == pa.string()
    assert out2.schema.field("close").type == pa.float64()
    assert out2.schema.field("volume").type == pa.int64()
    assert out2["close"].to_pylist() == [1.0, 2.0]
    assert out2["volume"].to_pylist() == [10, 20]
