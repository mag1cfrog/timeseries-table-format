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


def _write_symbols_parquet(path: str) -> None:
    tbl = pa.table(
        {
            "symbol": pa.array(["NVDA", "AAPL"], type=pa.string()),
            "exchange": pa.array(["NASDAQ", "NASDAQ"], type=pa.string()),
        }
    )
    pq.write_table(tbl, path)


def test_session_sql_join_tstable_and_parquet(tmp_path):
    # 1) Create TS table (entity is symbol, so table is for one symbol)
    table_root = tmp_path / "prices_tbl"
    tstable = ttf.TimeSeriesTable.create(
        table_root=str(table_root),
        time_column="ts",
        bucket="1h",
        entity_columns=["symbol"],
        timezone=None,
    )

    # 2) Append one NVDA segment (consistent entity identity)
    prices_seg = tmp_path / "prices.parquet"
    _write_prices_parquet(str(prices_seg))
    tstable.append_parquet(str(prices_seg))

    # 3) Create dim parquet
    symbols_path = tmp_path / "symbols.parquet"
    _write_symbols_parquet(str(symbols_path))

    # 4) Register + join
    sess = ttf.Session()
    sess.register_tstable("prices", str(table_root))
    sess.register_parquet("symbols", str(symbols_path))

    out = sess.sql(
        """
        select count(*) as n
        from prices p
        join symbols s
        on p.symbol = s.symbol
        """
    )
    assert out.column_names == ["n"]
    assert out["n"].to_pylist() == [2]

    out2 = sess.sql(
        """
        select p.ts as ts, p.symbol as symbol, p.close as close, s.exchange
as exchange
        from prices p
        join symbols s
        on p.symbol = s.symbol
        order by p.ts
        """
    )
    assert out2.column_names == ["ts", "symbol", "close", "exchange"]
    assert out2["symbol"].to_pylist() == ["NVDA", "NVDA"]
    assert out2["exchange"].to_pylist() == ["NASDAQ", "NASDAQ"]
    assert out2["close"].to_pylist() == [1.0, 2.0]
