import pyarrow as pa
import pyarrow.parquet as pq

import timeseries_table_format as ttf


def _is_string_like(dt: pa.DataType) -> bool:
    is_string_view = getattr(pa.types, "is_string_view", None)
    if callable(is_string_view) and is_string_view(dt):
        return True
    return pa.types.is_string(dt) or pa.types.is_large_string(dt)


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


def _write_symbols_parquet_missing_nvda(path: str) -> None:
    tbl = pa.table(
        {
            "symbol": pa.array(["AAPL"], type=pa.string()),
            "exchange": pa.array(["NASDAQ"], type=pa.string()),
        }
    )
    pq.write_table(tbl, path)


def _write_symbols_parquet_duplicate_nvda(path: str) -> None:
    # Real-world pitfall: dim tables are often not unique on the key.
    tbl = pa.table(
        {
            "symbol": pa.array(["NVDA", "NVDA"], type=pa.string()),
            "exchange": pa.array(["A", "B"], type=pa.string()),
        }
    )
    pq.write_table(tbl, path)


def _write_symbols_parquet_symbol_int(path: str) -> None:
    # Real-world pitfall: mismatched key types across datasets.
    tbl = pa.table(
        {
            "symbol": pa.array([1, 2], type=pa.int64()),
            "exchange": pa.array(["A", "B"], type=pa.string()),
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
        select p.ts as ts, p.symbol as symbol, p.close as close, s.exchange as exchange
        from prices p
        join symbols s
        on p.symbol = s.symbol
        order by p.ts
        """
    )
    assert isinstance(out2, pa.Table)
    assert out2.column_names == ["ts", "symbol", "close", "exchange"]
    assert out2.schema.field("ts").type == pa.timestamp("us")
    assert out2.schema.field("symbol").type == pa.string()
    assert out2.schema.field("close").type == pa.float64()
    assert _is_string_like(out2.schema.field("exchange").type)
    assert out2["symbol"].to_pylist() == ["NVDA", "NVDA"]
    assert out2["exchange"].to_pylist() == ["NASDAQ", "NASDAQ"]
    assert out2["close"].to_pylist() == [1.0, 2.0]


def test_session_sql_left_join_dim_missing_keys_preserves_fact_rows(tmp_path):
    table_root = tmp_path / "prices_tbl"
    tstable = ttf.TimeSeriesTable.create(
        table_root=str(table_root),
        time_column="ts",
        bucket="1h",
        entity_columns=["symbol"],
        timezone=None,
    )

    prices_seg = tmp_path / "prices.parquet"
    _write_prices_parquet(str(prices_seg))
    tstable.append_parquet(str(prices_seg))

    symbols_path = tmp_path / "symbols.parquet"
    _write_symbols_parquet_missing_nvda(str(symbols_path))

    sess = ttf.Session()
    sess.register_tstable("prices", str(table_root))
    sess.register_parquet("symbols", str(symbols_path))

    out = sess.sql(
        """
        select p.ts as ts, p.symbol as symbol, s.exchange as exchange
        from prices p
        left join symbols s
        on p.symbol = s.symbol
        order by p.ts
        """
    )
    assert out.column_names == ["ts", "symbol", "exchange"]
    assert out.num_rows == 2
    assert out["symbol"].to_pylist() == ["NVDA", "NVDA"]
    assert out["exchange"].to_pylist() == [None, None]


def test_session_sql_join_duplicate_dim_keys_multiplies_rows(tmp_path):
    table_root = tmp_path / "prices_tbl"
    tstable = ttf.TimeSeriesTable.create(
        table_root=str(table_root),
        time_column="ts",
        bucket="1h",
        entity_columns=["symbol"],
        timezone=None,
    )

    prices_seg = tmp_path / "prices.parquet"
    _write_prices_parquet(str(prices_seg))
    tstable.append_parquet(str(prices_seg))

    symbols_path = tmp_path / "symbols.parquet"
    _write_symbols_parquet_duplicate_nvda(str(symbols_path))

    sess = ttf.Session()
    sess.register_tstable("prices", str(table_root))
    sess.register_parquet("symbols", str(symbols_path))

    out = sess.sql(
        """
        select p.ts as ts, p.symbol as symbol, s.exchange as exchange
        from prices p
        join symbols s
        on p.symbol = s.symbol
        order by p.ts, s.exchange
        """
    )
    assert out.column_names == ["ts", "symbol", "exchange"]
    assert out.num_rows == 4  # 2 fact rows * 2 dim rows
    assert out["symbol"].to_pylist() == ["NVDA", "NVDA", "NVDA", "NVDA"]
    assert out["exchange"].to_pylist() == ["A", "B", "A", "B"]


def test_session_sql_join_key_type_mismatch_is_visible_and_yields_no_matches(
    tmp_path,
):
    table_root = tmp_path / "prices_tbl"
    tstable = ttf.TimeSeriesTable.create(
        table_root=str(table_root),
        time_column="ts",
        bucket="1h",
        entity_columns=["symbol"],
        timezone=None,
    )

    prices_seg = tmp_path / "prices.parquet"
    _write_prices_parquet(str(prices_seg))
    tstable.append_parquet(str(prices_seg))

    symbols_path = tmp_path / "symbols.parquet"
    _write_symbols_parquet_symbol_int(str(symbols_path))

    sess = ttf.Session()
    sess.register_tstable("prices", str(table_root))
    sess.register_parquet("symbols", str(symbols_path))

    # Pain point: key type mismatches can produce "silent" empty joins.
    # Make the mismatch visible via schema-only queries.
    prices_schema = sess.sql("select symbol from prices limit 0").schema
    symbols_schema = sess.sql("select symbol from symbols limit 0").schema
    assert prices_schema.field("symbol").type == pa.string()
    assert symbols_schema.field("symbol").type == pa.int64()

    out = sess.sql(
        """
        select count(*) as n
        from prices p
        join symbols s
        on p.symbol = s.symbol
        """
    )
    assert out["n"].to_pylist() == [0]
