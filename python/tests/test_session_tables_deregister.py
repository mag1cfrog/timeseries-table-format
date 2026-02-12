import pyarrow as pa
import pyarrow.parquet as pq
import pytest

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


def test_session_tables_sorted_and_deregister(tmp_path):
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
    _write_symbols_parquet(str(symbols_path))

    sess = ttf.Session()

    # Register in "non-sorted" order to verify tables() sorts.
    sess.register_parquet("symbols", str(symbols_path))
    sess.register_tstable("prices", str(table_root))

    assert sess.tables() == ["prices", "symbols"]

    sess.deregister("symbols")
    assert sess.tables() == ["prices"]

    with pytest.raises(KeyError):
        sess.deregister("symbols")

    with pytest.raises(ttf.DataFusionError):
        sess.sql("select * from symbols")
