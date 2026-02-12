import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import timeseries_table_format as ttf
import threading


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


def test_session_tables_empty_on_new_session():
    sess = ttf.Session()
    assert sess.tables() == []


def test_session_register_parquet_failure_does_not_add_name(tmp_path):
    sess = ttf.Session()

    bad = tmp_path / "bad.txt"
    with pytest.raises(ttf.DataFusionError):
        sess.register_parquet("x", str(bad))

    assert sess.tables() == []


def test_session_tables_no_duplicates_on_replace(tmp_path):
    p = tmp_path / "dim.parquet"
    _write_symbols_parquet(str(p))

    sess = ttf.Session()
    sess.register_parquet("dim", str(p))
    sess.register_parquet("dim", str(p))

    assert sess.tables() == ["dim"]


def test_session_deregister_empty_name_rejected():
    sess = ttf.Session()
    with pytest.raises(ValueError):
        sess.deregister("")


def test_session_deregister_and_reregister(tmp_path):
    p = tmp_path / "dim.parquet"
    _write_symbols_parquet(str(p))

    sess = ttf.Session()
    sess.register_parquet("dim", str(p))
    assert sess.tables() == ["dim"]

    sess.deregister("dim")
    assert sess.tables() == []

    sess.register_parquet("dim", str(p))
    assert sess.tables() == ["dim"]


def test_session_deregister_concurrent_one_wins(tmp_path):
    p = tmp_path / "dim.parquet"
    _write_symbols_parquet(str(p))

    sess = ttf.Session()
    sess.register_parquet("dim", str(p))

    errors: list[BaseException] = []
    ok = 0
    lock = threading.Lock()

    def worker():
        nonlocal ok
        try:
            sess.deregister("dim")
            with lock:
                ok += 1
        except BaseException as e:
            with lock:
                errors.append(e)

    a = threading.Thread(target=worker)
    b = threading.Thread(target=worker)
    a.start()
    b.start()
    a.join(timeout=5.0)
    b.join(timeout=5.0)

    assert not a.is_alive() and not b.is_alive()
    assert ok == 1
    assert len(errors) == 1
    assert isinstance(errors[0], KeyError)


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
