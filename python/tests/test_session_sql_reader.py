import datetime
import decimal
import threading
import time

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import timeseries_table_format as ttf
import timeseries_table_format._native as native


def _testing_module():
    testing = getattr(native, "_testing", None)
    if testing is None:
        pytest.skip("Rust extension built without feature 'test-utils'")
    return testing


def _write_dim_parquet(path: str) -> None:
    tbl = pa.table(
        {
            "symbol": pa.array(["NVDA", "AAPL"], type=pa.string()),
            "exchange": pa.array(["NASDAQ", "NASDAQ"], type=pa.string()),
        }
    )
    pq.write_table(tbl, path)


def _write_prices_parquet(path: str) -> None:
    tbl = pa.table(
        {
            "ts": pa.array([0, 3_600 * 1_000_000], type=pa.timestamp("us")),
            "symbol": pa.array(["NVDA", "NVDA"], type=pa.string()),
            "close": pa.array([10, 20], type=pa.int64()),
        }
    )
    pq.write_table(tbl, path)


def _write_supported_types_parquet(path: str) -> None:
    tbl = pa.table(
        {
            "id": pa.array([1, 2], type=pa.int64()),
            "dec": pa.array(
                [decimal.Decimal("1.23"), decimal.Decimal("4.56")],
                type=pa.decimal128(10, 2),
            ),
            "payload": pa.array([b"a", b"bc"], type=pa.binary()),
            "ts": pa.array(
                [
                    datetime.datetime(2024, 1, 1, 0, 0, 0),
                    datetime.datetime(2024, 1, 1, 1, 0, 0),
                ],
                type=pa.timestamp("us"),
            ),
            "d": pa.array(
                [datetime.date(2024, 1, 1), datetime.date(2024, 1, 2)],
                type=pa.date32(),
            ),
            "tags": pa.array([[1, 2], [3]], type=pa.list_(pa.int64())),
            "meta": pa.array(
                [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}],
                type=pa.struct([("a", pa.int64()), ("b", pa.string())]),
            ),
        }
    )
    pq.write_table(tbl, path)


def test_session_sql_reader_returns_recordbatchreader():
    sess = ttf.Session()

    reader = sess.sql_reader("select 1 as x union all select 2 as x order by x")
    try:
        assert isinstance(reader, pa.RecordBatchReader)
        table = pa.Table.from_batches(list(reader))
        assert table["x"].to_pylist() == [1, 2]
    finally:
        reader.close()


def test_session_sql_reader_read_all_matches_sql():
    sess = ttf.Session()
    query = "select cast(1 as bigint) as x union all select cast(2 as bigint) as x order by x"

    reader = sess.sql_reader(query)
    try:
        from_reader = reader.read_all()
    finally:
        reader.close()

    from_sql = sess.sql(query)
    assert from_reader.equals(from_sql)


def test_session_sql_reader_params_work():
    sess = ttf.Session()

    reader = sess.sql_reader("select cast($1 as bigint) as x", params=[1])
    try:
        table = reader.read_all()
    finally:
        reader.close()

    assert table["x"].to_pylist() == [1]


def test_session_sql_reader_can_close_early():
    sess = ttf.Session()

    reader = sess.sql_reader("select 1 as x union all select 2 as x")
    reader.close()


def test_session_sql_reader_named_params_work():
    sess = ttf.Session()

    reader = sess.sql_reader("select cast($value as bigint) as x", params={"value": 7})
    try:
        table = reader.read_all()
    finally:
        reader.close()

    assert table["x"].to_pylist() == [7]


def test_session_sql_reader_empty_result():
    sess = ttf.Session()

    reader = sess.sql_reader("select cast(1 as bigint) as x where false")
    try:
        table = reader.read_all()
    finally:
        reader.close()

    assert table.num_rows == 0
    assert table.column_names == ["x"]


def test_session_sql_reader_read_all_after_partial_iteration_reads_remainder():
    sess = ttf.Session()

    reader = sess.sql_reader("select * from range(10000)")
    try:
        first_batch = next(reader)
        remaining = reader.read_all()
    finally:
        reader.close()

    assert first_batch.num_rows > 0
    assert remaining.num_rows > 0
    assert first_batch.num_rows + remaining.num_rows == 10000
    assert first_batch.column_names == ["value"]
    assert remaining.column_names == ["value"]


def test_session_sql_reader_close_is_idempotent():
    sess = ttf.Session()

    reader = sess.sql_reader("select 1 as x")
    reader.close()
    reader.close()


def test_session_sql_reader_rejects_invalid_params_shape():
    sess = ttf.Session()

    with pytest.raises(TypeError, match="params"):
        sess.sql_reader("select 1 as x", params={1, 2, 3})


def test_session_sql_reader_rejects_unsupported_schema():
    testing = _testing_module()

    with pytest.raises(RuntimeError, match=r"schema cannot be exported via Arrow C Stream"):
        testing._test_sql_reader_unsupported_schema()


def test_session_sql_reader_midstream_error_surfaces_during_iteration():
    testing = _testing_module()

    reader = testing._test_sql_reader_midstream_error()
    try:
        first_batch = next(reader)
        assert first_batch["x"].to_pylist() == [1, 2]

        with pytest.raises(Exception, match="mid-stream boom"):
            next(reader)
    finally:
        reader.close()


def test_session_sql_reader_concurrent_readers_do_not_crash():
    sess = ttf.Session()
    errors: list[Exception] = []

    def worker() -> None:
        try:
            for _ in range(10):
                reader = sess.sql_reader("select * from range(10000)")
                try:
                    total_rows = sum(batch.num_rows for batch in reader)
                finally:
                    reader.close()
                assert total_rows == 10000
        except Exception as exc:  # pragma: no cover
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=10.0)

    assert all(not thread.is_alive() for thread in threads), "thread hung"
    assert not errors, errors[0]


@pytest.mark.skip(
    reason="sharing one pyarrow.RecordBatchReader across Python threads for concurrent next()/close() is currently unsafe"
)
def test_session_sql_reader_close_unblocks_active_iteration():
    testing = _testing_module()

    reader = testing._test_sql_reader_pending_after_first_batch()
    first_batch = next(reader)
    assert first_batch["x"].to_pylist() == [1, 2]

    started = threading.Event()
    finished = threading.Event()
    outcome: dict[str, object] = {}

    def worker() -> None:
        started.set()
        try:
            outcome["value"] = next(reader)
        except BaseException as exc:  # pragma: no cover
            outcome["value"] = exc
        finally:
            finished.set()

    thread = threading.Thread(target=worker)
    thread.start()
    assert started.wait(timeout=1.0)
    time.sleep(0.05)
    reader.close()

    thread.join(timeout=2.0)
    assert not thread.is_alive(), "close() did not unblock active iteration"
    assert finished.is_set()
    assert isinstance(outcome["value"], StopIteration)


def test_session_sql_reader_large_multi_batch_result():
    sess = ttf.Session()

    reader = sess.sql_reader("select * from range(500000)")
    try:
        batches = list(reader)
    finally:
        reader.close()

    assert len(batches) > 1
    assert sum(batch.num_rows for batch in batches) == 500000


def test_session_sql_reader_supported_parquet_types(tmp_path):
    path = tmp_path / "supported.parquet"
    _write_supported_types_parquet(str(path))

    sess = ttf.Session()
    sess.register_parquet("supported", str(path))

    reader = sess.sql_reader(
        "select id, dec, payload, ts, d, tags, meta from supported order by id"
    )
    try:
        table = reader.read_all()
    finally:
        reader.close()

    assert table.schema.field("dec").type == pa.decimal128(10, 2)
    assert pa.types.is_binary(table.schema.field("payload").type) or pa.types.is_binary_view(
        table.schema.field("payload").type
    )
    assert table.schema.field("ts").type == pa.timestamp("us")
    assert table.schema.field("d").type == pa.date32()
    assert table.schema.field("tags").type == pa.list_(pa.int64())
    assert table.schema.field("meta").type == pa.struct([("a", pa.int64()), ("b", pa.string())])
    assert table["dec"].to_pylist() == [decimal.Decimal("1.23"), decimal.Decimal("4.56")]
    assert table["tags"].to_pylist() == [[1, 2], [3]]
    assert table["meta"].to_pylist() == [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}]


def test_session_sql_reader_registered_parquet_query(tmp_path):
    path = tmp_path / "dim.parquet"
    _write_dim_parquet(str(path))

    sess = ttf.Session()
    sess.register_parquet("dim", str(path))

    reader = sess.sql_reader("select symbol, exchange from dim order by symbol")
    try:
        table = reader.read_all()
    finally:
        reader.close()

    assert table["symbol"].to_pylist() == ["AAPL", "NVDA"]
    assert table["exchange"].to_pylist() == ["NASDAQ", "NASDAQ"]


def test_session_sql_reader_registered_tstable_and_join(tmp_path):
    table_root = tmp_path / "prices_tbl"
    prices = ttf.TimeSeriesTable.create(
        table_root=str(table_root),
        time_column="ts",
        bucket="1h",
        entity_columns=["symbol"],
        timezone=None,
    )

    prices_path = tmp_path / "prices.parquet"
    dim_path = tmp_path / "dim.parquet"
    _write_prices_parquet(str(prices_path))
    _write_dim_parquet(str(dim_path))
    prices.append_parquet(str(prices_path))

    sess = ttf.Session()
    sess.register_tstable("prices", str(table_root))
    sess.register_parquet("dim", str(dim_path))

    reader = sess.sql_reader(
        """
        select p.ts, p.symbol, p.close, d.exchange
        from prices p
        join dim d
        on p.symbol = d.symbol
        order by p.ts
        """
    )
    try:
        table = reader.read_all()
    finally:
        reader.close()

    assert table["symbol"].to_pylist() == ["NVDA", "NVDA"]
    assert table["close"].to_pylist() == [10, 20]
    assert table["exchange"].to_pylist() == ["NASDAQ", "NASDAQ"]


def test_session_sql_reader_streams_first_batch_before_full_consumption():
    testing = _testing_module()

    reader = testing._test_sql_reader_delayed_batches(
        batch_count=4,
        rows_per_batch=3,
        delay_millis=100,
    )
    start = time.perf_counter()
    try:
        first_batch = next(reader)
        first_elapsed = time.perf_counter() - start

        remaining_batches = list(reader)
        total_elapsed = time.perf_counter() - start
    finally:
        reader.close()

    assert first_batch.num_rows == 3
    assert len(remaining_batches) == 3
    assert first_elapsed < total_elapsed
    assert total_elapsed - first_elapsed >= 0.15
    assert total_elapsed >= 0.25


def test_session_sql_reader_requires_pyarrow_from_stream(monkeypatch: pytest.MonkeyPatch):
    pa_mod = pytest.importorskip("pyarrow")

    class _FakeRecordBatchReader:
        pass

    monkeypatch.setattr(pa_mod, "RecordBatchReader", _FakeRecordBatchReader)

    sess = ttf.Session()
    with pytest.raises(ImportError, match=r"RecordBatchReader\.from_stream"):
        sess.sql_reader("select 1 as x")
