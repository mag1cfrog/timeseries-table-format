import pyarrow as pa

import timeseries_table_format as ttf


def _create_table(tmp_path) -> tuple[ttf.TimeSeriesTable, str]:
    root = tmp_path / "table"
    table = ttf.TimeSeriesTable.create(
        table_root=str(root),
        index_column="ts",
        index_type="int64",
        bucket_width=10,
        entity_columns=["symbol"],
    )
    return table, str(root)


def _batch(start: int = 0) -> pa.RecordBatch:
    return pa.record_batch(
        {
            "ts": pa.array([start, start + 10], type=pa.int64()),
            "symbol": pa.array(["A", "A"]),
            "value": pa.array([1.0, 2.0]),
        }
    )


def _assert_rows(root: str) -> None:
    session = ttf.Session()
    session.register_tstable("series", root)
    result = session.sql("SELECT ts, symbol, value FROM series ORDER BY ts")
    assert result.to_pydict() == {
        "ts": [0, 10],
        "symbol": ["A", "A"],
        "value": [1.0, 2.0],
    }


def test_append_record_batch_returns_version_and_preserves_source(tmp_path):
    table, root = _create_table(tmp_path)
    source = _batch()

    assert table.append(source) == 2
    assert source.column("value").to_pylist() == [1.0, 2.0]
    _assert_rows(root)


def test_append_multichunk_table_preserves_chunks_and_source(tmp_path):
    table, root = _create_table(tmp_path)
    source = pa.Table.from_batches([_batch().slice(0, 1), _batch().slice(1, 1)])
    chunk_counts = [column.num_chunks for column in source.columns]

    assert table.append(source) == 2
    assert [column.num_chunks for column in source.columns] == chunk_counts
    assert source.column("value").to_pylist() == [1.0, 2.0]
    _assert_rows(root)


def test_append_record_batch_reader_consumes_all_batches(tmp_path):
    table, root = _create_table(tmp_path)
    batch = _batch()
    source = pa.RecordBatchReader.from_batches(
        batch.schema, [batch.slice(0, 1), batch.slice(1, 1)]
    )

    assert table.append(source) == 2
    _assert_rows(root)


def test_append_arrow_stream_protocol_object_calls_exporter_once(tmp_path):
    class StreamSource:
        def __init__(self, batch: pa.RecordBatch):
            self.reader = pa.RecordBatchReader.from_batches(batch.schema, [batch])
            self.calls = 0

        def __arrow_c_stream__(self, requested_schema=None):
            self.calls += 1
            return self.reader.__arrow_c_stream__(requested_schema)

    table, root = _create_table(tmp_path)
    source = StreamSource(_batch())

    assert table.append(source) == 2
    assert source.calls == 1
    _assert_rows(root)
