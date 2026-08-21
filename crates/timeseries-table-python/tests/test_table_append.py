from pathlib import Path

import pyarrow as pa
import pytest

import timeseries_table_format as ttf
import timeseries_table_format._native as native


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


def _append_artifacts(root: str) -> list[Path]:
    root_path = Path(root)
    return sorted(
        path
        for directory in (root_path / "data", root_path / "_coverage")
        for path in directory.rglob("*")
        if path.is_file()
    )


def _testing_module():
    testing = getattr(native, "_testing", None)
    if testing is None:
        pytest.skip("Rust extension built without feature 'test-utils'")
    return testing


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


@pytest.mark.parametrize("source", ["input.parquet", {}, [], object()])
def test_append_rejects_unsupported_sources_before_mutation(tmp_path, source):
    table, root = _create_table(tmp_path)

    with pytest.raises(TypeError, match="RecordBatch.*Table.*RecordBatchReader"):
        table.append(source)

    assert table.version() == 1
    assert _append_artifacts(root) == []


def test_append_preserves_protocol_failure_as_cause(tmp_path):
    class BrokenSource:
        def __arrow_c_stream__(self):
            raise RuntimeError("producer failed")

    table, root = _create_table(tmp_path)

    with pytest.raises(ValueError, match="__arrow_c_stream__.*failed") as excinfo:
        table.append(BrokenSource())

    assert isinstance(excinfo.value.__cause__, RuntimeError)
    assert "producer failed" in str(excinfo.value.__cause__)
    assert table.version() == 1
    assert _append_artifacts(root) == []


def test_append_rejects_non_capsule_protocol_result_before_mutation(tmp_path):
    class BrokenSource:
        def __arrow_c_stream__(self):
            return object()

    table, root = _create_table(tmp_path)

    with pytest.raises(ValueError, match="must return an Arrow C Stream capsule"):
        table.append(BrokenSource())

    assert table.version() == 1
    assert _append_artifacts(root) == []


def test_append_rejects_consumed_stream_capsule_before_mutation(tmp_path):
    batch = _batch()
    reader = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    capsule = reader.__arrow_c_stream__()

    class ConsumedSource:
        def __arrow_c_stream__(self, requested_schema=None):
            return capsule

    source = ConsumedSource()
    consumer = pa.RecordBatchReader.from_stream(source)
    consumer.close()
    table, root = _create_table(tmp_path)

    with pytest.raises(ValueError, match="already released"):
        table.append(source)

    assert table.version() == 1
    assert _append_artifacts(root) == []


def test_append_empty_reader_does_not_commit_or_create_data(tmp_path):
    table, root = _create_table(tmp_path)
    batch = _batch()
    source = pa.RecordBatchReader.from_batches(batch.schema, [])

    with pytest.raises(
        ttf.TimeseriesTableError, match="empty Arrow batch source"
    ) as excinfo:
        table.append(source)

    assert getattr(excinfo.value, "table_root", None) == root
    assert table.version() == 1
    assert _append_artifacts(root) == []


def test_append_midstream_error_rolls_back_data_and_version(tmp_path):
    testing = _testing_module()
    root = tmp_path / "table"
    table = ttf.TimeSeriesTable.create(
        table_root=str(root),
        index_column="x",
        index_type="int64",
        bucket_width=1,
    )
    source = testing._test_sql_reader_midstream_error()

    with pytest.raises(ttf.TimeseriesTableError, match="mid-stream boom") as excinfo:
        table.append(source)

    assert getattr(excinfo.value, "table_root", None) == str(root)
    assert table.version() == 1
    assert _append_artifacts(str(root)) == []


def test_append_schema_error_preserves_exception_type_and_table_root(tmp_path):
    table, root = _create_table(tmp_path)
    assert table.append(_batch()) == 2
    append_artifacts = _append_artifacts(root)
    mismatched = pa.record_batch(
        {
            "ts": pa.array([20], type=pa.int64()),
            "symbol": pa.array(["A"]),
            "value": pa.array([3], type=pa.int64()),
        }
    )

    with pytest.raises(ttf.SchemaMismatchError) as excinfo:
        table.append(mismatched)

    assert getattr(excinfo.value, "table_root", None) == root
    assert table.version() == 2
    assert _append_artifacts(root) == append_artifacts


def test_append_overlap_preserves_exception_type_and_table_root(tmp_path):
    table, root = _create_table(tmp_path)
    assert table.append(_batch()) == 2
    append_artifacts = _append_artifacts(root)

    with pytest.raises(ttf.CoverageOverlapError) as excinfo:
        table.append(_batch())

    assert getattr(excinfo.value, "table_root", None) == root
    assert table.version() == 2
    assert _append_artifacts(root) == append_artifacts
