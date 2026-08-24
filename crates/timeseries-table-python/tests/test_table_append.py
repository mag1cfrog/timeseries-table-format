import gc
import inspect
import threading
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import timeseries_table_format as ttf
import timeseries_table_format._native as native


def _create_table(tmp_path) -> tuple[ttf.TimeSeriesTable, str]:
    root = tmp_path / "table"
    table = ttf.TimeSeriesTable.create(
        table_root=str(root),
        index_column="ts",
        index_type="int64",
        index_granularity=10,
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


def _assert_default_rows(root: str) -> None:
    session = ttf.Session()
    session.register_tstable("series", root)
    result = session.sql("SELECT ts, symbol, value FROM series ORDER BY ts")
    assert result.to_pydict() == {
        "ts": [0, 10],
        "symbol": ["A", "A"],
        "value": [1.0, 2.0],
    }


def _data_and_coverage_files(root: str) -> list[Path]:
    root_path = Path(root)
    return sorted(
        path
        for directory in (root_path / "data", root_path / "_coverage")
        for path in directory.rglob("*")
        if path.is_file()
    )


def _only_data_file(root: str) -> Path:
    files = sorted((Path(root) / "data").glob("*.parquet"))
    assert len(files) == 1
    return files[0]


def _testing_module():
    testing = getattr(native, "_testing", None)
    if testing is None:
        pytest.skip("Rust extension built without feature 'test-utils'")
    return testing


def test_append_runtime_signature_exposes_keyword_only_writer_settings():
    parameters = inspect.signature(ttf.TimeSeriesTable.append).parameters
    assert list(parameters) == [
        "self",
        "source",
        "compression",
        "max_rows_per_row_group",
        "max_bytes_per_row_group",
    ]
    for name in [
        "compression",
        "max_rows_per_row_group",
        "max_bytes_per_row_group",
    ]:
        assert parameters[name].kind is inspect.Parameter.KEYWORD_ONLY
        assert parameters[name].default is None


def test_append_record_batch_returns_version_and_preserves_source(tmp_path):
    table, root = _create_table(tmp_path)
    source = _batch()

    assert table.append(source) == 2
    assert source.column("value").to_pylist() == [1.0, 2.0]
    _assert_default_rows(root)


def test_append_defaults_to_zstd_compression(tmp_path):
    table, root = _create_table(tmp_path)

    assert table.append(_batch()) == 2

    metadata = pq.ParquetFile(_only_data_file(root)).metadata
    assert metadata.num_row_groups == 1
    for column_index in range(metadata.num_columns):
        assert metadata.row_group(0).column(column_index).compression == "ZSTD"


@pytest.mark.parametrize(
    ("compression", "expected"),
    [
        ("uncompressed", "UNCOMPRESSED"),
        ("snappy", "SNAPPY"),
        (" ZsTd ", "ZSTD"),
    ],
)
def test_append_compression_override_reaches_parquet_metadata(
    tmp_path, compression, expected
):
    table, root = _create_table(tmp_path)

    assert table.append(_batch(), compression=compression) == 2

    metadata = pq.ParquetFile(_only_data_file(root)).metadata
    for column_index in range(metadata.num_columns):
        assert metadata.row_group(0).column(column_index).compression == expected


def test_append_row_limit_splits_groups_and_round_trips(tmp_path):
    table, root = _create_table(tmp_path)
    source = pa.record_batch(
        {
            "ts": pa.array([0, 10, 20, 30, 40], type=pa.int64()),
            "symbol": pa.array(["A"] * 5),
            "value": pa.array([1.0, 2.0, 3.0, 4.0, 5.0]),
        }
    )

    assert (
        table.append(
            source,
            compression="uncompressed",
            max_rows_per_row_group=2,
            max_bytes_per_row_group=1024 * 1024,
        )
        == 2
    )

    metadata = pq.ParquetFile(_only_data_file(root)).metadata
    assert [metadata.row_group(i).num_rows for i in range(metadata.num_row_groups)] == [
        2,
        2,
        1,
    ]
    assert all(
        metadata.row_group(row_group).column(column).compression == "UNCOMPRESSED"
        for row_group in range(metadata.num_row_groups)
        for column in range(metadata.num_columns)
    )
    session = ttf.Session()
    session.register_tstable("series", root)
    assert (
        session.sql("SELECT * FROM series ORDER BY ts").to_pydict()
        == pa.Table.from_batches([source]).to_pydict()
    )


def test_append_byte_limit_splits_binary_batches_and_round_trips(tmp_path):
    table, root = _create_table(tmp_path)
    schema = pa.schema(
        [
            pa.field("ts", pa.int64(), nullable=False),
            pa.field("symbol", pa.string(), nullable=False),
            pa.field("payload", pa.binary(), nullable=False),
        ]
    )
    payloads = [bytes([value]) * 256 for value in [1, 2, 3]]
    batches = [
        pa.RecordBatch.from_arrays(
            [pa.array([index * 10]), pa.array(["A"]), pa.array([payload])],
            schema=schema,
        )
        for index, payload in enumerate(payloads)
    ]
    source = pa.RecordBatchReader.from_batches(schema, batches)

    assert (
        table.append(
            source,
            compression="uncompressed",
            max_rows_per_row_group=100,
            max_bytes_per_row_group=1,
        )
        == 2
    )

    metadata = pq.ParquetFile(_only_data_file(root)).metadata
    assert [metadata.row_group(i).num_rows for i in range(metadata.num_row_groups)] == [
        1,
        1,
        1,
    ]
    session = ttf.Session()
    session.register_tstable("series", root)
    assert session.sql("SELECT * FROM series ORDER BY ts").to_pydict() == {
        "ts": [0, 10, 20],
        "symbol": ["A", "A", "A"],
        "payload": payloads,
    }


@pytest.mark.parametrize(
    ("options", "error_type"),
    [
        ({"compression": "brotli"}, ValueError),
        ({"compression": ""}, ValueError),
        ({"max_rows_per_row_group": 0}, ValueError),
        ({"max_bytes_per_row_group": 0}, ValueError),
        ({"max_rows_per_row_group": -1}, OverflowError),
        ({"max_bytes_per_row_group": -1}, OverflowError),
    ],
)
def test_append_rejects_invalid_writer_settings_before_exporting_source(
    tmp_path, options, error_type
):
    class CountingSource:
        def __init__(self):
            self.calls = 0

        def __arrow_c_stream__(self, requested_schema=None):
            self.calls += 1
            raise AssertionError("invalid settings must not export the source")

    table, root = _create_table(tmp_path)
    source = CountingSource()

    with pytest.raises(error_type):
        table.append(source, **options)

    assert source.calls == 0
    assert table.version() == 1
    assert _data_and_coverage_files(root) == []


def test_append_multichunk_table_preserves_chunks_and_source(tmp_path):
    table, root = _create_table(tmp_path)
    source = pa.Table.from_batches([_batch().slice(0, 1), _batch().slice(1, 1)])
    chunk_counts = [column.num_chunks for column in source.columns]

    assert table.append(source) == 2
    assert [column.num_chunks for column in source.columns] == chunk_counts
    assert source.column("value").to_pylist() == [1.0, 2.0]
    _assert_default_rows(root)


def test_append_record_batch_reader_consumes_all_batches(tmp_path):
    table, root = _create_table(tmp_path)
    batch = _batch()
    source = pa.RecordBatchReader.from_batches(
        batch.schema, [batch.slice(0, 1), batch.slice(1, 1)]
    )

    assert table.append(source) == 2
    _assert_default_rows(root)
    with pytest.raises(StopIteration):
        source.read_next_batch()


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
    _assert_default_rows(root)


@pytest.mark.parametrize("source", ["input.parquet", {}, [], object()])
def test_append_rejects_unsupported_sources_before_mutation(tmp_path, source):
    table, root = _create_table(tmp_path)

    with pytest.raises(TypeError, match="RecordBatch.*Table.*RecordBatchReader"):
        table.append(source)

    assert table.version() == 1
    assert _data_and_coverage_files(root) == []


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
    assert _data_and_coverage_files(root) == []


@pytest.mark.parametrize(
    "exception_type", [KeyboardInterrupt, SystemExit, GeneratorExit]
)
def test_append_preserves_protocol_control_flow_exceptions(tmp_path, exception_type):
    class InterruptedSource:
        def __arrow_c_stream__(self):
            raise exception_type("cancel append")

    table, root = _create_table(tmp_path)

    with pytest.raises(exception_type, match="cancel append"):
        table.append(InterruptedSource())

    assert table.version() == 1
    assert _data_and_coverage_files(root) == []


def test_append_rejects_non_capsule_protocol_result_before_mutation(tmp_path):
    class BrokenSource:
        def __arrow_c_stream__(self):
            return object()

    table, root = _create_table(tmp_path)

    with pytest.raises(ValueError, match="must return an Arrow C Stream capsule"):
        table.append(BrokenSource())

    assert table.version() == 1
    assert _data_and_coverage_files(root) == []


def test_append_rejects_wrong_capsule_name_before_mutation(tmp_path):
    class WrongCapsuleSource:
        def __arrow_c_stream__(self):
            return pa.array([1]).__arrow_c_array__()[1]

    table, root = _create_table(tmp_path)

    with pytest.raises(ValueError, match="invalid Arrow C Stream capsule") as excinfo:
        table.append(WrongCapsuleSource())

    assert isinstance(excinfo.value.__cause__, ValueError)
    assert table.version() == 1
    assert _data_and_coverage_files(root) == []


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
    assert _data_and_coverage_files(root) == []


def test_append_empty_reader_does_not_commit_or_create_data(tmp_path):
    table, root = _create_table(tmp_path)
    batch = _batch()
    source = pa.RecordBatchReader.from_batches(batch.schema, [])

    with pytest.raises(
        ttf.TimeseriesTableError, match="Cannot append an empty Arrow input"
    ) as excinfo:
        table.append(source)

    assert getattr(excinfo.value, "table_root", None) == root
    assert table.version() == 1
    assert _data_and_coverage_files(root) == []


def test_append_midstream_error_rolls_back_data_and_version(tmp_path):
    testing = _testing_module()
    root = tmp_path / "table"
    table = ttf.TimeSeriesTable.create(
        table_root=str(root),
        index_column="x",
        index_type="int64",
        index_granularity=1,
    )
    source = testing._test_sql_reader_midstream_error()

    with pytest.raises(ttf.TimeseriesTableError, match="mid-stream boom") as excinfo:
        table.append(source)

    assert getattr(excinfo.value, "table_root", None) == str(root)
    assert table.version() == 1
    assert _data_and_coverage_files(str(root)) == []


def test_append_schema_error_preserves_exception_type_and_table_root(tmp_path):
    table, root = _create_table(tmp_path)
    assert table.append(_batch()) == 2
    files_before = _data_and_coverage_files(root)
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
    assert _data_and_coverage_files(root) == files_before


def test_append_overlap_preserves_exception_type_and_table_root(tmp_path):
    table, root = _create_table(tmp_path)
    assert table.append(_batch()) == 2
    files_before = _data_and_coverage_files(root)

    with pytest.raises(ttf.IndexIntervalOverlapError) as excinfo:
        table.append(_batch())

    error = excinfo.value
    assert getattr(error, "table_root", None) == root
    assert error.segment_path.startswith("data/")
    assert error.segment_path.endswith(".parquet")
    assert error.conflict_count == 2
    assert error.example_identity == {"symbol": "A"}
    assert error.example_index_interval == "[0, 10)"
    assert not hasattr(error, "example_index_interval_id")
    assert not hasattr(error, "overlap_count")
    assert not hasattr(error, "example_bucket")
    assert not hasattr(error, "example_bucket_range")
    assert table.version() == 2
    assert _data_and_coverage_files(root) == files_before


def test_append_duplicate_interval_rolls_back_and_allows_retry(tmp_path):
    table, root = _create_table(tmp_path)
    duplicate = pa.record_batch(
        {
            "ts": pa.array([0, 9], type=pa.int64()),
            "symbol": pa.array(["A", "A"]),
            "value": pa.array([1.0, 2.0]),
        }
    )

    with pytest.raises(
        ttf.DuplicateIndexIntervalError, match="Duplicate ordered-index interval"
    ) as excinfo:
        table.append(duplicate)

    error = excinfo.value
    assert type(error) is ttf.DuplicateIndexIntervalError
    assert getattr(error, "table_root", None) == root
    assert error.segment_path.startswith("data/")
    assert error.segment_path.endswith(".parquet")
    assert error.example_identity == {"symbol": "A"}
    assert error.example_index_interval == "[0, 10)"
    assert not hasattr(error, "conflict_count")
    assert not hasattr(error, "example_index_interval_id")
    assert table.version() == 1
    assert ttf.TimeSeriesTable.open(root).version() == 1
    assert _data_and_coverage_files(root) == []

    assert table.append(_batch()) == 2
    _assert_default_rows(root)


def test_interval_conflicts_without_entity_columns_have_no_example_identity(tmp_path):
    root = tmp_path / "global"
    table = ttf.TimeSeriesTable.create(
        table_root=str(root),
        index_column="ts",
        index_type="int64",
        index_granularity=10,
    )
    duplicate = pa.record_batch(
        {
            "ts": pa.array([0, 9], type=pa.int64()),
            "value": pa.array([1.0, 2.0]),
        }
    )

    with pytest.raises(ttf.DuplicateIndexIntervalError) as duplicate_error:
        table.append(duplicate)
    assert duplicate_error.value.example_identity is None

    first = pa.record_batch(
        {"ts": pa.array([0], type=pa.int64()), "value": pa.array([1.0])}
    )
    overlap = pa.record_batch(
        {"ts": pa.array([9], type=pa.int64()), "value": pa.array([2.0])}
    )
    assert table.append(first) == 2
    with pytest.raises(ttf.IndexIntervalOverlapError) as overlap_error:
        table.append(overlap)
    assert overlap_error.value.example_identity is None
    assert overlap_error.value.conflict_count == 1


def test_append_stale_writer_conflict_rolls_back_and_preserves_winner(tmp_path):
    winner, root = _create_table(tmp_path)
    stale = ttf.TimeSeriesTable.open(root)
    assert winner.append(_batch()) == 2
    files_before = _data_and_coverage_files(root)

    with pytest.raises(ttf.ConflictError) as excinfo:
        stale.append(_batch(20))

    assert getattr(excinfo.value, "expected", None) == 1
    assert getattr(excinfo.value, "found", None) == 2
    assert getattr(excinfo.value, "table_root", None) == root
    assert stale.version() == 1
    assert ttf.TimeSeriesTable.open(root).version() == 2
    assert _data_and_coverage_files(root) == files_before
    _assert_default_rows(root)


@pytest.mark.parametrize("fail_after_first", [False, True])
def test_append_releases_native_stream_exactly_once(tmp_path, fail_after_first):
    testing = _testing_module()
    root = tmp_path / "table"
    table = ttf.TimeSeriesTable.create(
        table_root=str(root),
        index_column="x",
        index_type="int64",
        index_granularity=1,
    )
    source, counter = testing._test_append_stream_with_release_counter(
        fail_after_first=fail_after_first
    )

    if fail_after_first:
        with pytest.raises(
            ttf.TimeseriesTableError, match="test append stream failure"
        ):
            table.append(source)
        assert table.version() == 1
        assert _data_and_coverage_files(str(root)) == []
    else:
        assert table.append(source) == 2

    assert counter.count == 1
    del source
    gc.collect()
    assert counter.count == 1


def test_append_maps_missing_stream_error_details_and_releases_once(tmp_path):
    testing = _testing_module()
    root = tmp_path / "table"
    table = ttf.TimeSeriesTable.create(
        table_root=str(root),
        index_column="x",
        index_type="int64",
        index_granularity=1,
    )
    source, counter = testing._test_append_stream_with_release_counter(
        fail_after_first=True,
        with_error_details=False,
    )

    with pytest.raises(
        ttf.TimeseriesTableError, match="failed without error details"
    ) as excinfo:
        table.append(source)

    assert getattr(excinfo.value, "table_root", None) == str(root)
    assert table.version() == 1
    assert _data_and_coverage_files(str(root)) == []
    assert counter.count == 1
    del source
    gc.collect()
    assert counter.count == 1


def test_append_releases_native_stream_once_when_schema_import_fails(tmp_path):
    testing = _testing_module()
    table, root = _create_table(tmp_path)
    source, counter = testing._test_append_stream_with_schema_import_error()

    with pytest.raises(ValueError, match="failed to import Arrow C Stream"):
        table.append(source)

    assert table.version() == 1
    assert _data_and_coverage_files(root) == []
    assert counter.count == 1
    del source
    gc.collect()
    assert counter.count == 1


@pytest.mark.parametrize(
    ("name", "create_options", "batch"),
    [
        (
            "negative-int64",
            {"index_column": "idx", "index_type": "int64", "index_granularity": 10},
            pa.record_batch(
                {
                    "idx": pa.array([-20, -10], type=pa.int64()),
                    "value": pa.array([1.0, 2.0]),
                }
            ),
        ),
        (
            "high-uint64",
            {"index_column": "idx", "index_type": "uint64", "index_granularity": 10},
            pa.record_batch(
                {
                    "idx": pa.array([2**64 - 20, 2**64 - 11], type=pa.uint64()),
                    "value": pa.array([1.0, 2.0]),
                }
            ),
        ),
        (
            "timestamp-timezone",
            {
                "index_column": "ts",
                "index_type": "timestamp",
                "index_granularity": "1h",
                "timezone": "America/Phoenix",
            },
            pa.record_batch(
                {
                    "ts": pa.array(
                        [0, 3_600_000_000],
                        type=pa.timestamp("us", tz="America/Phoenix"),
                    ),
                    "value": pa.array([1.0, 2.0]),
                }
            ),
        ),
    ],
)
def test_append_round_trips_index_domains_through_c_stream(
    tmp_path, name, create_options, batch
):
    root = tmp_path / name
    table = ttf.TimeSeriesTable.create(table_root=str(root), **create_options)

    assert table.append(batch) == 2

    session = ttf.Session()
    session.register_tstable("series", str(root))
    index_column = create_options["index_column"]
    result = session.sql(f'SELECT * FROM series ORDER BY "{index_column}"')
    assert result.to_pydict() == pa.Table.from_batches([batch]).to_pydict()


def test_append_releases_gil_while_consuming_slow_native_stream(tmp_path):
    testing = _testing_module()
    duration_ms = 500

    def count_while(function) -> int:
        ready = threading.Event()
        stop = threading.Event()
        counter = [0]

        def count():
            ready.set()
            while not stop.is_set():
                counter[0] += 1

        thread = threading.Thread(target=count)
        thread.start()
        assert ready.wait(timeout=1.0)
        try:
            function()
        finally:
            stop.set()
            thread.join(timeout=2.0)
        assert not thread.is_alive()
        return counter[0]

    baseline = count_while(lambda: time.sleep(duration_ms / 1000))
    root = tmp_path / "table"
    table = ttf.TimeSeriesTable.create(
        table_root=str(root),
        index_column="value",
        index_type="int64",
        index_granularity=1,
    )
    source = testing._test_sql_reader_delayed_batches(
        batch_count=5,
        rows_per_batch=1,
        delay_millis=duration_ms // 5,
    )

    during_append = count_while(lambda: table.append(source))

    assert table.version() == 2
    assert baseline > 0
    assert during_append >= max(1, int(baseline * 0.2))
