import gc
import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import timeseries_table_format as ttf
from timeseries_table_format import _native


SCHEMA = pa.schema(
    [
        pa.field("idx", pa.int64()),
        pa.field("exchange", pa.int32()),
        pa.field("symbol", pa.string()),
        pa.field("value", pa.int64()),
        pa.field("blob", pa.binary()),
        pa.field("detail", pa.struct([pa.field("items", pa.list_(pa.int64()))])),
    ]
)
KEYS = ["idx", "exchange", "symbol"]


def batch(rows, names=KEYS + ["value"], schema=SCHEMA):
    selected = pa.schema([schema.field(name) for name in names])
    return pa.RecordBatch.from_pylist(rows, schema=selected)


def row(idx=0, *, exchange=1, symbol="A", value=90, **extra):
    return dict(idx=idx, exchange=exchange, symbol=symbol, value=value, **extra)


def create(root):
    table = ttf.TimeSeriesTable.create(
        table_root=str(root),
        index_column="idx",
        index_type="int64",
        index_granularity=1,
        entity_columns=["exchange", "symbol"],
    )
    for idx in [0, 1, 2]:
        table.append(
            batch(
                [
                    row(
                        idx,
                        exchange=e,
                        symbol=s,
                        value=idx + e,
                        blob=b"wide" * 1024,
                        detail={"items": [idx, e]},
                    )
                    for e, s in [(1, "A"), (2, "B")]
                ],
                SCHEMA.names,
            )
        )
    return table


def files(root):
    return {
        p.relative_to(root).as_posix(): p.read_bytes()
        for p in root.rglob("*")
        if p.is_file()
    }


def session(root):
    result = ttf.Session()
    result.register_tstable("t", str(root))
    return result


def read(query):
    return query.sql("SELECT * FROM t ORDER BY idx, exchange, symbol")


def test_update_workflow_preserves_unselected_values_and_composes(tmp_path):
    root = tmp_path / "t"
    table = create(root)
    table.add_columns(pa.schema([pa.field("label", pa.string())]))
    version = table.version()
    query = session(root)
    before = read(query)
    original = files(root)
    schema = pa.schema([*SCHEMA, pa.field("label", pa.string())])
    source = batch(
        [
            row(2, exchange=2, symbol="B", value=None, label="reviewed"),
            row(value=99, label="first"),
        ],
        ["label", "symbol", "idx", "value", "exchange"],
        schema,
    )
    seen = []

    def batches():
        for part in [source.slice(0, 1), source.slice(1, 1)]:
            seen.append(part)
            yield part

    report = table.update_rows(
        pa.RecordBatchReader.from_batches(source.schema, batches()),
        columns=("value", "label"),
        expected_version=version,
    )
    assert len(seen) == 2
    assert isinstance(report, ttf.UpdateRowsReport)
    assert report.starting_version == version
    assert report.committed_version == table.version() == version + 1
    assert report.rows_updated == report.segments_rewritten == 2
    assert report.no_op is False
    assert "rows_updated=2" in repr(report)
    for field in [
        "starting_version",
        "committed_version",
        "rows_updated",
        "segments_rewritten",
        "source_file_bytes",
        "replacement_file_bytes",
        "no_op",
    ]:
        with pytest.raises(AttributeError):
            setattr(report, field, 0)
    after = read(query)  # No SQL re-registration after a value update.
    assert after.schema == before.schema
    assert after.select(KEYS + ["blob", "detail"]).equals(
        before.select(KEYS + ["blob", "detail"])
    )
    assert after["value"].to_pylist() == [99, 2, 2, 3, 3, None]
    assert after["label"].to_pylist() == ["first", None, None, None, None, "reviewed"]
    assert query.sql("SELECT count(*) n, sum(value) total FROM t").to_pydict() == {
        "n": [6],
        "total": [109],
    }
    assert query.sql("SELECT count(*) n FROM t WHERE value IS NULL")[
        "n"
    ].to_pylist() == [1]
    assert query.sql("SELECT count(*) n FROM t WHERE value > 90 AND symbol = 'A'")[
        "n"
    ].to_pylist() == [1]
    committed = files(root)
    actions = json.loads(committed[f"_timeseries_log/{version + 1:010}.json"])[
        "actions"
    ]
    removed = [a["RemoveSegment"]["path"] for a in actions if "RemoveSegment" in a]
    added = [a["AddSegment"] for a in actions if "AddSegment" in a]
    assert len(removed) == len(added) == 2
    assert report.source_file_bytes == sum(len(original[p]) for p in removed)
    assert report.replacement_file_bytes == sum(
        len(committed[a["path"]]) for a in added
    )
    for path, contents in original.items():
        if path != "_timeseries_log/CURRENT":
            assert committed[path] == contents
    table = ttf.TimeSeriesTable.open(str(root))
    assert table.version() == report.committed_version
    assert read(session(root)).equals(after)
    table.optimize()
    assert read(query).equals(after)
    table.append(batch([row(3, value=4)], KEYS + ["value"]))
    assert read(query).num_rows == 7
    with pytest.raises(ttf.IndexIntervalOverlapError):
        table.append(batch([row()]))


@pytest.mark.parametrize("form", ["batch", "table", "reader", "exporter"])
def test_input_forms_empty_and_equal_assignments(tmp_path, form):
    table = create(tmp_path)
    source = batch([row(value=1)])
    calls = []

    class Exporter:
        def __arrow_c_stream__(self, requested_schema=None):
            calls.append("export")
            return source.__arrow_c_stream__(requested_schema)

    inputs = {
        "batch": lambda: source,
        "table": lambda: pa.Table.from_batches([source]),
        "reader": lambda: pa.RecordBatchReader.from_batches(source.schema, [source]),
        "exporter": Exporter,
    }
    old_version = table.version()
    report = table.update_rows(
        inputs[form](), columns=["value"], expected_version=old_version
    )
    assert not report.no_op and report.rows_updated == 1
    assert table.version() == old_version + 1
    assert calls == (["export"] if form == "exporter" else [])
    before = files(tmp_path)
    report = table.update_rows(
        pa.RecordBatchReader.from_batches(source.schema, [source.slice(0, 0)] * 3),
        columns=["value"],
        expected_version=table.version(),
    )
    assert report.no_op
    assert report.starting_version == report.committed_version == table.version()
    assert report.rows_updated == report.segments_rewritten == 0
    assert report.source_file_bytes == report.replacement_file_bytes == 0
    assert files(tmp_path) == before


@pytest.mark.parametrize(
    "argument, value, error_type",
    [
        ("columns", "value", TypeError),
        ("columns", b"value", TypeError),
        ("columns", {"value"}, TypeError),
        ("columns", {"value": 1}, TypeError),
        ("columns", None, TypeError),
        ("columns", [1], TypeError),
        ("columns", [b"value"], TypeError),
        ("columns", iter(["value"]), TypeError),
        ("expected_version", True, TypeError),
        ("expected_version", False, TypeError),
        ("expected_version", 2.0, TypeError),
        ("expected_version", "2", TypeError),
        ("expected_version", None, TypeError),
        ("expected_version", 0, ValueError),
        ("expected_version", -1, ValueError),
        ("expected_version", 2**64, ValueError),
    ],
)
def test_representation_errors_precede_export(tmp_path, argument, value, error_type):
    table = create(tmp_path)
    before = files(tmp_path)

    class Uninspected:
        def __arrow_c_stream__(self):
            pytest.fail("invalid representation must not export source")

    kwargs = {"columns": ["value"], "expected_version": table.version()}
    kwargs[argument] = value
    with pytest.raises(error_type) as error:
        table.update_rows(Uninspected(), **kwargs)
    assert type(error.value) is error_type
    assert getattr(error.value, "table_root") == str(tmp_path)
    assert files(tmp_path) == before


@pytest.mark.parametrize("source", ["file.parquet", [], {}, lambda: None, None])
def test_invalid_source_forms_have_root(tmp_path, source):
    table = create(tmp_path)
    with pytest.raises(TypeError) as error:
        table.update_rows(source, columns=["value"], expected_version=table.version())
    assert getattr(error.value, "table_root") == str(tmp_path)


@pytest.mark.parametrize(
    "columns", [[], ["value", "value"], ["idx"], ["symbol"], ["absent"]]
)
def test_destination_validation_stays_in_core(tmp_path, columns):
    table = create(tmp_path)
    before = files(tmp_path)
    with pytest.raises(ttf.SchemaMismatchError) as error:
        table.update_rows(
            batch([row()]), columns=columns, expected_version=table.version()
        )
    assert getattr(error.value, "table_root") == str(tmp_path)
    assert files(tmp_path) == before


@pytest.mark.parametrize("case", ["missing", "extra", "type", "annotation", "empty"])
def test_schema_failures_are_atomic(tmp_path, case):
    table = create(tmp_path)
    source = batch([row()])
    if case == "missing":
        source = source.select(KEYS)
    elif case == "extra":
        source = source.append_column("extra", pa.array([1]))
    elif case == "empty":
        source = pa.record_batch([], names=[])
    else:
        fields = list(source.schema)
        fields[-1] = pa.field(
            "value",
            pa.string() if case == "type" else pa.int64(),
            nullable=case == "type",
        )
        source = batch(
            [row(value="bad" if case == "type" else 90)], schema=pa.schema(fields)
        )
    before = files(tmp_path)
    with pytest.raises(ttf.SchemaMismatchError):
        table.update_rows(source, columns=["value"], expected_version=table.version())
    assert files(tmp_path) == before


@pytest.mark.parametrize(
    "kind", ["duplicate_source_key", "unmatched_source_key", "null_identity"]
)
def test_key_failures_preserve_diagnostics_and_table(tmp_path, kind):
    table = create(tmp_path)
    bad = row() if kind == "duplicate_source_key" else row(99)
    if kind == "null_identity":
        bad = row(symbol=None)
    parts = [batch([row()]), *[batch([]) for _ in range(20)], batch([bad])]
    before = files(tmp_path)
    version = table.version()
    with pytest.raises(ttf.TimeseriesTableError) as error:
        table.update_rows(
            pa.RecordBatchReader.from_batches(parts[0].schema, parts),
            columns=["value"],
            expected_version=version,
        )
    err = error.value
    assert type(err) is ttf.TimeseriesTableError
    assert getattr(err, "reason") == kind
    assert getattr(err, "input_rows_seen") == 2
    assert getattr(err, "observed_violations") == 1
    assert getattr(err, "example_key") == {k: bad[k] for k in KEYS}
    assert getattr(err, "table_root") == str(tmp_path)
    assert (
        table.version() == ttf.TimeSeriesTable.open(str(tmp_path)).version() == version
    )
    assert files(tmp_path) == before


def test_versions_are_required_exact_and_never_rebased(tmp_path):
    table = create(tmp_path)
    stale = ttf.TimeSeriesTable.open(str(tmp_path))
    version = table.version()
    source = batch([row()])
    with pytest.raises(TypeError):
        table.update_rows(source, ["value"], version)
    with pytest.raises(TypeError):
        table.update_rows(source, columns=["value"])
    table.update_rows(source, columns=["value"], expected_version=version)
    for handle, expected, found in [
        (stale, version, version + 1),
        (table, version, version + 1),
        (stale, version + 1, version),
        (table, 2**64 - 1, version + 1),
    ]:
        consumed = []

        def batches():
            consumed.append(True)
            yield source

        with pytest.raises(ttf.ConflictError) as error:
            handle.update_rows(
                pa.RecordBatchReader.from_batches(source.schema, batches()),
                columns=["value"],
                expected_version=expected,
            )
        assert getattr(error.value, "expected") == expected
        assert getattr(error.value, "found") == found
        assert consumed == []
    assert stale.version() == version


@pytest.mark.parametrize("empty", [False, True])
def test_late_stream_error_has_no_partial_commit(tmp_path, empty):
    table = create(tmp_path)
    before = files(tmp_path)
    version = table.version()
    part = batch([] if empty else [row()])

    def batches():
        yield part
        yield part.slice(0, 0)
        raise RuntimeError("upstream computation failed")

    reader = pa.RecordBatchReader.from_batches(part.schema, batches())
    with pytest.raises(ttf.TimeseriesTableError, match="upstream computation failed"):
        table.update_rows(reader, columns=["value"], expected_version=version)
    reader.close()
    assert table.version() == version
    assert files(tmp_path) == before


@pytest.mark.parametrize("fail, details", [(False, True), (True, True), (True, False)])
def test_native_stream_is_released_once(tmp_path, fail, details):
    testing = getattr(_native, "_testing", None)
    if testing is None:
        pytest.skip("requires test-utils")
    table = ttf.TimeSeriesTable.create(
        table_root=str(tmp_path),
        index_column="x",
        index_type="int64",
        index_granularity=1,
    )
    schema = pa.schema(
        [pa.field(n, pa.int64(), nullable=False) for n in ["x", "value"]]
    )
    table.append(
        pa.Table.from_arrays([pa.array([1, 2]), pa.array([0, 0])], schema=schema)
    )
    source, counter = testing._test_append_stream_with_release_counter(
        fail_after_first=fail,
        with_error_details=details,
        with_payload=True,
    )
    if fail:
        message = (
            "test append stream failure" if details else "failed without error details"
        )
        with pytest.raises(ttf.TimeseriesTableError, match=message):
            table.update_rows(source, columns=["value"], expected_version=2)
        assert table.version() == 2
    else:
        assert (
            table.update_rows(
                source, columns=["value"], expected_version=2
            ).rows_updated
            == 2
        )
    assert counter.count == 1
    del source
    gc.collect()
    assert counter.count == 1


@pytest.mark.parametrize(
    "unit, ticks", [("ms", 1001), ("us", 1_000_001), ("ns", 1_000_000_001)]
)
@pytest.mark.parametrize("timezone", [None, "UTC"])
def test_nested_destinations_literal_dots_no_entities_and_precise_keys(
    tmp_path, unit, ticks, timezone
):
    root = tmp_path / "nested"
    table = ttf.TimeSeriesTable.create(
        table_root=str(root),
        index_column="clock",
        index_type="timestamp",
        index_granularity="1s",
    )
    schema = pa.schema(
        [
            pa.field("clock", pa.timestamp(unit, timezone)),
            pa.field("a.b", pa.struct([pa.field("items", pa.list_(pa.int64()))])),
        ]
    )

    def source(ticks, value):
        return pa.Table.from_arrays(
            [
                pa.array([ticks], type=schema.field("clock").type),
                pa.array([value], type=schema.field("a.b").type),
            ],
            schema=schema,
        )

    table.append(source(ticks, {"items": [1, 2]}))
    version = table.version()
    with pytest.raises(ttf.TimeseriesTableError) as error:
        table.update_rows(
            source(ticks + 2, None), columns=["a.b"], expected_version=version
        )
    assert type(error.value) is ttf.TimeseriesTableError
    assert getattr(error.value, "reason") == "unmatched_source_key"
    key = getattr(error.value, "example_key")
    assert list(key) == ["clock"]
    assert isinstance(key["clock"], pa.TimestampScalar)
    assert key["clock"].type == pa.timestamp(unit, timezone)
    assert key["clock"].value == ticks + 2
    table.update_rows(
        source(ticks, {"items": [9, None]}),
        columns=["a.b"],
        expected_version=version,
    )
    query = session(root)
    assert query.sql('SELECT "a.b" FROM t')["a.b"].to_pylist() == [{"items": [9, None]}]


def test_unsigned_keys_are_not_truncated(tmp_path):
    table = ttf.TimeSeriesTable.create(
        table_root=str(tmp_path),
        index_column="tick",
        index_type="uint64",
        index_granularity=1,
    )

    def source(tick):
        return pa.table({"tick": pa.array([tick], type=pa.uint64()), "value": [1]})

    table.append(source(2**64 - 2))
    with pytest.raises(ttf.TimeseriesTableError) as error:
        table.update_rows(source(2**64 - 1), columns=["value"], expected_version=2)
    assert getattr(error.value, "example_key") == {"tick": 2**64 - 1}


def test_lossless_scalar_widening_and_arrow_metadata_follow_append(tmp_path):
    table = create(tmp_path)
    fields = [SCHEMA.field(k) for k in KEYS] + [
        pa.field("value", pa.int32(), metadata={"origin": "external"})
    ]
    source = batch([row(value=17)], schema=pa.schema(fields))
    source = source.replace_schema_metadata({"source": "outside"})
    version = table.version()
    table.update_rows(source, columns=["value"], expected_version=version)
    result = read(session(tmp_path))
    assert result.schema == SCHEMA
    assert result["value"][0].as_py() == 17


def test_ambiguous_target_uses_key_error_not_append_interval_error(tmp_path):
    table = create(tmp_path)
    # Valid public writes prohibit this ambiguity. Corrupt just one physical source
    # to exercise the defensive exact-key check against an already opened snapshot.
    second = next(tmp_path.rglob("*.parquet"))
    data = pq.read_table(second)
    old_idx = data["idx"][0].as_py()
    other_idx = (old_idx + 1) % 3
    pq.write_table(
        data.set_column(0, SCHEMA.field("idx"), pa.array([other_idx, other_idx])),
        second,
    )
    version = table.version()
    before = files(tmp_path)
    with pytest.raises(ttf.TimeseriesTableError) as error:
        table.update_rows(
            batch([row(other_idx)]), columns=["value"], expected_version=version
        )
    assert type(error.value) is ttf.TimeseriesTableError
    assert getattr(error.value, "reason") == "ambiguous_target_key"
    assert getattr(error.value, "example_key") == {
        "idx": other_idx,
        "exchange": 1,
        "symbol": "A",
    }
    assert table.version() == version
    assert files(tmp_path) == before


def test_missing_source_file_preserves_storage_path(tmp_path):
    table = create(tmp_path)
    missing = next(tmp_path.rglob("*.parquet"))
    missing.unlink()
    with pytest.raises(ttf.StorageError) as error:
        table.update_rows(
            batch([row()]), columns=["value"], expected_version=table.version()
        )
    assert getattr(error.value, "table_root") == str(tmp_path)
    assert getattr(error.value, "path").endswith(missing.name)


def test_stream_import_failure_releases_interface_and_preserves_cause(tmp_path):
    table = create(tmp_path)

    class ExportFailure:
        def __arrow_c_stream__(self):
            raise RuntimeError("export failed upstream")

    with pytest.raises(ValueError) as error:
        table.update_rows(
            ExportFailure(), columns=["value"], expected_version=table.version()
        )
    assert getattr(error.value, "table_root") == str(tmp_path)
    assert isinstance(error.value.__cause__, RuntimeError)
    testing = getattr(_native, "_testing", None)
    if testing is None:
        pytest.skip("requires test-utils")
    source, counter = testing._test_append_stream_with_schema_import_error()
    with pytest.raises(ValueError, match="failed to import Arrow C Stream"):
        table.update_rows(source, columns=["value"], expected_version=table.version())
    assert counter.count == 1
    del source
    gc.collect()
    assert counter.count == 1
