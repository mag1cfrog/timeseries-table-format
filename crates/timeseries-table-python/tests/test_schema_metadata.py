import base64
import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import timeseries_table_format as ttf


METADATA = {"unit": "ms", "description": "\u6e29\u5ea6", "opaque": '{"version":1}'}
ARROW_METADATA = {key.encode(): value.encode() for key, value in METADATA.items()}


def create(root):
    return ttf.TimeSeriesTable.create(
        table_root=str(root),
        index_column="idx",
        index_type="int64",
        index_granularity=1,
        entity_columns=["entity"],
    )


def batch(idx, metadata=None):
    return pa.record_batch(
        {"idx": [idx, idx], "entity": ["A", "B"], "value": [10, 20]}
    ).replace_schema_metadata(metadata)


def files(root):
    return {p.relative_to(root): p.read_bytes() for p in root.rglob("*") if p.is_file()}


@pytest.mark.parametrize("evolve", [False, True])
def test_schema_metadata_survives_the_table_lifecycle(tmp_path, evolve):
    table = create(tmp_path)
    table.append(batch(0, METADATA))
    commit = json.loads((tmp_path / "_timeseries_log/0000000002.json").read_text())
    meta = next(
        a["UpdateTableMeta"] for a in commit["actions"] if "UpdateTableMeta" in a
    )
    assert meta["logical_schema"]["metadata"] == METADATA
    assert meta["required_reader_features"] == ["schema_metadata"]
    assert meta["required_writer_features"] == ["schema_metadata"]

    # Missing or partial annotations inherit the canonical values.
    table.append(batch(1))
    table.append(batch(2, {"unit": "ms"}))
    column = "score" if evolve else "value"
    if evolve:
        table.add_columns(pa.schema([pa.field(column, pa.int64())]))
    update = pa.record_batch(
        [pa.array([0]), pa.array(["A"]), pa.array([99], type=pa.int32())],
        names=["idx", "entity", column],
    ).replace_schema_metadata({"opaque": METADATA["opaque"]})
    table.update_rows(update, columns=[column], expected_version=table.version())
    table.optimize()
    table = ttf.TimeSeriesTable.open(str(tmp_path))
    query = ttf.Session()
    query.register_tstable("t", str(tmp_path))
    result = query.sql("SELECT * FROM t ORDER BY idx, entity")
    assert result.num_rows == 6
    assert result[column].to_pylist() == (
        [99, None, None, None, None, None] if evolve else [99, 20, 10, 20, 10, 20]
    )
    assert result.schema.metadata == ARROW_METADATA
    # Includes both rewritten files and retained originals.
    for path in tmp_path.rglob("*.parquet"):
        assert pq.read_schema(path).metadata == ARROW_METADATA


@pytest.mark.parametrize("operation", ["append", "update"])
@pytest.mark.parametrize("metadata", [{"unit": "s"}, {"new": "value"}])
def test_conflicting_or_extra_metadata_is_rejected_before_consumption(
    tmp_path, operation, metadata
):
    table = create(tmp_path)
    table.append(batch(0, METADATA))
    version = table.version()
    before = files(tmp_path)
    source = batch(1 if operation == "append" else 0, metadata)
    consumed = []

    def batches():
        consumed.append(True)
        yield source

    reader = pa.RecordBatchReader.from_batches(source.schema, batches())
    with pytest.raises(ttf.TimeseriesTableError, match="schema metadata conflicts"):
        if operation == "append":
            table.append(reader)
        else:
            table.update_rows(reader, columns=["value"], expected_version=version)
    assert not consumed
    assert table.version() == version
    assert files(tmp_path) == before

    assert ttf.TimeSeriesTable.open(str(tmp_path)).version() == version


@pytest.mark.parametrize("metadata", [{b"bad": b"\xff"}, {b"\xff": b"bad"}])
def test_non_utf8_metadata_is_rejected_before_first_append(tmp_path, metadata):
    table = create(tmp_path)
    before = files(tmp_path)
    with pytest.raises(ValueError, match="[Uu][Tt][Ff]-?8"):
        table.append(batch(0, metadata))
    assert table.version() == 1
    assert files(tmp_path) == before


def test_legacy_schema_does_not_adopt_later_annotations(tmp_path):
    table = create(tmp_path)
    table.append(batch(0))
    table.append(batch(1, METADATA))
    query = ttf.Session()
    query.register_tstable("t", str(tmp_path))
    assert query.sql("SELECT * FROM t").schema.metadata in (None, {})


def change_file_unit(path):
    # Change only the embedded annotation, leaving data and recorded size intact.
    data = path.read_bytes()
    encoded = pq.ParquetFile(path).metadata.metadata[b"ARROW:schema"]
    decoded = base64.b64decode(encoded)
    assert decoded.count(b"ms") == data.count(encoded) == 1
    replacement = base64.b64encode(decoded.replace(b"ms", b"us"))
    assert len(encoded) == len(replacement)
    path.write_bytes(data.replace(encoded, replacement))
    assert pq.read_schema(path).metadata == {b"unit": b"us"}
    assert path.stat().st_size == len(data)


@pytest.mark.parametrize("operation", ["sql", "sql_reader", "update", "optimize"])
@pytest.mark.parametrize("evolve", [False, True])
def test_file_metadata_conflicts_are_rejected_without_publication(
    tmp_path, operation, evolve
):
    table = create(tmp_path)
    report = table.append(batch(0, {"unit": "ms"}))
    if evolve:
        table.add_columns(pa.schema([pa.field("score", pa.int64())]))
    change_file_unit(tmp_path / report.segment_path)
    before = files(tmp_path)
    version = table.version()
    error_type = (
        pa.ArrowInvalid if operation == "sql_reader" else ttf.TimeseriesTableError
    )
    with pytest.raises(error_type, match="Parquet schema metadata does not match"):
        if operation in ("sql", "sql_reader"):
            query = ttf.Session()
            query.register_tstable("t", str(tmp_path))
            if operation == "sql":
                query.sql("SELECT * FROM t")
            else:
                with query.sql_reader("SELECT * FROM t") as reader:
                    reader.read_all()
        elif operation == "update":
            table.update_rows(batch(0), columns=["value"], expected_version=version)
        else:
            table.optimize()
    assert table.version() == version
    assert files(tmp_path) == before


def test_sql_checks_only_files_selected_by_segment_pruning(tmp_path):
    table = create(tmp_path)
    table.append(batch(0, {"unit": "ms"}))
    unused = table.append(batch(1))
    change_file_unit(tmp_path / unused.segment_path)
    query = ttf.Session()
    query.register_tstable("t", str(tmp_path))
    assert query.sql("SELECT value FROM t WHERE idx = 0").num_rows == 2
    with pytest.raises(
        ttf.TimeseriesTableError, match="Parquet schema metadata does not match"
    ):
        query.sql("SELECT value FROM t WHERE idx = 1")


@pytest.mark.parametrize("kind", ["struct", "list", "map"])
def test_top_level_metadata_does_not_break_nested_optimize(tmp_path, kind):
    child = pa.field("value", pa.int64(), metadata={"unit": "ms"})
    data_type, values = {
        "struct": (pa.struct([child]), [{"value": 1}, {"value": 2}]),
        "list": (pa.list_(child.with_name("item")), [[1], [2]]),
        "map": (pa.map_(pa.string(), child), [[("a", 1)], [("b", 2)]]),
    }[kind]
    schema = pa.schema(
        [
            pa.field("idx", pa.int64()),
            pa.field("entity", pa.string()),
            pa.field("detail", data_type, metadata={"origin": "sensor"}),
        ],
        metadata=METADATA,
    )
    source = pa.record_batch(
        [pa.array([0, 0]), pa.array(["A", "B"]), pa.array(values, type=data_type)],
        schema=schema,
    )
    table = create(tmp_path)
    table.append(source)
    table.optimize()
    table = ttf.TimeSeriesTable.open(str(tmp_path))
    for path in tmp_path.rglob("*.parquet"):
        # PyArrow's Parquet schema conversion drops Map child annotations even
        # on the original file. Inspect the full Arrow schema stored in the footer.
        encoded = pq.ParquetFile(path).metadata.metadata[b"ARROW:schema"]
        restored = pa.ipc.read_schema(pa.BufferReader(base64.b64decode(encoded)))
        assert restored.equals(schema, check_metadata=True)
    query = ttf.Session()
    query.register_tstable("t", str(tmp_path))
    assert query.sql("SELECT COUNT(*) AS n FROM t")["n"].to_pylist() == [2]


def detail_field(kind, metadata):
    child = pa.field("value", pa.int64(), metadata=metadata)
    data_type = {
        "scalar": pa.int64(),
        "struct": pa.struct([child]),
        "list": pa.list_(child.with_name("item")),
        "map": pa.map_(
            pa.field("key", pa.string(), nullable=False, metadata=metadata), child
        ),
    }[kind]
    return pa.field("detail", data_type, metadata=metadata)


def detail_values(kind):
    return {
        "scalar": [1, None, 3, 4],
        "struct": [{"value": 1}, None, {"value": None}, {"value": 4}],
        "list": [[1, None], None, [], [4]],
        "map": [[("a", 1), ("b", None)], None, [], [("d", 4)]],
    }[kind]


@pytest.mark.parametrize("kind", ["scalar", "struct", "list", "map"])
@pytest.mark.parametrize("metadata", [{b"bad": b"\xff"}, {b"\xff": b"bad"}])
def test_non_utf8_field_metadata_is_rejected_before_publication(tmp_path, kind, metadata):
    table = create(tmp_path)
    schema = pa.schema([
        pa.field("idx", pa.int64()), pa.field("entity", pa.string()),
        detail_field(kind, metadata),
    ])
    source = pa.Table.from_pylist(
        [{"idx": 0, "entity": "A", "detail": detail_values(kind)[0]}], schema=schema
    )
    before = files(tmp_path)
    with pytest.raises(ValueError, match="[Uu][Tt][Ff]-?8"):
        table.append(source)
    assert files(tmp_path) == before
    assert table.version() == 1


@pytest.mark.parametrize("kind", ["struct", "list", "map"])
def test_legacy_nested_annotations_do_not_block_updates(tmp_path, kind):
    table = create(tmp_path)
    source = pa.Table.from_pylist(
        [{"idx": 0, "entity": "A", "value": 10, "detail": detail_values(kind)[0]}],
        schema=pa.schema([
            pa.field("idx", pa.int64()), pa.field("entity", pa.string()),
            pa.field("value", pa.int64()), detail_field(kind, {"unit": "ms"}),
        ]),
    )
    table.append(source)
    # Model a pre-feature table whose files contain unregistered annotations.
    path = tmp_path / "_timeseries_log/0000000002.json"
    commit = json.loads(path.read_text())

    def without_annotations(value):
        if isinstance(value, dict):
            return {
                key: without_annotations(child) for key, child in value.items()
                if key not in {"metadata", "entries_metadata", "null_value_metadata"}
            }
        if isinstance(value, list):
            return [without_annotations(child) for child in value]
        return value

    meta = next(a["UpdateTableMeta"] for a in commit["actions"] if "UpdateTableMeta" in a)
    meta["logical_schema"] = without_annotations(meta["logical_schema"])
    meta["required_reader_features"] = []
    meta["required_writer_features"] = []
    path.write_text(json.dumps(commit))
    table = ttf.TimeSeriesTable.open(str(tmp_path))
    table.update_rows(
        pa.record_batch({"idx": [0], "entity": ["A"], "value": [99]}),
        columns=["value"], expected_version=table.version(),
    )
    query = ttf.Session()
    query.register_tstable("t", str(tmp_path))
    result = query.sql("SELECT * FROM t")
    assert result["value"].to_pylist() == [99]
    assert result["detail"].to_pylist() == detail_values(kind)[:1]
    assert result.schema.field("detail").equals(detail_field(kind, None), check_metadata=True)


@pytest.mark.parametrize("kind", ["scalar", "struct", "list", "map"])
@pytest.mark.parametrize("evolve", [False, True])
def test_field_metadata_is_persisted_and_inherited(tmp_path, kind, evolve):
    table = create(tmp_path)
    field = detail_field(kind, {"unit": "ms", "description": "sample"})
    schema = pa.schema(
        [
            pa.field("idx", pa.int64()),
            pa.field("entity", pa.string()),
            pa.field("value", pa.int64()),
            field,
        ]
    )
    source = pa.record_batch(
        [
            pa.array([0, 0, 1, 1]),
            pa.array(["A", "B", "A", "B"]),
            pa.array([10, 20, 30, 40]),
            pa.array(detail_values(kind), type=field.type),
        ],
        schema=schema,
    )
    table.append(source.slice(0, 2))
    meta = next(
        action["UpdateTableMeta"]
        for action in json.loads(
            (tmp_path / "_timeseries_log/0000000002.json").read_text()
        )["actions"]
        if "UpdateTableMeta" in action
    )
    assert meta["logical_schema"]["columns"][3]["metadata"] == {
        "unit": "ms", "description": "sample"
    }
    assert meta["required_reader_features"] == ["schema_metadata"]
    assert meta["required_writer_features"] == ["schema_metadata"]

    bare_schema = schema.set(3, detail_field(kind, None))
    table.append(pa.Table.from_pylist(source.slice(2).to_pylist(), schema=bare_schema))
    if evolve:
        table.add_columns(pa.schema([pa.field("score", pa.int64())]))
        schema = schema.append(pa.field("score", pa.int64()))
    # This is the #447 reproduction: update an unrelated scalar column.
    table.update_rows(
        batch(0), columns=["value"], expected_version=table.version()
    )
    replacement = pa.Table.from_pylist(
        [{"idx": 1, "entity": "B", "detail": detail_values(kind)[0]}],
        schema=pa.schema([bare_schema.field(i) for i in [0, 1, 3]]),
    )
    table.update_rows(
        replacement, columns=["detail"], expected_version=table.version()
    )
    table.optimize()
    table = ttf.TimeSeriesTable.open(str(tmp_path))
    query = ttf.Session()
    query.register_tstable("t", str(tmp_path))
    result = query.sql("SELECT * FROM t ORDER BY idx, entity")
    assert result.schema.equals(schema, check_metadata=True)
    assert result["detail"].to_pylist() == detail_values(kind)[:3] + [detail_values(kind)[0]]
    if evolve:
        assert result["score"].to_pylist() == [None] * 4
    for path in tmp_path.rglob("*.parquet"):
        encoded = pq.ParquetFile(path).metadata.metadata[b"ARROW:schema"]
        stored = pa.ipc.read_schema(pa.BufferReader(base64.b64decode(encoded)))
        assert stored.field("detail").equals(field, check_metadata=True)


@pytest.mark.parametrize("kind", ["scalar", "struct", "list", "map"])
@pytest.mark.parametrize("operation", ["append", "update"])
@pytest.mark.parametrize("metadata", [{"unit": "us"}, {"extra": "value"}])
def test_field_metadata_conflicts_are_atomic(tmp_path, kind, operation, metadata):
    table = create(tmp_path)
    source = pa.Table.from_pylist(
        [{"idx": 0, "entity": "A", "detail": detail_values(kind)[0]}],
        schema=pa.schema([
            pa.field("idx", pa.int64()), pa.field("entity", pa.string()),
            detail_field(kind, {"unit": "ms"}),
        ]),
    )
    table.append(source)
    before = files(tmp_path)
    version = table.version()
    changed_field = detail_field(kind, metadata)
    if kind != "scalar":
        # Exercise the nested annotation check, keeping the outer field unchanged.
        changed_field = changed_field.with_metadata(source.schema.field(2).metadata)
    changed = source.schema.set(2, changed_field)
    consumed = []

    def batches():
        consumed.append(True)
        yield pa.Table.from_pylist(source.to_pylist(), schema=changed).to_batches()[0]

    reader = pa.RecordBatchReader.from_batches(changed, batches())
    with pytest.raises(ttf.TimeseriesTableError, match="field metadata conflicts"):
        if operation == "append":
            table.append(reader)
        else:
            table.update_rows(reader, columns=["detail"], expected_version=version)
    assert not consumed
    assert table.version() == version
    assert files(tmp_path) == before


@pytest.mark.parametrize("kind", ["scalar", "struct", "list", "map"])
@pytest.mark.parametrize("operation", ["sql", "update", "optimize"])
def test_committed_field_metadata_conflicts_are_rejected(tmp_path, kind, operation):
    table = create(tmp_path)
    field = detail_field(kind, {"unit": "ms"})
    source = pa.Table.from_pylist(
        [
            {"idx": 0, "entity": "A", "detail": detail_values(kind)[0]},
            {"idx": 0, "entity": "B", "detail": detail_values(kind)[0]},
        ],
        schema=pa.schema([
            pa.field("idx", pa.int64()), pa.field("entity", pa.string()), field
        ]),
    )
    report = table.append(source)
    path = tmp_path / report.segment_path
    data = path.read_bytes()
    encoded = pq.ParquetFile(path).metadata.metadata[b"ARROW:schema"]
    decoded = base64.b64decode(encoded)
    assert b"ms" in decoded
    changed = base64.b64encode(decoded.replace(b"ms", b"us"))
    assert len(changed) == len(encoded) and data.count(encoded) == 1
    path.write_bytes(data.replace(encoded, changed))
    before = files(tmp_path)
    version = table.version()
    with pytest.raises(ttf.TimeseriesTableError, match="Parquet field metadata does not match"):
        if operation == "sql":
            query = ttf.Session()
            query.register_tstable("t", str(tmp_path))
            query.sql("SELECT * FROM t")
        elif operation == "update":
            table.update_rows(source, columns=["detail"], expected_version=version)
        else:
            table.optimize()
    assert files(tmp_path) == before
    assert table.version() == version
    assert ttf.TimeSeriesTable.open(str(tmp_path)).version() == version
