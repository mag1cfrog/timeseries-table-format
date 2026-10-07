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
