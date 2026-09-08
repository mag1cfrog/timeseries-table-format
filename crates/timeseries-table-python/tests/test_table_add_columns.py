import json

import pyarrow as pa
import pytest

import timeseries_table_format as ttf


BASE_SCHEMA = pa.schema(
    [
        pa.field("idx", pa.int64(), nullable=False),
        pa.field("exchange", pa.int32(), nullable=False),
        pa.field("symbol", pa.string(), nullable=False),
        pa.field("value", pa.float64()),
    ]
)
ADDITIONS = pa.schema(
    [pa.field("quality", pa.float64()), pa.field("reviewed", pa.bool_())]
)


def _batch(idx, *, schema=BASE_SCHEMA, **payload):
    values = {"idx": [idx, idx], "exchange": [1, 2], "symbol": ["A", "B"], **payload}
    values.setdefault("value", [1.0, 2.0])
    return pa.RecordBatch.from_arrays(
        [pa.array(values[field.name], type=field.type) for field in schema],
        schema=schema,
    )


def _create(root, *, append=True):
    table = ttf.TimeSeriesTable.create(
        table_root=str(root),
        index_column="idx",
        index_type="int64",
        index_granularity=1,
        entity_columns=["exchange", "symbol"],
    )
    if append:
        table.append(_batch(0))
    return table


def _files(root):
    return {
        path.relative_to(root).as_posix(): path.read_bytes()
        for path in root.rglob("*")
        if path.is_file()
    }


def test_add_columns_query_append_optimize_and_reopen(tmp_path):
    root = tmp_path / "table"
    table = _create(root)
    session = ttf.Session()
    session.register_tstable("t", str(root))
    before = _files(root)
    assert table.add_columns(ADDITIONS) == table.version() == 3
    after = _files(root)
    assert len(after) == len(before) + 1
    for path, contents in before.items():
        if path != "_timeseries_log/CURRENT":
            assert after[path] == contents
    actions = json.loads(after["_timeseries_log/0000000003.json"])["actions"]
    assert len(actions) == 1
    assert list(actions[0]) == ["UpdateTableMeta"]
    assert actions[0]["UpdateTableMeta"]["required_reader_features"] == [
        "schema_add_columns"
    ]
    assert actions[0]["UpdateTableMeta"]["required_writer_features"] == []

    for query in ["SELECT * FROM t", "SELECT COUNT(*) FROM t"]:
        with pytest.raises(ttf.DataFusionError, match="schema changed; re-register"):
            session.sql(query)
    session.register_tstable("t", str(root))
    old = session.sql("SELECT * FROM t ORDER BY idx, exchange, symbol")
    assert old.column_names == BASE_SCHEMA.names + ADDITIONS.names
    assert old["quality"].to_pylist() == old["reviewed"].to_pylist() == [None, None]

    schema = pa.schema([*BASE_SCHEMA, *ADDITIONS])
    table.append(_batch(1, schema=schema, quality=[7.0, None], reviewed=[True, False]))
    # Even pre-existing nullable payloads may be omitted after evolution.
    table.append(_batch(2, schema=pa.schema(list(BASE_SCHEMA)[:3])))
    table = ttf.TimeSeriesTable.open(str(root))
    assert table.version() == 5
    before_optimize = session.sql("SELECT * FROM t ORDER BY idx, exchange, symbol")
    assert before_optimize.to_pydict() == {
        "idx": [0, 0, 1, 1, 2, 2],
        "exchange": [1, 2, 1, 2, 1, 2],
        "symbol": ["A", "B", "A", "B", "A", "B"],
        "value": [1.0, 2.0, 1.0, 2.0, None, None],
        "quality": [None, None, 7.0, None, None, None],
        "reviewed": [None, None, True, False, None, None],
    }
    for optimize in [False, True]:
        if optimize:
            report = table.optimize()
            assert report.source_segments_replaced == 3
            assert report.distinct_identities_materialized == 2
            assert report.rows_read == report.rows_written == 6
        assert session.sql("SELECT * FROM t ORDER BY idx, exchange, symbol").equals(
            before_optimize
        )
        for predicate, count in [
            ("true", 6),
            ("quality IS NULL", 5),
            ("quality IS NOT NULL", 1),
            ("quality = 7", 1),
            ("quality > 7", 0),
            ("quality IS NULL AND exchange = 1", 2),
        ]:
            assert session.sql(f"SELECT COUNT(*) AS n FROM t WHERE {predicate}")[
                "n"
            ].to_pylist() == [count]
        assert session.sql(
            "SELECT COUNT(quality) AS n, SUM(quality) AS total FROM t"
        ).to_pydict() == {"n": [1], "total": [7.0]}
        assert session.sql("SELECT quality FROM t ORDER BY idx, exchange, symbol")[
            "quality"
        ].to_pylist() == [None, None, 7.0, None, None, None]
        # Duplicate identity/index intervals remain occupied after metadata changes and rewrites.
        with pytest.raises(ttf.IndexIntervalOverlapError):
            ttf.TimeSeriesTable.open(str(root)).append(_batch(0))

    assert table.add_columns(pa.schema([pa.field("later", pa.int64())])) == 7
    with pytest.raises(ttf.DataFusionError, match="schema changed; re-register"):
        session.sql("SELECT COUNT(*) FROM t")
    session.register_tstable("t", str(root))
    assert session.sql("SELECT later FROM t")["later"].to_pylist() == [None] * 6
    assert ttf.TimeSeriesTable.open(str(root)).version() == table.version()


@pytest.mark.parametrize(
    "columns",
    [
        None,
        [],
        {},
        "quality: int64",
        pa.field("quality", pa.int64()),
        pa.table({"quality": [1]}),
    ],
)
def test_wrong_type_is_not_coerced(tmp_path, columns):
    root = tmp_path / "table"
    table = _create(root)
    before = _files(root)
    with pytest.raises(TypeError, match="pyarrow.Schema") as error:
        table.add_columns(columns)
    assert type(error.value) is TypeError
    assert getattr(error.value, "table_root") == str(root)
    assert table.version() == 2
    assert _files(root) == before


@pytest.mark.parametrize(
    "columns",
    [
        pa.schema([pa.field("new", pa.int64())], metadata={"source": "x"}),
        pa.schema([pa.field("new", pa.int64(), metadata={"source": "x"})]),
        pa.schema(
            [
                pa.field(
                    "new",
                    pa.struct([pa.field("child", pa.int64(), metadata={"x": "y"})]),
                )
            ]
        ),
        pa.schema(
            [
                pa.field(
                    "new", pa.list_(pa.field("item", pa.int64(), metadata={"x": "y"}))
                )
            ]
        ),
        pa.schema(
            [
                pa.field(
                    "new",
                    pa.map_(
                        pa.field(
                            "key", pa.string(), nullable=False, metadata={"x": "y"}
                        ),
                        pa.int64(),
                    ),
                )
            ]
        ),
        pa.schema(
            [
                pa.field(
                    "new",
                    pa.map_(
                        pa.string(), pa.field("value", pa.int64(), metadata={"x": "y"})
                    ),
                )
            ]
        ),
        pa.schema(
            [
                pa.field(
                    "new",
                    pa.dictionary(
                        pa.int32(),
                        pa.struct([pa.field("child", pa.int64(), metadata={"x": "y"})]),
                    ),
                )
            ]
        ),
        pa.schema([pa.field("new", pa.int64())], metadata={b"binary": b"\xff"}),
    ],
)
def test_metadata_is_rejected_without_publication(tmp_path, columns):
    root = tmp_path / "table"
    table = _create(root)
    before = _files(root)
    with pytest.raises(ValueError) as error:
        table.add_columns(columns)
    assert type(error.value) is ValueError
    assert table.version() == 2
    assert _files(root) == before


@pytest.mark.parametrize(
    "data_type",
    [
        pa.decimal128(10, -129),
        pa.decimal256(40, 128),
        pa.list_(pa.decimal128(10, 128)),
        pa.dense_union(
            [pa.field("a", pa.int64()), pa.field("b", pa.int64())],
            type_codes=[0, 0],
        ),
    ],
)
def test_unimportable_arrow_parameters_are_atomic_value_errors(tmp_path, data_type):
    root = tmp_path / "table"
    table = _create(root)
    before = _files(root)
    columns = pa.schema([pa.field("valid", pa.int64()), pa.field("bad", data_type)])
    with pytest.raises(ValueError, match="Cannot import Arrow schema") as error:
        table.add_columns(columns)
    assert type(error.value) is ValueError
    assert getattr(error.value, "table_root") == str(root)
    assert table.version() == ttf.TimeSeriesTable.open(str(root)).version() == 2
    assert _files(root) == before
    # A rejected representation must leave the same handle usable.
    assert table.add_columns(ADDITIONS) == 3


@pytest.mark.parametrize(
    "columns,context",
    [
        (pa.schema([]), "at least one"),
        (pa.schema([pa.field("new", pa.int64()), pa.field("new", pa.int64())]), "new"),
        (pa.schema([pa.field("value", pa.float64())]), "value"),
        (pa.schema([pa.field("idx", pa.int64())]), "idx"),
        (pa.schema([pa.field("symbol", pa.string())]), "symbol"),
        (pa.schema([pa.field("", pa.int64())]), "empty"),
        (pa.schema([pa.field(" \t", pa.int64())]), "whitespace"),
        (pa.schema([pa.field("new", pa.int64(), nullable=False)]), "new"),
        (pa.schema([pa.field("new", pa.date32())]), "new"),
        (pa.schema([pa.field("new", pa.dictionary(pa.int32(), pa.string()))]), "new"),
        (pa.schema([pa.field("new", pa.struct([]))]), "new"),
        (
            pa.schema([pa.field("valid", pa.int64()), pa.field("bad", pa.uint8())]),
            "bad",
        ),
    ],
)
def test_core_schema_failures_are_atomic_and_typed(tmp_path, columns, context):
    root = tmp_path / "table"
    table = _create(root)
    before = _files(root)
    with pytest.raises(ttf.SchemaMismatchError, match=context) as error:
        table.add_columns(columns)
    assert type(error.value) is ttf.SchemaMismatchError
    assert getattr(error.value, "table_root") == str(root)
    assert table.version() == ttf.TimeSeriesTable.open(str(root)).version() == 2
    assert _files(root) == before


def test_schema_adoption_is_required_and_stale_handles_do_not_refresh(tmp_path):
    root = tmp_path / "table"
    table = _create(root, append=False)
    before = _files(root)
    with pytest.raises(ttf.SchemaMismatchError, match="established canonical schema"):
        table.add_columns(ADDITIONS)
    assert table.version() == 1
    assert _files(root) == before
    table.append(_batch(0))
    stale = ttf.TimeSeriesTable.open(str(root))
    assert table.add_columns(ADDITIONS) == 3
    before = _files(root)
    with pytest.raises(ttf.ConflictError) as error:
        stale.add_columns(pa.schema([pa.field("other", pa.int64())]))
    assert type(error.value) is ttf.ConflictError
    assert getattr(error.value, "expected") == 2
    assert getattr(error.value, "found") == 3
    assert getattr(error.value, "table_root") == str(root)
    assert stale.version() == 2
    assert _files(root) == before


def test_exact_names_nested_values_and_nullable_annotations(tmp_path):
    root = tmp_path / "table"
    table = _create(root)
    columns = pa.schema(
        [
            pa.field("Value", pa.int64()),
            pa.field(" value ", pa.string()),
            pa.field("nested.value", pa.int64()),
            pa.field(
                "nested", pa.struct([pa.field("value", pa.int64(), nullable=False)])
            ),
        ],
        metadata={},
    )
    assert table.add_columns(columns=columns) == 3
    assert columns.names == ["Value", " value ", "nested.value", "nested"]
    table.append(
        _batch(
            1,
            schema=pa.schema([*BASE_SCHEMA, *columns]),
            **{
                "Value": [1, 2],
                " value ": ["yes", "no"],
                "nested.value": [3, 4],
                "nested": [{"value": 5}, {"value": 6}],
            },
        )
    )
    session = ttf.Session()
    session.register_tstable("t", str(root))
    assert session.sql(
        'SELECT "Value", " value ", "nested.value", nested[\'value\'] AS child FROM t ORDER BY idx, exchange'
    ).to_pydict() == {
        "Value": [None, None, 1, 2],
        " value ": [None, None, "yes", "no"],
        "nested.value": [None, None, 3, 4],
        "child": [None, None, 5, 6],
    }
    invalid = pa.schema([*BASE_SCHEMA, pa.field("Value", pa.int64(), nullable=False)])
    before = _files(root)
    with pytest.raises(ttf.SchemaMismatchError):
        table.append(_batch(2, schema=invalid, Value=[1, 2]))
    assert table.version() == 4
    assert _files(root) == before
