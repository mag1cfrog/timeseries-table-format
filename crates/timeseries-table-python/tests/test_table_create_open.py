import inspect
from typing import Any, cast

import pytest
import timeseries_table_format as ttf


def test_create_then_open(tmp_path):
    root = tmp_path / "table"

    t1 = ttf.TimeSeriesTable.create(
        table_root=str(root),
        index_column="ts",
        index_type="timestamp",
        index_granularity="1h",
        entity_columns=["symbol"],
        timezone=None,
    )
    assert isinstance(t1, ttf.TimeSeriesTable)

    t2 = ttf.TimeSeriesTable.open(str(root))
    assert isinstance(t2, ttf.TimeSeriesTable)


def test_open_error_includes_root_in_message(tmp_path):
    root = tmp_path / "empty_root"
    root.mkdir()

    with pytest.raises(ttf.TimeseriesTableError) as excinfo:
        ttf.TimeSeriesTable.open(str(root))

    msg = str(excinfo.value)
    assert str(root) in msg
    assert getattr(excinfo.value, "table_root", None) == str(root)


def test_create_invalid_index_granularity_includes_root(tmp_path):
    root = tmp_path / "table"

    with pytest.raises(ttf.TimeseriesTableError) as excinfo:
        ttf.TimeSeriesTable.create(
            table_root=str(root),
            index_column="ts",
            index_type="timestamp",
            index_granularity="bogus",
            entity_columns=None,
            timezone=None,
        )

    e = excinfo.value
    assert str(root) in str(e)
    assert getattr(e, "table_root", None) == str(root)


def test_open_nonexistent_root_includes_root(tmp_path):
    root = tmp_path / "does_not_exist"

    with pytest.raises(ttf.TimeseriesTableError) as excinfo:
        ttf.TimeSeriesTable.open(str(root))

    e = excinfo.value
    assert str(root) in str(e)
    assert getattr(e, "table_root", None) == str(root)


def test_create_twice_includes_root(tmp_path):
    root = tmp_path / "table"

    ttf.TimeSeriesTable.create(
        table_root=str(root),
        index_column="ts",
        index_type="timestamp",
        index_granularity="1h",
        entity_columns=["symbol"],
        timezone=None,
    )

    with pytest.raises(ttf.TimeseriesTableError) as excinfo:
        ttf.TimeSeriesTable.create(
            table_root=str(root),
            index_column="ts",
            index_type="timestamp",
            index_granularity="1h",
            entity_columns=["symbol"],
            timezone=None,
        )

    e = excinfo.value
    assert str(root) in str(e)
    assert getattr(e, "table_root", None) == str(root)


def test_open_rejects_unsupported_scheme_includes_root():
    root = "s3://bucket/path"

    with pytest.raises(ttf.TimeseriesTableError) as excinfo:
        ttf.TimeSeriesTable.open(root)

    e = excinfo.value
    assert root in str(e)
    assert getattr(e, "table_root", None) == root


def test_create_rejects_unsupported_scheme_includes_root():
    root = "s3://bucket/path"

    with pytest.raises(ttf.TimeseriesTableError) as excinfo:
        ttf.TimeSeriesTable.create(
            table_root=root,
            index_column="ts",
            index_type="timestamp",
            index_granularity="1h",
            entity_columns=["symbol"],
            timezone=None,
        )

    e = excinfo.value
    assert root in str(e)
    assert getattr(e, "table_root", None) == root


@pytest.mark.parametrize(
    ("index_type", "index_granularity", "field"),
    [
        ("timestamp", 1, "index_granularity"),
        ("int64", "1h", "index_granularity"),
        ("uint64", "1h", "index_granularity"),
        ("other", 1, "index_type"),
    ],
)
def test_create_rejects_invalid_index_options_before_io(
    tmp_path, index_type, index_granularity, field
):
    root = tmp_path / "table"

    with pytest.raises(ttf.TimeseriesTableError) as excinfo:
        ttf.TimeSeriesTable.create(
            table_root=str(root),
            index_column="idx",
            index_type=index_type,
            index_granularity=index_granularity,
        )

    error = excinfo.value
    assert field in str(error)
    assert str(root) in str(error)
    assert getattr(error, "table_root", None) == str(root)
    assert getattr(error, "index_type", None) == index_type
    assert not root.exists()


@pytest.mark.parametrize(
    ("index_type", "index_granularity"),
    [
        ("timestamp", None),
        ("timestamp", "1"),
        ("int64", None),
        ("int64", True),
        ("int64", 0),
        ("int64", -1),
        ("int64", 2**64),
        ("int64", 1.5),
        ("int64", "1"),
        ("uint64", False),
        ("uint64", 0),
        ("uint64", -1),
        ("uint64", 2**64),
        ("uint64", 1.5),
        ("uint64", "1"),
    ],
)
def test_create_rejects_invalid_index_granularity_before_io(
    tmp_path, index_type, index_granularity
):
    root = tmp_path / "table"

    with pytest.raises(ttf.TimeseriesTableError, match="index_granularity") as excinfo:
        ttf.TimeSeriesTable.create(
            table_root=str(root),
            index_column="idx",
            index_type=index_type,
            index_granularity=index_granularity,
        )

    required_form = (
        "string using s, m, h, or d units"
        if index_type == "timestamp"
        else "Python int in 1..="
    )
    assert required_form in str(excinfo.value)
    assert not root.exists()


def test_create_requires_index_granularity_before_io(tmp_path):
    root = tmp_path / "table"
    create = cast(Any, ttf.TimeSeriesTable.create)

    with pytest.raises(TypeError, match="index_granularity"):
        create(
            table_root=str(root),
            index_column="idx",
            index_type="int64",
        )

    assert not root.exists()


def test_create_rejects_timezone_for_integer_index_before_io(tmp_path):
    root = tmp_path / "table"

    with pytest.raises(ttf.TimeseriesTableError, match="timezone"):
        ttf.TimeSeriesTable.create(
            table_root=str(root),
            index_column="idx",
            index_type="int64",
            index_granularity=1,
            timezone="UTC",
        )

    assert not root.exists()


@pytest.mark.parametrize("removed_name", ["bucket", "bucket_width"])
def test_create_rejects_removed_granularity_names_before_io(tmp_path, removed_name):
    root = tmp_path / "table"
    create = cast(Any, ttf.TimeSeriesTable.create)

    with pytest.raises(TypeError, match=removed_name):
        create(
            table_root=str(root),
            index_column="idx",
            index_type="timestamp",
            index_granularity="1h",
            **{removed_name: "1h"},
        )

    assert not root.exists()


def test_create_signature_exposes_only_ordered_index_names():
    create = inspect.signature(ttf.TimeSeriesTable.create)
    assert list(create.parameters) == [
        "table_root",
        "index_column",
        "index_type",
        "index_granularity",
        "entity_columns",
        "timezone",
    ]
    assert "time_column" not in create.parameters
