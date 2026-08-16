import inspect

import pytest
import timeseries_table_format as ttf


def test_create_then_open(tmp_path):
    root = tmp_path / "table"

    t1 = ttf.TimeSeriesTable.create(
        table_root=str(root),
        index_column="ts",
        index_type="timestamp",
        bucket="1h",
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


def test_create_invalid_bucket_includes_root(tmp_path):
    root = tmp_path / "table"

    with pytest.raises(ttf.TimeseriesTableError) as excinfo:
        ttf.TimeSeriesTable.create(
            table_root=str(root),
            index_column="ts",
            index_type="timestamp",
            bucket="bogus",
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
        bucket="1h",
        entity_columns=["symbol"],
        timezone=None,
    )

    with pytest.raises(ttf.TimeseriesTableError) as excinfo:
        ttf.TimeSeriesTable.create(
            table_root=str(root),
            index_column="ts",
            index_type="timestamp",
            bucket="1h",
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
            bucket="1h",
            entity_columns=["symbol"],
            timezone=None,
        )

    e = excinfo.value
    assert root in str(e)
    assert getattr(e, "table_root", None) == root


@pytest.mark.parametrize(
    ("index_type", "options", "field"),
    [
        ("timestamp", {}, "bucket"),
        ("timestamp", {"bucket": "1h", "bucket_width": 1}, "bucket_width"),
        ("int64", {}, "bucket_width"),
        ("int64", {"bucket_width": 1, "bucket": "1h"}, "bucket"),
        ("int64", {"bucket_width": 1, "timezone": "UTC"}, "timezone"),
        ("uint64", {"bucket": "1h"}, "bucket"),
        ("other", {}, "index_type"),
    ],
)
def test_create_rejects_invalid_index_options_before_io(
    tmp_path, index_type, options, field
):
    root = tmp_path / "table"

    with pytest.raises(ttf.TimeseriesTableError) as excinfo:
        ttf.TimeSeriesTable.create(
            table_root=str(root),
            index_column="idx",
            index_type=index_type,
            **options,
        )

    error = excinfo.value
    assert field in str(error)
    assert str(root) in str(error)
    assert getattr(error, "table_root", None) == str(root)
    assert getattr(error, "index_type", None) == index_type
    assert not root.exists()


@pytest.mark.parametrize("bucket_width", [True, False, 0, -1, 2**64, 1.5, "1"])
def test_create_rejects_invalid_python_bucket_width_before_io(tmp_path, bucket_width):
    root = tmp_path / "table"

    with pytest.raises(ttf.TimeseriesTableError, match="bucket_width"):
        ttf.TimeSeriesTable.create(
            table_root=str(root),
            index_column="idx",
            index_type="uint64",
            bucket_width=bucket_width,
        )

    assert not root.exists()


def test_runtime_signatures_expose_only_ordered_index_names():
    create = inspect.signature(ttf.TimeSeriesTable.create)
    assert list(create.parameters) == [
        "table_root",
        "index_column",
        "index_type",
        "entity_columns",
        "bucket",
        "bucket_width",
        "timezone",
    ]
    assert "time_column" not in create.parameters

    append = inspect.signature(ttf.TimeSeriesTable.append_parquet)
    assert list(append.parameters) == ["self", "parquet_path", "copy_if_outside"]
    assert "time_column" not in append.parameters
