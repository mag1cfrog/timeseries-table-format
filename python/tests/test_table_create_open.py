import pytest
import timeseries_table_format as ttf


def test_create_then_open(tmp_path):
    root = tmp_path / "table"

    t1 = ttf.TimeSeriesTable.create(
        table_root=str(root),
        time_column="ts",
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
            time_column="ts",
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
        time_column="ts",
        bucket="1h",
        entity_columns=["symbol"],
        timezone=None,
    )

    with pytest.raises(ttf.TimeseriesTableError) as excinfo:
        ttf.TimeSeriesTable.create(
            table_root=str(root),
            time_column="ts",
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
            time_column="ts",
            bucket="1h",
            entity_columns=["symbol"],
            timezone=None,
        )

    e = excinfo.value
    assert root in str(e)
    assert getattr(e, "table_root", None) == root
