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
    