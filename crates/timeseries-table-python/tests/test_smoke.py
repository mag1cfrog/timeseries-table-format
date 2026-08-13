import timeseries_table_format as ttf


def test_smoke_import_has_expected_symbols():
    assert isinstance(ttf.__version__, str)
    assert ttf.Session is not None
    assert ttf.TimeSeriesTable is not None


def test_smoke_constructs_session_and_table(tmp_path):
    sess = ttf.Session()
    assert sess is not None

    root = tmp_path / "table"
    tbl = ttf.TimeSeriesTable.create(
        table_root=str(root),
        time_column="ts",
        bucket="1h",
        entity_columns=["symbol"],
        timezone=None,
    )
    assert tbl is not None
