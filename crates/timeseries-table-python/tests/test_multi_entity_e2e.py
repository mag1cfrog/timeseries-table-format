import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import timeseries_table_format as ttf


def _write_segment(path, rows: list[tuple[int, str, int]]) -> None:
    ticks, devices, readings = zip(*rows, strict=True)
    pq.write_table(
        pa.table(
            {
                "tick": pa.array(ticks, type=pa.uint64()),
                "device_id": pa.array(devices, type=pa.string()),
                "reading": pa.array(readings, type=pa.int64()),
            }
        ),
        path,
    )


def test_multi_entity_append_reopen_and_sql(tmp_path):
    root = tmp_path / "frames"
    table = ttf.TimeSeriesTable.create(
        table_root=str(root),
        index_column="tick",
        index_type="uint64",
        bucket_width=10,
        entity_columns=["device_id"],
    )
    device_a = tmp_path / "device-a.parquet"
    device_b = tmp_path / "device-b.parquet"
    mixed = tmp_path / "mixed.parquet"
    duplicate = tmp_path / "duplicate.parquet"
    _write_segment(device_a, [(0, "A", 1), (10, "A", 2)])
    _write_segment(device_b, [(0, "B", 3), (10, "B", 4)])
    _write_segment(mixed, [(20, "A", 5), (20, "B", 6), (30, "A", 7), (30, "B", 8)])
    _write_segment(duplicate, [(5, "A", 9), (15, "B", 10), (0, "C", 11)])

    table.append_parquet(str(device_a))
    table.append_parquet(str(device_b))
    table.append_parquet(str(mixed))
    version = table.version()
    assert table.index_spec()["entity_columns"] == ["device_id"]

    with pytest.raises(ttf.CoverageOverlapError) as excinfo:
        table.append_parquet(str(duplicate))

    error = excinfo.value
    assert error.segment_path == "data/duplicate.parquet"
    assert error.overlap_count == 2
    assert error.example_entity_identity == {"device_id": "A"}
    assert error.example_bucket == 0
    assert table.version() == version

    del table
    reopened = ttf.TimeSeriesTable.open(str(root))
    assert reopened.version() == version
    assert reopened.index_spec()["entity_columns"] == ["device_id"]

    session = ttf.Session()
    session.register_tstable("frames", str(root))
    all_rows = session.sql(
        "select device_id, tick, reading from frames order by device_id, tick"
    )
    assert all_rows.to_pydict() == {
        "device_id": ["A", "A", "A", "A", "B", "B", "B", "B"],
        "tick": [0, 10, 20, 30, 0, 10, 20, 30],
        "reading": [1, 2, 5, 7, 3, 4, 6, 8],
    }

    device_a_rows = session.sql(
        "select tick, reading from frames "
        "where device_id = 'A' and tick >= 20 order by tick"
    )
    assert device_a_rows.to_pydict() == {
        "tick": [20, 30],
        "reading": [5, 7],
    }

    grouped = session.sql(
        "select device_id, count(*) as n, sum(reading) as total "
        "from frames group by device_id order by device_id"
    )
    assert grouped.to_pydict() == {
        "device_id": ["A", "B"],
        "n": [4, 4],
        "total": [15, 21],
    }
