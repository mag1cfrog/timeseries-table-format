import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from parquet_helpers import parquet_reader

import timeseries_table_format as ttf


def _write_segment(path, rows: list[tuple[int, int, str, int]]) -> None:
    ticks, fleet_ids, devices, readings = zip(*rows, strict=True)
    pq.write_table(
        pa.table(
            {
                "tick": pa.array(ticks, type=pa.uint64()),
                "fleet_id": pa.array(fleet_ids, type=pa.int32()),
                "device_id": pa.array(devices, type=pa.string()),
                "reading": pa.array(readings, type=pa.int64()),
            }
        ),
        path,
    )


def _query_results(root) -> tuple[dict, dict, dict]:
    session = ttf.Session()
    session.register_tstable("frames", str(root))
    all_rows = session.sql(
        "select fleet_id, device_id, tick, reading from frames "
        "order by fleet_id, device_id, tick"
    )
    device_a_rows = session.sql(
        "select tick, reading from frames "
        "where fleet_id = -1 and device_id = 'A' and tick >= 20 order by tick"
    )
    grouped = session.sql(
        "select fleet_id, device_id, count(*) as n, sum(reading) as total "
        "from frames group by fleet_id, device_id order by fleet_id, device_id"
    )
    return all_rows.to_pydict(), device_a_rows.to_pydict(), grouped.to_pydict()


def test_multi_entity_optimize_preserves_queries_across_reopen(tmp_path):
    root = tmp_path / "frames"
    table = ttf.TimeSeriesTable.create(
        table_root=str(root),
        index_column="tick",
        index_type="uint64",
        index_granularity=10,
        entity_columns=["fleet_id", "device_id"],
    )
    device_a = tmp_path / "device-a.parquet"
    device_b = tmp_path / "device-b.parquet"
    mixed = tmp_path / "mixed.parquet"
    duplicate = tmp_path / "duplicate.parquet"
    _write_segment(device_a, [(0, -1, "A", 1), (10, -1, "A", 2)])
    _write_segment(device_b, [(0, 7, "B", 3), (10, 7, "B", 4)])
    _write_segment(
        mixed,
        [(20, -1, "A", 5), (20, 7, "B", 6), (30, -1, "A", 7), (30, 7, "B", 8)],
    )
    _write_segment(
        duplicate,
        [(5, -1, "A", 9), (15, 7, "B", 10), (0, 99, "C", 11)],
    )

    table.append(parquet_reader(device_a))
    table.append(parquet_reader(device_b))
    table.append(parquet_reader(mixed))
    version = table.version()
    assert table.index_spec()["entity_columns"] == ["fleet_id", "device_id"]

    with pytest.raises(ttf.CoverageOverlapError) as excinfo:
        table.append(parquet_reader(duplicate))

    error = excinfo.value
    assert error.segment_path.startswith("data/")
    assert error.segment_path.endswith(".parquet")
    assert error.overlap_count == 2
    assert error.example_entity_identity == {"fleet_id": -1, "device_id": "A"}
    assert error.example_bucket == 0
    assert table.version() == version

    expected = (
        {
            "fleet_id": [-1, -1, -1, -1, 7, 7, 7, 7],
            "device_id": ["A", "A", "A", "A", "B", "B", "B", "B"],
            "tick": [0, 10, 20, 30, 0, 10, 20, 30],
            "reading": [1, 2, 5, 7, 3, 4, 6, 8],
        },
        {"tick": [20, 30], "reading": [5, 7]},
        {
            "fleet_id": [-1, 7],
            "device_id": ["A", "B"],
            "n": [4, 4],
            "total": [15, 21],
        },
    )
    before = _query_results(root)
    assert before == expected

    report = table.optimize()
    assert report.starting_version == 4
    assert report.committed_version == 5
    assert report.candidate_source_segments == 1
    assert report.source_segments_replaced == 1
    assert report.replacement_segments_written == 2
    assert report.distinct_identities_materialized == 2
    assert report.rows_read == 4
    assert report.rows_written == 4
    assert report.no_op is False

    del table
    reopened = ttf.TimeSeriesTable.open(str(root))
    assert reopened.version() == 5
    assert reopened.index_spec()["entity_columns"] == ["fleet_id", "device_id"]
    assert _query_results(root) == before

    no_op = reopened.optimize()
    assert no_op.starting_version == 5
    assert no_op.committed_version == 5
    assert no_op.candidate_source_segments == 0
    assert no_op.source_segments_replaced == 0
    assert no_op.replacement_segments_written == 0
    assert no_op.distinct_identities_materialized == 0
    assert no_op.rows_read == 0
    assert no_op.rows_written == 0
    assert no_op.no_op is True
    assert reopened.version() == 5
