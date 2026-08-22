import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from parquet_helpers import parquet_reader

import timeseries_table_format as ttf


def _write_mixed_segment(path) -> None:
    pq.write_table(
        pa.table(
            {
                "tick": pa.array([0, 10], type=pa.uint64()),
                "device_id": pa.array(["A", "B"], type=pa.string()),
                "reading": pa.array([1, 2], type=pa.int64()),
            }
        ),
        path,
    )


def _create_entity_table(root) -> ttf.TimeSeriesTable:
    return ttf.TimeSeriesTable.create(
        table_root=str(root),
        index_column="tick",
        index_type="uint64",
        bucket_width=10,
        entity_columns=["device_id"],
    )


def test_optimize_returns_read_only_report_and_updates_table(tmp_path):
    root = tmp_path / "table"
    table = _create_entity_table(root)
    source = tmp_path / "mixed.parquet"
    _write_mixed_segment(source)
    assert table.append(parquet_reader(source)) == 2

    report = table.optimize()

    assert isinstance(report, ttf.OptimizeReport)
    assert report.starting_version == 2
    assert report.committed_version == 3
    assert report.candidate_source_segments == 1
    assert report.source_segments_replaced == 1
    assert report.replacement_segments_written == 2
    assert report.distinct_identities_materialized == 2
    assert report.rows_read == 2
    assert report.rows_written == 2
    assert report.no_op is False
    assert table.version() == 3
    with pytest.raises(AttributeError):
        setattr(report, "rows_read", 0)

    no_op = table.optimize()
    assert isinstance(no_op, ttf.OptimizeReport)
    assert no_op.starting_version == 3
    assert no_op.committed_version == 3
    assert no_op.candidate_source_segments == 0
    assert no_op.source_segments_replaced == 0
    assert no_op.replacement_segments_written == 0
    assert no_op.distinct_identities_materialized == 0
    assert no_op.rows_read == 0
    assert no_op.rows_written == 0
    assert no_op.no_op is True
    assert table.version() == 3


def test_optimize_maps_applicability_error_with_table_context(tmp_path):
    root = tmp_path / "table"
    table = ttf.TimeSeriesTable.create(
        table_root=str(root),
        index_column="tick",
        index_type="uint64",
        bucket_width=10,
    )

    with pytest.raises(ttf.TimeseriesTableError, match="no entity columns") as excinfo:
        table.optimize()

    assert getattr(excinfo.value, "table_root") == str(root)
    assert str(root) in str(excinfo.value)
    assert table.version() == 1


def test_optimize_preserves_failed_source_context(tmp_path):
    root = tmp_path / "table"
    table = _create_entity_table(root)
    source = tmp_path / "mixed.parquet"
    _write_mixed_segment(source)
    table.append(parquet_reader(source))
    managed_path = next((root / "data").glob("*.parquet"))
    managed_path.unlink()

    with pytest.raises(ttf.TimeseriesTableError, match=managed_path.name) as excinfo:
        table.optimize()

    assert getattr(excinfo.value, "table_root") == str(root)
    assert table.version() == 2
