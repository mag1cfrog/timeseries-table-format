import os
import tempfile

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import timeseries_table_format as ttf


def test_coverage_overlap_maps_to_specific_exception():
    with tempfile.TemporaryDirectory() as tmp:
        table_root = os.path.join(tmp, "table")
        parquet_path = os.path.join(tmp, "seg.parquet")

        ts = pa.array([0, 60 * 60 * 1_000_000], type=pa.timestamp("us"))
        v = pa.array([1, 2], type=pa.int64())
        tbl = pa.table({"ts": ts, "v": v})
        pq.write_table(tbl, parquet_path)

        with pytest.raises(ttf.CoverageOverlapError) as excinfo:
            ttf._test_trigger_overlap(table_root, parquet_path)

            e = excinfo.value
            assert isinstance(e.segment_path, str)
            assert isinstance(e.overlap_count, int)
            assert e.overlap_count > 0
            assert e.example_bucket is None or isinstance(e.example_bucket, int)
