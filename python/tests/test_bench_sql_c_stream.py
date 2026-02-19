import pytest
import pyarrow as pa

import timeseries_table_format as ttf
import timeseries_table_format._native as native


def _testing_module():
    testing = getattr(native, "_testing", None)
    if testing is None:
        pytest.skip("Rust extension built without feature 'test-utils'")
    return testing


def test_bench_sql_c_stream_capsule_roundtrip():
    testing = _testing_module()

    sess = ttf.Session()
    capsule, m = testing._bench_sql_c_stream(sess, "select 1 as x")

    class _Wrapper:
        def __init__(self, c: object):
            self._c = c

        def __arrow_c_stream__(self, requested_schema=None) -> object:
            return self._c

    reader = pa.RecordBatchReader.from_stream(_Wrapper(capsule))
    try:
        tbl = reader.read_all()
    finally:
        reader.close()

    assert tbl.column_names == ["x"]
    assert tbl["x"].to_pylist() == [1]

    assert "arrow_mem_bytes" in m
    assert "row_count" in m
    assert "batch_count" in m
    assert "plan_ms" in m
    assert "collect_ms" in m
    assert "c_stream_export_ms" in m
    assert "total_ms" in m
