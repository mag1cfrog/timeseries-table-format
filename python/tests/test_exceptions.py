from typing import Protocol
import os
import tempfile
import threading
import time

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import timeseries_table_format as ttf
import timeseries_table_format._dev as dev


class _TestingModule(Protocol):
    def _test_trigger_overlap(self, table_root: str, parquet_path: str) -> None: ...
    def _test_sleep_without_gil(self, millis: int) -> None: ...


def test_coverage_overlap_maps_to_specific_exception():
    testing: _TestingModule | None = getattr(dev, "_testing", None)
    if testing is None:
        pytest.skip("Rust extension built without feature 'test-utils'")
        return

    with tempfile.TemporaryDirectory() as tmp:
        table_root = os.path.join(tmp, "table")
        parquet_path = os.path.join(tmp, "seg.parquet")

        ts = pa.array([0, 60 * 60 * 1_000_000], type=pa.timestamp("us"))
        v = pa.array([1, 2], type=pa.int64())
        tbl = pa.table({"ts": ts, "v": v})
        pq.write_table(tbl, parquet_path)

        with pytest.raises(ttf.CoverageOverlapError) as excinfo:
            testing._test_trigger_overlap(table_root, parquet_path)

        e = excinfo.value
        assert isinstance(e.segment_path, str)
        assert isinstance(e.overlap_count, int)
        assert e.overlap_count > 0
        assert e.example_bucket is None or isinstance(e.example_bucket, int)


def test_test_sleep_without_gil_allows_other_threads_to_run():
    testing: _TestingModule | None = getattr(dev, "_testing", None)
    if testing is None:
        pytest.skip("Rust extension built without feature 'test-utils'")
        return

    duration_ms = 200
    duration_s = duration_ms / 1000.0

    # Baseline: how many Python increments we can do in `duration_s` with no other work.
    baseline = 0
    t0 = time.perf_counter()
    while (time.perf_counter() - t0) < duration_s:
        baseline += 1
    assert baseline > 10_000

    entered = threading.Event()
    done = threading.Event()

    def worker():
        entered.set()
        testing._test_sleep_without_gil(duration_ms)
        done.set()

    t = threading.Thread(target=worker)
    t.start()
    assert entered.wait(timeout=1.0)

    # While the other thread is sleeping in Rust, we should still be able to run Python
    # if the binding releases the GIL.
    count = 0
    t0 = time.perf_counter()
    while (time.perf_counter() - t0) < duration_s:
        count += 1

    t.join(timeout=2.0)
    assert done.is_set()
    assert not t.is_alive()

    # If the GIL was held for the whole call, `count` will be near zero vs baseline.
    assert count >= int(baseline * 0.2)
