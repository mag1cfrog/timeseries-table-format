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

    duration_ms = 500
    duration_s = duration_ms / 1000.0

    def run_counter_while(fn) -> int:
        ready = threading.Event()
        stop = threading.Event()
        counter = [0]

        def counter_thread():
            ready.set()
            while not stop.is_set():
                counter[0] += 1

        t = threading.Thread(target=counter_thread)
        t.start()
        assert ready.wait(timeout=1.0)

        fn()
        stop.set()
        t.join(timeout=2.0)
        assert not t.is_alive()
        return counter[0]

    # Baseline: other thread runs while we do a pure-Python sleep (releases GIL).
    baseline = run_counter_while(lambda: time.sleep(duration_s))

    # Experiment: other thread should still run while Rust blocks if the binding releases the GIL.
    during_rust = run_counter_while(lambda: testing._test_sleep_without_gil(duration_ms))

    # Avoid flaky absolute thresholds: compare against baseline measured on the same machine.
    # If the Rust call doesn't release the GIL, `during_rust` will be near zero relative to baseline.
    assert baseline > 0
    assert during_rust >= max(1, int(baseline * 0.2))
