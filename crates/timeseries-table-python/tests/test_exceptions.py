from typing import Protocol
import threading
import time

import pytest

import timeseries_table_format as ttf
import timeseries_table_format._native as native


class _TestingModule(Protocol):
    def _test_sleep_without_gil(self, millis: int) -> None: ...


def test_public_index_interval_exceptions_are_exact():
    for module in (ttf, native):
        assert issubclass(module.IndexIntervalOverlapError, module.TimeseriesTableError)
        assert issubclass(
            module.DuplicateIndexIntervalError, module.TimeseriesTableError
        )
        assert not hasattr(module, "CoverageOverlapError")

    assert "IndexIntervalOverlapError" in ttf.__all__
    assert "DuplicateIndexIntervalError" in ttf.__all__
    assert "CoverageOverlapError" not in ttf.__all__


def test_test_sleep_without_gil_allows_other_threads_to_run():
    testing: _TestingModule | None = getattr(native, "_testing", None)
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
    during_rust = run_counter_while(
        lambda: testing._test_sleep_without_gil(duration_ms)
    )

    # Avoid flaky absolute thresholds: compare against baseline measured on the same machine.
    # If the Rust call doesn't release the GIL, `during_rust` will be near zero relative to baseline.
    assert baseline > 0
    assert during_rust >= max(1, int(baseline * 0.2))
