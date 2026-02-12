import threading
import gc
import timeseries_table_format as ttf


def test_many_session_construct_and_drop():
    # This catches regressions where each Session spawns lots of threads
    # or where dropping SEssion is slow/hangs.
    sessions = [ttf.Session() for _ in range(200)]
    assert len(sessions) == 200

    # Drop them all and force collection/finalizers.
    del sessions
    gc.collect()
    gc.collect()


def test_session_construct_and_drop_in_python_thread():
    err: list[Exception] = []

    def worker() -> None:
        try:
            s = ttf.Session()
            del s
            gc.collect()
            gc.collect()
        except Exception as e:
            err.append(e)

    t = threading.Thread(target=worker)
    t.start()
    t.join(timeout=5.0)

    assert not t.is_alive(), "thread hung (possible runtime drop/shutdown issue)"
    assert not err, err[0]
