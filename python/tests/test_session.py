import threading
import gc
import timeseries_table_format as ttf


def test_session_constructs():
    s = ttf.Session()
    assert s is not None


def test_session_constructs_many_times():
    for _ in range(50):
        s = ttf.Session()
        assert s is not None


def test_session_drop_does_not_hang_or_crash():
    s = ttf.Session()
    del s
    gc.collect()
    gc.collect()


def test_session_construct_in_python_thread():
    err = []

    def worker():
        try:
            s = ttf.Session()
            del s
            gc.collect()
        except Exception as e:
            err.append(e)

    t = threading.Thread(target=worker)
    t.start()
    t.join(timeout=5.0)
    assert not t.is_alive(), "thread hung (possible runtime drop issue)"
    assert not err, err[0]
