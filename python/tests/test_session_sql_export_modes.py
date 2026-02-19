import gc
import threading

import pyarrow.ipc as pa_ipc
import pytest

import timeseries_table_format as ttf


def test_session_sql_ipc_equals_c_stream(monkeypatch: pytest.MonkeyPatch):
    sess = ttf.Session()
    # Ensure stable row ordering across executions.
    q = "select 1 as x union all select 2 as x order by x"

    monkeypatch.setenv("TTF_SQL_EXPORT_MODE", "ipc")
    t_ipc = sess.sql(q)

    monkeypatch.setenv("TTF_SQL_EXPORT_MODE", "c_stream")
    t_cs = sess.sql(q)

    assert t_cs.equals(t_ipc)


def test_session_sql_invalid_export_mode_rejected(monkeypatch: pytest.MonkeyPatch):
    sess = ttf.Session()
    monkeypatch.setenv("TTF_SQL_EXPORT_MODE", "definitely-not-a-real-mode")
    with pytest.raises(ValueError, match="TTF_SQL_EXPORT_MODE"):
        sess.sql("select 1 as x")


def test_session_sql_c_stream_does_not_call_ipc_decode(monkeypatch: pytest.MonkeyPatch):
    sess = ttf.Session()

    def _boom(*_args, **_kwargs):
        raise AssertionError("pyarrow.ipc.open_stream should not be called in c_stream mode")

    monkeypatch.setattr(pa_ipc, "open_stream", _boom)
    monkeypatch.setenv("TTF_SQL_EXPORT_MODE", "c_stream")

    out = sess.sql("select 1 as x")
    assert out["x"].to_pylist() == [1]


def test_session_sql_c_stream_repeated_calls_do_not_crash(monkeypatch: pytest.MonkeyPatch):
    sess = ttf.Session()
    monkeypatch.setenv("TTF_SQL_EXPORT_MODE", "c_stream")

    for _ in range(200):
        out = sess.sql("select 1 as x")
        assert out.num_rows == 1
        del out
        gc.collect()


def test_session_sql_c_stream_concurrent_calls_do_not_crash(monkeypatch: pytest.MonkeyPatch):
    sess = ttf.Session()
    monkeypatch.setenv("TTF_SQL_EXPORT_MODE", "c_stream")

    errors: list[Exception] = []

    def worker() -> None:
        try:
            for _ in range(50):
                out = sess.sql("select 1 as x")
                assert out["x"].to_pylist() == [1]
        except Exception as e:  # pragma: no cover
            errors.append(e)

    threads = [threading.Thread(target=worker) for _ in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10.0)

    assert all(not t.is_alive() for t in threads), "thread hung (possible deadlock)"
    assert not errors, errors[0]


def test_session_sql_auto_falls_back_to_ipc_if_c_stream_import_fails(
    monkeypatch: pytest.MonkeyPatch,
):
    """
    Ensure 'auto' mode falls back to IPC if the C Stream import path fails at runtime.
    This guards against regressions where an internal PyArrow import error would surface
    to the user unnecessarily.
    """
    pa = pytest.importorskip("pyarrow")

    class _FakeRecordBatchReader:
        @staticmethod
        def _import_from_c_capsule(_capsule: object):
            raise RuntimeError("boom")

    # Patch the module attribute (patching the underlying Cython type is not reliable / reversible).
    monkeypatch.setattr(pa, "RecordBatchReader", _FakeRecordBatchReader)

    called = {"open_stream": 0}
    real_open_stream = pa_ipc.open_stream

    def _open_stream(*args, **kwargs):
        called["open_stream"] += 1
        return real_open_stream(*args, **kwargs)

    monkeypatch.setattr(pa_ipc, "open_stream", _open_stream)

    sess = ttf.Session()
    monkeypatch.setenv("TTF_SQL_EXPORT_MODE", "auto")

    out = sess.sql("select 1 as x")
    assert out["x"].to_pylist() == [1]
    assert called["open_stream"] >= 1


def test_session_sql_ipc_does_not_use_c_stream_import(monkeypatch: pytest.MonkeyPatch):
    pa = pytest.importorskip("pyarrow")

    class _FakeRecordBatchReader:
        @staticmethod
        def _import_from_c_capsule(_capsule: object):
            raise AssertionError("C Stream import should not be called in ipc mode")

    monkeypatch.setattr(pa, "RecordBatchReader", _FakeRecordBatchReader)

    sess = ttf.Session()
    monkeypatch.setenv("TTF_SQL_EXPORT_MODE", "ipc")

    out = sess.sql("select 1 as x")
    assert out["x"].to_pylist() == [1]


def test_session_sql_c_stream_does_not_fall_back_to_ipc_when_forced(
    monkeypatch: pytest.MonkeyPatch,
):
    pa = pytest.importorskip("pyarrow")

    class _FakeRecordBatchReader:
        @staticmethod
        def _import_from_c_capsule(_capsule: object):
            raise RuntimeError("boom")

    monkeypatch.setattr(pa, "RecordBatchReader", _FakeRecordBatchReader)

    def _boom(*_args, **_kwargs):
        raise AssertionError("IPC fallback should not be used in forced c_stream mode")

    monkeypatch.setattr(pa_ipc, "open_stream", _boom)

    sess = ttf.Session()
    monkeypatch.setenv("TTF_SQL_EXPORT_MODE", "c_stream")

    with pytest.raises(RuntimeError, match="boom"):
        sess.sql("select 1 as x")
