import gc
import threading

import pyarrow as pa
import pyarrow.ipc as pa_ipc
import pyarrow.parquet as pq
import pytest

import timeseries_table_format as ttf


def test_session_sql_default_mode_is_c_stream(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("TTF_SQL_EXPORT_MODE", raising=False)
    sess = ttf.Session()
    out = sess.sql("select 1 as x")
    assert out["x"].to_pylist() == [1]


def test_session_sql_ipc_equals_c_stream(monkeypatch: pytest.MonkeyPatch):
    sess = ttf.Session()
    # Ensure stable row ordering across executions.
    q = "select 1 as x union all select 2 as x order by x"

    monkeypatch.setenv("TTF_SQL_EXPORT_MODE", "ipc")
    t_ipc = sess.sql(q)

    monkeypatch.setenv("TTF_SQL_EXPORT_MODE", "c_stream")
    t_cs = sess.sql(q)

    assert t_cs.equals(t_ipc)


def test_session_sql_empty_result_schema_matches_between_modes(
    monkeypatch: pytest.MonkeyPatch,
):
    sess = ttf.Session()
    q = "select cast(1 as bigint) as x where false"

    monkeypatch.setenv("TTF_SQL_EXPORT_MODE", "ipc")
    t_ipc = sess.sql(q)

    monkeypatch.setenv("TTF_SQL_EXPORT_MODE", "c_stream")
    t_cs = sess.sql(q)

    assert t_ipc.num_rows == 0
    assert t_cs.num_rows == 0
    assert t_cs.schema == t_ipc.schema


def test_session_sql_multi_column_types_match_between_modes(
    monkeypatch: pytest.MonkeyPatch,
):
    sess = ttf.Session()
    q = "select cast(1 as bigint) as a, cast(1.5 as double) as b, cast('x' as varchar) as c"

    monkeypatch.setenv("TTF_SQL_EXPORT_MODE", "ipc")
    t_ipc = sess.sql(q)

    monkeypatch.setenv("TTF_SQL_EXPORT_MODE", "c_stream")
    t_cs = sess.sql(q)

    assert t_cs.equals(t_ipc)


def test_session_sql_params_work_in_c_stream_mode(monkeypatch: pytest.MonkeyPatch):
    sess = ttf.Session()
    monkeypatch.setenv("TTF_SQL_EXPORT_MODE", "c_stream")

    out = sess.sql("select cast($1 as bigint) as x", params=[1])
    assert out["x"].to_pylist() == [1]


def test_session_sql_invalid_export_mode_rejected(monkeypatch: pytest.MonkeyPatch):
    sess = ttf.Session()
    monkeypatch.setenv("TTF_SQL_EXPORT_MODE", "definitely-not-a-real-mode")
    with pytest.raises(ValueError, match="TTF_SQL_EXPORT_MODE"):
        sess.sql("select 1 as x")


def test_session_sql_c_stream_does_not_call_ipc_decode(monkeypatch: pytest.MonkeyPatch):
    sess = ttf.Session()

    def _boom(*_args, **_kwargs):
        raise AssertionError(
            "pyarrow.ipc.open_stream should not be called in c_stream mode"
        )

    monkeypatch.setattr(pa_ipc, "open_stream", _boom)
    monkeypatch.setenv("TTF_SQL_EXPORT_MODE", "c_stream")

    out = sess.sql("select 1 as x")
    assert out["x"].to_pylist() == [1]


def test_session_sql_c_stream_repeated_calls_do_not_crash(
    monkeypatch: pytest.MonkeyPatch,
):
    sess = ttf.Session()
    monkeypatch.setenv("TTF_SQL_EXPORT_MODE", "c_stream")

    for _ in range(200):
        out = sess.sql("select 1 as x")
        assert out.num_rows == 1
        del out
        gc.collect()


def test_session_sql_c_stream_concurrent_calls_do_not_crash(
    monkeypatch: pytest.MonkeyPatch,
):
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


def test_session_sql_auto_rerun_fallback_env_var_still_falls_back_to_ipc(
    monkeypatch: pytest.MonkeyPatch,
):
    pa = pytest.importorskip("pyarrow")

    class _FakeRecordBatchReader:
        @staticmethod
        def _import_from_c_capsule(_capsule: object):
            raise RuntimeError("boom")

    monkeypatch.setattr(pa, "RecordBatchReader", _FakeRecordBatchReader)

    called = {"open_stream": 0}
    real_open_stream = pa_ipc.open_stream

    def _open_stream(*args, **kwargs):
        called["open_stream"] += 1
        return real_open_stream(*args, **kwargs)

    monkeypatch.setattr(pa_ipc, "open_stream", _open_stream)

    sess = ttf.Session()
    monkeypatch.setenv("TTF_SQL_EXPORT_MODE", "auto")
    monkeypatch.setenv("TTF_SQL_EXPORT_AUTO_RERUN_FALLBACK", "1")

    out = sess.sql("select 1 as x")
    assert out["x"].to_pylist() == [1]
    assert called["open_stream"] >= 1


def test_session_sql_auto_falls_back_to_ipc_if_c_stream_read_all_fails(
    monkeypatch: pytest.MonkeyPatch,
):
    pa_mod = pytest.importorskip("pyarrow")

    class _FakeReader:
        def read_all(self):
            raise RuntimeError("read_all boom")

        def close(self):
            return None

    class _FakeRecordBatchReader:
        @staticmethod
        def _import_from_c_capsule(_capsule: object):
            return _FakeReader()

    monkeypatch.setattr(pa_mod, "RecordBatchReader", _FakeRecordBatchReader)

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


def test_session_sql_auto_returns_table_if_c_stream_close_fails(
    monkeypatch: pytest.MonkeyPatch,
):
    pa_mod = pytest.importorskip("pyarrow")

    class _FakeReader:
        def read_all(self):
            # Return a valid table so close failure is the only error.
            return pa.table({"x": [1]})

        def close(self):
            raise RuntimeError("close boom")

    class _FakeRecordBatchReader:
        @staticmethod
        def _import_from_c_capsule(_capsule: object):
            return _FakeReader()

    monkeypatch.setattr(pa_mod, "RecordBatchReader", _FakeRecordBatchReader)

    def _boom(*_args, **_kwargs):
        raise AssertionError(
            "IPC fallback should not be used when read_all() succeeded"
        )

    monkeypatch.setattr(pa_ipc, "open_stream", _boom)

    sess = ttf.Session()
    monkeypatch.setenv("TTF_SQL_EXPORT_MODE", "auto")

    out = sess.sql("select 1 as x")
    assert out["x"].to_pylist() == [1]


def test_session_sql_auto_falls_back_to_ipc_if_c_stream_import_method_missing(
    monkeypatch: pytest.MonkeyPatch,
):
    pa_mod = pytest.importorskip("pyarrow")

    class _FakeRecordBatchReader:
        pass

    monkeypatch.setattr(pa_mod, "RecordBatchReader", _FakeRecordBatchReader)

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
    pa_mod = pytest.importorskip("pyarrow")

    class _FakeRecordBatchReader:
        @staticmethod
        def _import_from_c_capsule(_capsule: object):
            raise AssertionError("C Stream import should not be called in ipc mode")

    monkeypatch.setattr(pa_mod, "RecordBatchReader", _FakeRecordBatchReader)

    sess = ttf.Session()
    monkeypatch.setenv("TTF_SQL_EXPORT_MODE", "ipc")

    out = sess.sql("select 1 as x")
    assert out["x"].to_pylist() == [1]


def test_session_sql_auto_raises_if_both_c_stream_and_ipc_decode_fail(
    monkeypatch: pytest.MonkeyPatch,
):
    pa_mod = pytest.importorskip("pyarrow")

    class _FakeRecordBatchReader:
        @staticmethod
        def _import_from_c_capsule(_capsule: object):
            raise RuntimeError("c stream import boom")

    monkeypatch.setattr(pa_mod, "RecordBatchReader", _FakeRecordBatchReader)

    def _boom(*_args, **_kwargs):
        raise RuntimeError("ipc open_stream boom")

    monkeypatch.setattr(pa_ipc, "open_stream", _boom)

    sess = ttf.Session()
    monkeypatch.setenv("TTF_SQL_EXPORT_MODE", "auto")

    with pytest.raises(RuntimeError, match="ipc open_stream boom"):
        sess.sql("select 1 as x")


def test_session_sql_c_stream_does_not_fall_back_to_ipc_when_forced(
    monkeypatch: pytest.MonkeyPatch,
):
    pa_mod = pytest.importorskip("pyarrow")

    class _FakeRecordBatchReader:
        @staticmethod
        def _import_from_c_capsule(_capsule: object):
            raise RuntimeError("boom")

    monkeypatch.setattr(pa_mod, "RecordBatchReader", _FakeRecordBatchReader)

    def _boom(*_args, **_kwargs):
        raise AssertionError("IPC fallback should not be used in forced c_stream mode")

    monkeypatch.setattr(pa_ipc, "open_stream", _boom)

    sess = ttf.Session()
    monkeypatch.setenv("TTF_SQL_EXPORT_MODE", "c_stream")

    with pytest.raises(RuntimeError, match="boom"):
        sess.sql("select 1 as x")


def test_session_sql_c_stream_propagates_reader_read_all_failures_when_forced(
    monkeypatch: pytest.MonkeyPatch,
):
    pa_mod = pytest.importorskip("pyarrow")

    class _FakeReader:
        def read_all(self):
            raise RuntimeError("read_all boom")

        def close(self):
            return None

    class _FakeRecordBatchReader:
        @staticmethod
        def _import_from_c_capsule(_capsule: object):
            return _FakeReader()

    monkeypatch.setattr(pa_mod, "RecordBatchReader", _FakeRecordBatchReader)

    def _boom(*_args, **_kwargs):
        raise AssertionError("IPC fallback should not be used in forced c_stream mode")

    monkeypatch.setattr(pa_ipc, "open_stream", _boom)

    sess = ttf.Session()
    monkeypatch.setenv("TTF_SQL_EXPORT_MODE", "c_stream")

    with pytest.raises(RuntimeError, match="read_all boom"):
        sess.sql("select 1 as x")


def test_session_sql_c_stream_read_all_and_close_both_fail_adds_note(
    monkeypatch: pytest.MonkeyPatch,
):
    pa_mod = pytest.importorskip("pyarrow")

    class _FakeReader:
        def read_all(self):
            raise RuntimeError("read_all boom")

        def close(self):
            raise RuntimeError("close boom")

    class _FakeRecordBatchReader:
        @staticmethod
        def _import_from_c_capsule(_capsule: object):
            return _FakeReader()

    monkeypatch.setattr(pa_mod, "RecordBatchReader", _FakeRecordBatchReader)

    def _boom(*_args, **_kwargs):
        raise AssertionError("IPC fallback should not be used in forced c_stream mode")

    monkeypatch.setattr(pa_ipc, "open_stream", _boom)

    sess = ttf.Session()
    monkeypatch.setenv("TTF_SQL_EXPORT_MODE", "c_stream")

    with pytest.raises(RuntimeError, match="read_all boom") as excinfo:
        sess.sql("select 1 as x")

    # Python 3.11+ has `BaseException.__notes__` for exception notes.
    notes = getattr(excinfo.value, "__notes__", None)
    if notes is not None:
        assert any("close boom" in str(n) for n in notes)


def test_session_sql_export_mode_switches_per_call(monkeypatch: pytest.MonkeyPatch):
    sess = ttf.Session()

    # IPC call should use IPC decode.
    called = {"open_stream": 0}
    real_open_stream = pa_ipc.open_stream

    def _counting_open_stream(*args, **kwargs):
        called["open_stream"] += 1
        return real_open_stream(*args, **kwargs)

    monkeypatch.setattr(pa_ipc, "open_stream", _counting_open_stream)

    monkeypatch.setenv("TTF_SQL_EXPORT_MODE", "ipc")
    out = sess.sql("select 1 as x")
    assert out["x"].to_pylist() == [1]
    assert called["open_stream"] >= 1

    # C Stream call should not use IPC decode.
    def _boom(*_args, **_kwargs):
        raise AssertionError(
            "pyarrow.ipc.open_stream should not be called in c_stream mode"
        )

    monkeypatch.setattr(pa_ipc, "open_stream", _boom)
    monkeypatch.setenv("TTF_SQL_EXPORT_MODE", "c_stream")
    out = sess.sql("select 1 as x")
    assert out["x"].to_pylist() == [1]


def test_session_sql_c_stream_missing_import_method_errors_when_forced(
    monkeypatch: pytest.MonkeyPatch,
):
    pa_mod = pytest.importorskip("pyarrow")

    class _FakeRecordBatchReader:
        pass

    monkeypatch.setattr(pa_mod, "RecordBatchReader", _FakeRecordBatchReader)
    monkeypatch.setenv("TTF_SQL_EXPORT_MODE", "c_stream")

    sess = ttf.Session()
    with pytest.raises(AttributeError):
        sess.sql("select 1 as x")


def test_session_sql_returns_pyarrow_table_in_both_modes(
    monkeypatch: pytest.MonkeyPatch,
):
    sess = ttf.Session()

    monkeypatch.setenv("TTF_SQL_EXPORT_MODE", "ipc")
    out = sess.sql("select 1 as x")
    assert isinstance(out, pa.Table)

    monkeypatch.setenv("TTF_SQL_EXPORT_MODE", "c_stream")
    out = sess.sql("select 1 as x")
    assert isinstance(out, pa.Table)


def test_session_sql_list_roundtrips_via_c_stream(
    monkeypatch: pytest.MonkeyPatch, tmp_path
):
    tbl = pa.table({"x": pa.array([[1, 2], [3], None], type=pa.list_(pa.int64()))})
    p = tmp_path / "list.parquet"
    pq.write_table(tbl, p)

    sess = ttf.Session()
    sess.register_parquet("t", str(p))

    monkeypatch.setenv("TTF_SQL_EXPORT_MODE", "ipc")
    t_ipc = sess.sql("select x from t")

    monkeypatch.setenv("TTF_SQL_EXPORT_MODE", "c_stream")
    t_cs = sess.sql("select x from t")

    assert t_cs.equals(t_ipc)


def test_session_sql_struct_roundtrips_via_c_stream(
    monkeypatch: pytest.MonkeyPatch, tmp_path
):
    struct_ty = pa.struct([("a", pa.int64()), ("b", pa.string())])
    tbl = pa.table(
        {"s": pa.array([{"a": 1, "b": "x"}, {"a": 2, "b": None}, None], type=struct_ty)}
    )
    p = tmp_path / "struct.parquet"
    pq.write_table(tbl, p)

    sess = ttf.Session()
    sess.register_parquet("t", str(p))

    monkeypatch.setenv("TTF_SQL_EXPORT_MODE", "ipc")
    t_ipc = sess.sql("select s from t")

    monkeypatch.setenv("TTF_SQL_EXPORT_MODE", "c_stream")
    t_cs = sess.sql("select s from t")

    assert t_cs.equals(t_ipc)


def test_session_sql_map_roundtrips_via_c_stream(
    monkeypatch: pytest.MonkeyPatch, tmp_path
):
    map_ty = pa.map_(pa.string(), pa.int64())
    tbl = pa.table({"m": pa.array([{"k1": 1, "k2": 2}, None, {}], type=map_ty)})
    p = tmp_path / "map.parquet"
    pq.write_table(tbl, p)

    sess = ttf.Session()
    sess.register_parquet("t", str(p))

    monkeypatch.setenv("TTF_SQL_EXPORT_MODE", "ipc")
    t_ipc = sess.sql("select m from t")

    monkeypatch.setenv("TTF_SQL_EXPORT_MODE", "c_stream")
    t_cs = sess.sql("select m from t")

    assert t_cs.equals(t_ipc)
