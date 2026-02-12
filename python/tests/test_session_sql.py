import datetime
import threading
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import timeseries_table_format as ttf


def test_session_sql_select_literal():
    sess = ttf.Session()
    out = sess.sql("select 1 as x")
    assert isinstance(out, pa.Table)
    assert out.column_names == ["x"]
    assert out["x"].to_pylist() == [1]


def test_session_sql_positional_params():
    sess = ttf.Session()
    out = sess.sql("select 1 as x where 1 = $1", params=[1])
    assert out["x"].to_pylist() == [1]


def test_session_sql_named_params():
    sess = ttf.Session()
    out = sess.sql("select 1 as x where 1 = $a", params={"a": 1})
    assert out["x"].to_pylist() == [1]


def test_session_sql_params_kw_only_enforced():
    sess = ttf.Session()
    with pytest.raises(TypeError):
        sql: Any = sess.sql
        sql("select 1 as x", [1])  # type: ignore[too-many-positional-arguments]


@pytest.mark.parametrize("params", [1, {1}, object()])
def test_session_sql_bad_params_container_type(params):
    sess = ttf.Session()
    with pytest.raises(TypeError):
        sess.sql("select 1 as x", params=params)


def test_session_sql_params_dict_key_must_be_str():
    sess = ttf.Session()
    with pytest.raises(TypeError):
        sess.sql("select 1 as x where 1 = $a", params={1: 1})


@pytest.mark.parametrize(
    "params",
    [
        [{"x": 1}],
        [pa.scalar(1)],
        [datetime.datetime.now()],
    ],
)
def test_session_sql_bad_params_value_type(params):
    sess = ttf.Session()
    with pytest.raises(TypeError):
        sess.sql("select $1 as x", params=params)


def test_session_sql_big_int_overflow_rejected():
    sess = ttf.Session()
    with pytest.raises(ValueError):
        sess.sql("select $1 as x", params=[2**200])


def test_session_sql_placeholder_mismatch_positional_empty():
    sess = ttf.Session()
    with pytest.raises(ttf.DataFusionError):
        sess.sql("select 1 as x where 1 = $1", params=[])


def test_session_sql_placeholder_mismatch_positional_wrong_arity():
    sess = ttf.Session()
    with pytest.raises(ttf.DataFusionError):
        sess.sql("select 1 as x where 1 = $2", params=[1])


def test_session_sql_placeholder_mismatch_named_missing_key():
    sess = ttf.Session()
    with pytest.raises(ttf.DataFusionError):
        sess.sql("select 1 as x where 1 = $a", params={"b": 1})


def test_session_sql_params_in_projection_positional():
    sess = ttf.Session()
    # Note: without an explicit cast, DataFusion may type placeholder projections as NULL.
    out = sess.sql("select cast($1 as bigint) as x", params=[1])
    assert out.column_names == ["x"]
    assert out["x"].to_pylist() == [1]


def test_session_sql_params_in_projection_named():
    sess = ttf.Session()
    out = sess.sql("select cast($a as varchar) as x", params={"a": "hi"})
    assert out.column_names == ["x"]
    assert out["x"].to_pylist() == ["hi"]


def test_session_sql_named_params_accepts_leading_dollar_in_dict_key():
    sess = ttf.Session()
    out = sess.sql("select cast($a as varchar) as x", params={"$a": "hi"})
    assert out["x"].to_pylist() == ["hi"]


def test_session_sql_empty_result_set_still_has_schema():
    sess = ttf.Session()
    out = sess.sql("select 1 as x where false")
    assert out.column_names == ["x"]
    assert out.num_rows == 0


def test_session_sql_invalid_sql_maps_to_datafusion_error():
    sess = ttf.Session()
    with pytest.raises(ttf.DataFusionError):
        sess.sql("selectt 1")


def test_session_sql_missing_table_maps_to_datafusion_error():
    sess = ttf.Session()
    with pytest.raises(ttf.DataFusionError):
        sess.sql("select * from does_not_exist")


def test_session_sql_concurrent_calls_do_not_deadlock_or_crash():
    sess = ttf.Session()
    errors: list[BaseException] = []

    def worker() -> None:
        try:
            for _ in range(20):
                out = sess.sql("select 1 as x")
                assert out["x"].to_pylist() == [1]
        except BaseException as e:
            errors.append(e)

    threads = [threading.Thread(target=worker) for _ in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10.0)

    assert all(not t.is_alive() for t in threads), "thread hung (possible deadlock)"
    assert not errors, errors[0]


def test_session_register_and_sql_concurrently_stays_consistent(tmp_path):
    tbl = pa.table(
        {
            "symbol": pa.array(["NVDA", "AAPL"], type=pa.string()),
            "exchange": pa.array(["NASDAQ", "NASDAQ"], type=pa.string()),
        }
    )
    dim_path = tmp_path / "dim.parquet"
    pq.write_table(tbl, str(dim_path))

    sess = ttf.Session()
    sess.register_parquet("dim", str(dim_path))

    errors: list[BaseException] = []
    start = threading.Event()

    def registrar() -> None:
        try:
            start.wait(timeout=1.0)
            for _ in range(20):
                sess.register_parquet("dim", str(dim_path))
        except BaseException as e:
            errors.append(e)

    def querier() -> None:
        try:
            start.set()
            for _ in range(50):
                out = sess.sql("select count(*) as n from dim")
                assert out["n"].to_pylist() == [2]
        except BaseException as e:
            errors.append(e)

    t1 = threading.Thread(target=registrar)
    t2 = threading.Thread(target=querier)
    t1.start()
    t2.start()
    t1.join(timeout=10.0)
    t2.join(timeout=10.0)

    assert not t1.is_alive() and not t2.is_alive(), "thread hung (possible deadlock)"
    assert not errors, errors[0]
