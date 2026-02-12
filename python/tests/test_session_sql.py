import pyarrow as pa

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
