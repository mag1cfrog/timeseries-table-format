import pyarrow as pa
import pytest

import timeseries_table_format as ttf


def test_session_sql_reader_returns_recordbatchreader():
    sess = ttf.Session()

    reader = sess.sql_reader("select 1 as x union all select 2 as x order by x")
    try:
        assert isinstance(reader, pa.RecordBatchReader)
        table = pa.Table.from_batches(list(reader))
        assert table["x"].to_pylist() == [1, 2]
    finally:
        reader.close()


def test_session_sql_reader_read_all_matches_sql():
    sess = ttf.Session()
    query = "select cast(1 as bigint) as x union all select cast(2 as bigint) as x order by x"

    reader = sess.sql_reader(query)
    try:
        from_reader = reader.read_all()
    finally:
        reader.close()

    from_sql = sess.sql(query)
    assert from_reader.equals(from_sql)


def test_session_sql_reader_params_work():
    sess = ttf.Session()

    reader = sess.sql_reader("select cast($1 as bigint) as x", params=[1])
    try:
        table = reader.read_all()
    finally:
        reader.close()

    assert table["x"].to_pylist() == [1]


def test_session_sql_reader_can_close_early():
    sess = ttf.Session()

    reader = sess.sql_reader("select 1 as x union all select 2 as x")
    reader.close()


def test_session_sql_reader_named_params_work():
    sess = ttf.Session()

    reader = sess.sql_reader("select cast($value as bigint) as x", params={"value": 7})
    try:
        table = reader.read_all()
    finally:
        reader.close()

    assert table["x"].to_pylist() == [7]

def test_session_sql_reader_requires_pyarrow_from_stream(monkeypatch: pytest.MonkeyPatch):
    pa_mod = pytest.importorskip("pyarrow")

    class _FakeRecordBatchReader:
        pass

    monkeypatch.setattr(pa_mod, "RecordBatchReader", _FakeRecordBatchReader)

    sess = ttf.Session()
    with pytest.raises(ImportError, match=r"RecordBatchReader\.from_stream"):
        sess.sql_reader("select 1 as x")
