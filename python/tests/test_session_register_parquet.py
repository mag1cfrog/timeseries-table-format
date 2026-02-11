import threading
import pytest
import pyarrow as pa
import pyarrow.parquet as pq

import timeseries_table_format as ttf
import timeseries_table_format._native as native


def _write_dim_parquet(path: str) -> None:
    tbl = pa.table(
        {
            "symbol": pa.array(["NVDA", "AAPL"], type=pa.string()),
            "exchange": pa.array(["NASDAQ", "NASDAQ"], type=pa.string()),
        }
    )
    pq.write_table(tbl, path)


def _testing_module():
    testing = getattr(native, "_testing", None)
    if testing is None:
        pytest.skip("Rust extension built without feature 'test-utils'")
    return testing


def test_register_parquet_file_succeeds_and_replaces(tmp_path):
    p = tmp_path / "dim.parquet"
    _write_dim_parquet(str(p))

    sess = ttf.Session()
    sess.register_parquet("dim", str(p))
    sess.register_parquet("dim", str(p))  # replace existing registration


def test_register_parquet_directory_succeeds(tmp_path):
    d = tmp_path / "dim_dir"
    d.mkdir()
    p = d / "part-0.parquet"
    _write_dim_parquet(str(p))

    sess = ttf.Session()
    sess.register_parquet("dim", str(d))


def test_register_parquet_empty_name_rejected(tmp_path):
    p = tmp_path / "dim.parquet"
    _write_dim_parquet(str(p))

    sess = ttf.Session()
    with pytest.raises(ValueError):
        sess.register_parquet("", str(p))


def test_register_parquet_missing_path_raises_with_path_context(tmp_path):
    # Use an invalid extension to force DataFusion to reject registration at register time
    # (missing files can be accepted by DataFusion and only fail at query time).
    missing = tmp_path / "missing.txt"

    sess = ttf.Session()
    with pytest.raises(ttf.DataFusionError) as excinfo:
        sess.register_parquet("dim", str(missing))

    e = excinfo.value
    assert str(missing) in str(e)
    assert getattr(e, "name", None) == "dim"
    assert getattr(e, "path", None) == str(missing)


def test_register_parquet_concurrent_replace_does_not_crash(tmp_path):
    p = tmp_path / "dim.parquet"
    _write_dim_parquet(str(p))

    sess = ttf.Session()
    errors: list[Exception] = []

    def w():
        try:
            sess.register_parquet("dim", str(p))
        except Exception as e:
            errors.append(e)

    a = threading.Thread(target=w)
    b = threading.Thread(target=w)
    a.start()
    b.start()
    a.join(timeout=5.0)
    b.join(timeout=5.0)

    assert not a.is_alive() and not b.is_alive()
    assert not errors, errors[0]


def test_register_parquet_failed_replace_restores_previous_registration(tmp_path):
    testing = _testing_module()

    p = tmp_path / "dim.parquet"
    _write_dim_parquet(str(p))

    sess = ttf.Session()
    sess.register_parquet("dim", str(p))
    assert testing._test_session_table_exists(sess, "dim") is True

    missing = tmp_path / "bad.txt"
    with pytest.raises(ttf.DataFusionError):
        sess.register_parquet("dim", str(missing))

    # Should rollback to the previous provider.
    assert testing._test_session_table_exists(sess, "dim") is True


def test_register_parquet_missing_path_does_not_register_when_none_existed(tmp_path):
    testing = _testing_module()

    sess = ttf.Session()
    missing = tmp_path / "bad.txt"

    with pytest.raises(ttf.DataFusionError):
        sess.register_parquet("dim", str(missing))

    assert testing._test_session_table_exists(sess, "dim") is False


def test_register_parquet_value_error_does_not_remove_existing_registration(tmp_path):
    testing = _testing_module()

    p = tmp_path / "dim.parquet"
    _write_dim_parquet(str(p))

    sess = ttf.Session()
    sess.register_parquet("dim", str(p))
    assert testing._test_session_table_exists(sess, "dim") is True

    with pytest.raises(ValueError):
        sess.register_parquet("dim", "")

    assert testing._test_session_table_exists(sess, "dim") is True


def test_register_parquet_empty_path_does_not_register_when_none_existed(tmp_path):
    testing = _testing_module()

    sess = ttf.Session()
    with pytest.raises(ValueError):
        sess.register_parquet("dim", "")

    assert testing._test_session_table_exists(sess, "dim") is False


def test_register_parquet_empty_directory_registers_empty_table(tmp_path):
    testing = _testing_module()

    d = tmp_path / "empty_dir"
    d.mkdir()

    sess = ttf.Session()
    sess.register_parquet("dim", str(d))

    # DataFusion allows registering a directory with zero parquet files; it becomes an empty table.
    assert testing._test_session_table_exists(sess, "dim") is True


# TODO(Session.sql): query registered parquet and joins (tstable + dim parquet).
