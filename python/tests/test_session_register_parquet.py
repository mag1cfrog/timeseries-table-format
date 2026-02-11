import threading
import pytest
import pyarrow as pa
import pyarrow.parquet as pq

import timeseries_table_format as ttf

def _write_dim_parquet(path: str) -> None:
    tbl = pa.table(
        {
            "symbol": pa.array(["NVDA", "AAPL"],
type=pa.string()),
            "exchange": pa.array(["NASDAQ", "NASDAQ"],
type=pa.string()),
        }
    )
    pq.write_table(tbl, path)

def test_register_parquet_file_succeeds_and_replaces(tmp_path):
    p = tmp_path / "dim.parquet"
    _write_dim_parquet(str(p))

    sess = ttf.Session()
    sess.register_parquet("dim", str(p))
    sess.register_parquet("dim", str(p)) # replace existing registration

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
    missing = tmp_path / "missing_parquet"

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