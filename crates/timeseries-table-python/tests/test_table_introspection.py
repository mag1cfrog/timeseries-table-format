import re

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import timeseries_table_format as ttf


def _safe_name(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9]+", "_", s).strip("_")


def _write_parquet(
    path: str, ts_us: list[int], symbol: list[str], close: list[float]
) -> None:
    tbl = pa.table(
        {
            "ts": pa.array(ts_us, type=pa.timestamp("us")),
            "symbol": pa.array(symbol, type=pa.string()),
            "close": pa.array(close, type=pa.float64()),
        }
    )
    pq.write_table(tbl, path)


def test_table_introspection_matches_expected_spec(tmp_path):
    root = tmp_path / "table"

    tbl = ttf.TimeSeriesTable.create(
        table_root=str(root),
        index_column="ts",
        index_type="timestamp",
        bucket="2hours",  # alias input; expect canonical "2h"
        entity_columns=["symbol"],
        timezone=None,
    )

    assert tbl.root() == str(root)
    assert isinstance(tbl.root(), str)

    assert tbl.version() == 1
    assert isinstance(tbl.version(), int)

    spec = tbl.index_spec()
    assert set(spec.keys()) == {
        "column",
        "entity_columns",
        "kind",
        "bucket",
        "timezone",
    }
    assert spec == {
        "column": "ts",
        "entity_columns": ["symbol"],
        "kind": "timestamp",
        "bucket": "2h",
        "timezone": None,
    }

    opened = ttf.TimeSeriesTable.open(str(root))
    assert opened.root() == str(root)
    assert opened.version() == 1
    assert opened.index_spec() == spec


def test_table_introspection_defaults_and_timezone(tmp_path):
    root = tmp_path / "table"

    tbl = ttf.TimeSeriesTable.create(
        table_root=str(root),
        index_column="timestamp",
        index_type="timestamp",
        bucket="15min",  # alias input; expect canonical "15m"
        entity_columns=None,
        timezone="America/New_York",
    )

    spec = tbl.index_spec()
    assert set(spec.keys()) == {
        "column",
        "entity_columns",
        "kind",
        "bucket",
        "timezone",
    }
    assert spec == {
        "column": "timestamp",
        "entity_columns": [],
        "kind": "timestamp",
        "bucket": "15m",
        "timezone": "America/New_York",
    }

    assert isinstance(spec["column"], str)
    assert isinstance(spec["entity_columns"], list)
    assert all(isinstance(x, str) for x in spec["entity_columns"])
    assert isinstance(spec["bucket"], str)
    assert isinstance(spec["timezone"], str)


@pytest.mark.parametrize(
    ("index_type", "bucket_width"),
    [("int64", 4), ("uint64", 2**64 - 1)],
)
def test_integer_index_spec_has_only_variant_keys(tmp_path, index_type, bucket_width):
    root = tmp_path / index_type
    table = ttf.TimeSeriesTable.create(
        table_root=str(root),
        index_column="idx",
        index_type=index_type,
        entity_columns=["symbol"],
        bucket_width=bucket_width,
    )

    assert table.index_spec() == {
        "column": "idx",
        "entity_columns": ["symbol"],
        "kind": index_type,
        "bucket_width": bucket_width,
    }
    assert ttf.TimeSeriesTable.open(str(root)).index_spec() == table.index_spec()


@pytest.mark.parametrize(
    ("bucket_spec", "expected"),
    [
        ("1s", "1s"),
        ("2sec", "2s"),
        ("2seconds", "2s"),
        ("3m", "3m"),
        ("3min", "3m"),
        ("3minutes", "3m"),
        ("4h", "4h"),
        ("4hr", "4h"),
        ("4hours", "4h"),
        ("5d", "5d"),
        ("5day", "5d"),
        ("5days", "5d"),
        ("6H", "6h"),
    ],
)
def test_table_introspection_bucket_formatting_canonical(
    bucket_spec, expected, tmp_path
):
    root = tmp_path / f"table_{_safe_name(bucket_spec)}"

    tbl = ttf.TimeSeriesTable.create(
        table_root=str(root),
        index_column="ts",
        index_type="timestamp",
        bucket=bucket_spec,
        entity_columns=["symbol"],
        timezone=None,
    )

    spec = tbl.index_spec()
    assert spec["bucket"] == expected


def test_table_introspection_entity_columns_preserves_order(tmp_path):
    root = tmp_path / "table"

    tbl = ttf.TimeSeriesTable.create(
        table_root=str(root),
        index_column="ts",
        index_type="timestamp",
        bucket="1h",
        entity_columns=["b", "a"],
        timezone=None,
    )

    spec = tbl.index_spec()
    assert spec["entity_columns"] == ["b", "a"]


def test_table_introspection_version_updates_after_append(tmp_path):
    root = tmp_path / "table"

    tbl = ttf.TimeSeriesTable.create(
        table_root=str(root),
        index_column="ts",
        index_type="timestamp",
        bucket="1h",
        entity_columns=["symbol"],
        timezone=None,
    )
    assert tbl.version() == 1

    (root / "data").mkdir(parents=True, exist_ok=True)
    seg = root / "data" / "seg.parquet"
    _write_parquet(
        str(seg),
        ts_us=[0, 3_600 * 1_000_000],
        symbol=["NVDA", "NVDA"],
        close=[1.0, 2.0],
    )

    v = tbl.append_parquet("data/seg.parquet", copy_if_outside=False)
    assert isinstance(v, int)
    assert v > 1
    assert tbl.version() == v

    reopened = ttf.TimeSeriesTable.open(str(root))
    assert reopened.version() == v


def test_table_introspection_returns_python_native_types(tmp_path):
    root = tmp_path / "table"

    tbl = ttf.TimeSeriesTable.create(
        table_root=str(root),
        index_column="ts",
        index_type="timestamp",
        bucket="1h",
        entity_columns=["symbol"],
        timezone=None,
    )

    assert isinstance(tbl.root(), str)
    assert isinstance(tbl.version(), int)

    spec = tbl.index_spec()
    assert set(spec.keys()) == {
        "column",
        "entity_columns",
        "kind",
        "bucket",
        "timezone",
    }
    assert isinstance(spec["column"], str)
    assert isinstance(spec["entity_columns"], list)
    assert all(isinstance(x, str) for x in spec["entity_columns"])
    assert isinstance(spec["bucket"], str)
    assert spec["timezone"] is None or isinstance(spec["timezone"], str)
