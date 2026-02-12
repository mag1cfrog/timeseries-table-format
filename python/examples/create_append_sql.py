#!/usr/bin/env python3
from __future__ import annotations

import argparse
import tempfile
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

import timeseries_table_format as ttf


def _write_tiny_prices_parquet(path: Path) -> None:
    table = pa.table(
        {
            "ts": pa.array([0, 3_600 * 1_000_000, 7_200 * 1_000_000], type=pa.timestamp("us")),
            "symbol": pa.array(["NVDA", "NVDA", "NVDA"], type=pa.string()),
            "close": pa.array([10.0, 20.0, 30.0], type=pa.float64()),
        }
    )
    pq.write_table(table, str(path))


def run(*, table_root: Path, rows: int) -> pa.Table:
    print(f"timeseries_table_format {ttf.__version__}")
    print(f"Table root: {table_root}")

    table_root.mkdir(parents=True, exist_ok=True)

    tbl = ttf.TimeSeriesTable.create(
        table_root=str(table_root),
        time_column="ts",
        bucket="1h",
        entity_columns=["symbol"],
        timezone=None,
    )

    # In real usage, your Parquet file may live outside the table root.
    # By default, append will copy segments under the table root if needed.
    seg_path = table_root / "incoming" / "input.parquet"
    seg_path.parent.mkdir(parents=True, exist_ok=True)
    _write_tiny_prices_parquet(seg_path)
    new_version = tbl.append_parquet(str(seg_path))
    print(f"Appended segment, table version is now {new_version}")

    sess = ttf.Session()
    sess.register_tstable("prices", str(table_root))

    out = sess.sql(
        """
        select ts, symbol, close
        from prices
        order by ts
        """
    )

    print("\nResult schema:")
    print(out.schema)

    print(f"\nFirst {rows} rows:")
    print(out.slice(0, rows))

    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "End-to-end demo: create a table, append a tiny Parquet segment, "
            "register it in a Session, and query with SQL."
        )
    )
    parser.add_argument(
        "--table-root",
        type=Path,
        default=None,
        help=(
            "Directory to create the table in. If omitted, a temporary directory is used."
        ),
    )
    parser.add_argument(
        "--keep",
        action="store_true",
        help="If using a temporary directory, do not delete it (prints the path).",
    )
    parser.add_argument(
        "--rows",
        type=int,
        default=5,
        help="How many result rows to print.",
    )
    args = parser.parse_args(argv)

    if args.rows < 0:
        raise SystemExit("--rows must be >= 0")

    if args.table_root is not None:
        run(table_root=args.table_root, rows=args.rows)
        return 0

    if args.keep:
        tmp = Path(tempfile.mkdtemp(prefix="ttf-demo-"))
        print(f"Using temp dir (kept): {tmp}")
        run(table_root=tmp / "prices_tbl", rows=args.rows)
        return 0

    with tempfile.TemporaryDirectory(prefix="ttf-demo-") as td:
        tmp = Path(td)
        run(table_root=tmp / "prices_tbl", rows=args.rows)
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
