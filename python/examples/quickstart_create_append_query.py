from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

import timeseries_table_format as ttf


def _write_tiny_prices_parquet(path: Path) -> None:
    table = pa.table(
        {
            "ts": pa.array(
                [0, 3_600 * 1_000_000, 7_200 * 1_000_000],
                type=pa.timestamp("us"),
            ),
            "symbol": pa.array(["NVDA", "NVDA", "NVDA"], type=pa.string()),
            "close": pa.array([10.0, 20.0, 30.0], type=pa.float64()),
        }
    )
    pq.write_table(table, str(path))


def run(*, table_root: Path) -> pa.Table:
    table_root.mkdir(parents=True, exist_ok=True)

    tbl = ttf.TimeSeriesTable.create(
        table_root=str(table_root),
        time_column="ts",
        bucket="1h",
        entity_columns=["symbol"],
        timezone=None,
    )

    seg_path = table_root / "incoming" / "prices.parquet"
    seg_path.parent.mkdir(parents=True, exist_ok=True)
    _write_tiny_prices_parquet(seg_path)

    tbl.append_parquet(str(seg_path))

    sess = ttf.Session()
    sess.register_tstable("prices", str(table_root))

    return sess.sql(
        """
        select ts, symbol, close
        from prices
        order by ts
        """
    )


def main() -> None:
    out = run(table_root=Path("./my_table"))
    print(out)


if __name__ == "__main__":
    main()
