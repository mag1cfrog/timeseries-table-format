from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

import timeseries_table_format as ttf


def _write_prices_parquet(path: Path) -> None:
    tbl = pa.table(
        {
            "ts": pa.array([0, 3_600 * 1_000_000], type=pa.timestamp("us")),
            "symbol": pa.array(["NVDA", "NVDA"], type=pa.string()),
            "close": pa.array([1.0, 2.0], type=pa.float64()),
        }
    )
    pq.write_table(tbl, str(path))


def _write_volumes_parquet(path: Path) -> None:
    tbl = pa.table(
        {
            "ts": pa.array([0, 3_600 * 1_000_000], type=pa.timestamp("us")),
            "symbol": pa.array(["NVDA", "NVDA"], type=pa.string()),
            "volume": pa.array([10, 20], type=pa.int64()),
        }
    )
    pq.write_table(tbl, str(path))


def run(*, base_dir: Path) -> pa.Table:
    prices_root = base_dir / "prices_tbl"
    prices = ttf.TimeSeriesTable.create(
        table_root=str(prices_root),
        time_column="ts",
        bucket="1h",
        entity_columns=["symbol"],
        timezone=None,
    )
    prices_seg = base_dir / "prices.parquet"
    _write_prices_parquet(prices_seg)
    prices.append_parquet(str(prices_seg))

    volumes_root = base_dir / "volumes_tbl"
    volumes = ttf.TimeSeriesTable.create(
        table_root=str(volumes_root),
        time_column="ts",
        bucket="1h",
        entity_columns=["symbol"],
        timezone=None,
    )
    volumes_seg = base_dir / "volumes.parquet"
    _write_volumes_parquet(volumes_seg)
    volumes.append_parquet(str(volumes_seg))

    sess = ttf.Session()
    sess.register_tstable("prices", str(prices_root))
    sess.register_tstable("volumes", str(volumes_root))

    return sess.sql(
        """
        select p.ts as ts, p.symbol as symbol, p.close as close, v.volume as volume
        from prices p
        join volumes v
        on p.ts = v.ts and p.symbol = v.symbol
        order by p.ts
        """
    )


def main() -> None:
    out = run(base_dir=Path("./my_tables"))
    print(out)


if __name__ == "__main__":
    main()
