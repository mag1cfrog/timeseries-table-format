from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

import timeseries_table_format as ttf


def _write_tiny_prices_parquet(path: Path) -> None:
    table = pa.table(
        {
            "ts": pa.array(
                [0, 0, 3_600 * 1_000_000, 3_600 * 1_000_000],
                type=pa.timestamp("us"),
            ),
            "exchange_id": pa.array([1, 2, 1, 2], type=pa.int32()),
            "symbol": pa.array(["NVDA", "AAPL", "NVDA", "AAPL"], type=pa.string()),
            "close": pa.array([10.0, 20.0, 11.0, 21.0], type=pa.float64()),
        }
    )
    pq.write_table(table, str(path))


def run(*, table_root: Path) -> pa.Table:
    table_root.mkdir(parents=True, exist_ok=True)

    tbl = ttf.TimeSeriesTable.create(
        table_root=str(table_root),
        index_column="ts",
        index_type="timestamp",
        bucket="1h",
        entity_columns=["exchange_id", "symbol"],
        timezone=None,
    )

    seg_path = table_root / "incoming" / "prices.parquet"
    seg_path.parent.mkdir(parents=True, exist_ok=True)
    _write_tiny_prices_parquet(seg_path)

    parquet_file = pq.ParquetFile(seg_path)
    tbl.append(
        pa.RecordBatchReader.from_batches(
            parquet_file.schema_arrow,
            parquet_file.iter_batches(),
        )
    )

    sess = ttf.Session()
    sess.register_tstable("prices", str(table_root))
    before = sess.sql(
        """
        select ts, exchange_id, symbol, close
        from prices
        order by exchange_id, symbol, ts
        """
    )

    report = tbl.optimize()
    assert report.source_segments_replaced == 1
    assert report.replacement_segments_written == 2
    assert report.distinct_identities_materialized == 2
    assert report.rows_read == report.rows_written == 4
    assert not report.no_op

    repeated = tbl.optimize()
    assert repeated.no_op
    assert repeated.starting_version == repeated.committed_version
    assert repeated.committed_version == report.committed_version

    sess = ttf.Session()
    sess.register_tstable("prices", str(table_root))
    after = sess.sql(
        """
        select ts, exchange_id, symbol, close
        from prices
        order by exchange_id, symbol, ts
        """
    )
    assert after.equals(before)
    return after


def main() -> None:
    out = run(table_root=Path("./my_table"))
    print(out)


if __name__ == "__main__":
    main()
