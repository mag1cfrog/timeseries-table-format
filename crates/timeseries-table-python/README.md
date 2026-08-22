# timeseries-table-format (Python)

Build local, append-only time-series tables from Parquet files. The Python API
tracks coverage, rejects overlapping appends, and queries tables with
DataFusion SQL. Query results are returned as `pyarrow.Table` objects.

- [Documentation](https://mag1cfrog.github.io/timeseries-table-format/)
- [PyPI](https://pypi.org/project/timeseries-table-format/)
- [Repository](https://github.com/mag1cfrog/timeseries-table-format)
- [Changelog](https://github.com/mag1cfrog/timeseries-table-format/blob/main/CHANGELOG.md)

> **Early MVP:** APIs and on-disk layouts may change before v1.0.

## What it provides

- Timestamp, Int64, or UInt64 ordered indexes
- Per-entity coverage tracking and overlap-safe ingestion
- Local table roots containing metadata and Parquet segments
- DataFusion SQL over managed tables and standalone Parquet data
- Materialized and streaming Arrow result APIs

The current release supports local filesystems only. It does not yet support
object storage, small-file compaction, schema evolution, row updates, merges,
or time-travel queries.

## Install

```bash
pip install timeseries-table-format
```

The package requires Python 3.10 or later and installs `pyarrow` automatically.
See [Installation](https://mag1cfrog.github.io/timeseries-table-format/install/)
for supported platforms, Python implementations, and source-build guidance.

## Verify the installation

```python
import timeseries_table_format as ttf

result = ttf.Session().sql("SELECT 1 AS value")
assert result.column("value").to_pylist() == [1]
```

## Create, append, and query a table

Assuming `prices.parquet` contains a Timestamp column named `ts` and a string
column named `symbol`:

```python
import pyarrow as pa
import pyarrow.parquet as pq
import timeseries_table_format as ttf

table = ttf.TimeSeriesTable.create(
    table_root="prices",
    index_column="ts",
    index_type="timestamp",
    bucket="1h",
    entity_columns=["symbol"],
)
parquet_file = pq.ParquetFile("prices.parquet")
version = table.append(
    pa.RecordBatchReader.from_batches(
        parquet_file.schema_arrow,
        parquet_file.iter_batches(),
    )
)

session = ttf.Session()
session.register_tstable("prices", "prices")
result = session.sql("SELECT * FROM prices ORDER BY symbol, ts")
```

For a complete runnable example, follow
[Create, append, and query your first table](https://mag1cfrog.github.io/timeseries-table-format/tutorials/create_append_query/).

## Documentation

- [Decide if the project fits your workload](https://mag1cfrog.github.io/timeseries-table-format/concepts/when_to_use_this/)
- [Append files incrementally](https://mag1cfrog.github.io/timeseries-table-format/tutorials/real_world_workflow/)
- [Register and join tables](https://mag1cfrog.github.io/timeseries-table-format/tutorials/register_and_join/)
- [Use SQL parameters](https://mag1cfrog.github.io/timeseries-table-format/tutorials/parameterized_queries/)
- [Stream query results](https://mag1cfrog.github.io/timeseries-table-format/guides/stream_query_results/)
- [Configure native logging](https://mag1cfrog.github.io/timeseries-table-format/guides/native_logging/)
- [Configure notebook display](https://mag1cfrog.github.io/timeseries-table-format/guides/notebook_display/)
- [Read the API reference](https://mag1cfrog.github.io/timeseries-table-format/reference/session/)
- [Troubleshoot common problems](https://mag1cfrog.github.io/timeseries-table-format/troubleshooting/)

## Contributing

See [Develop the Python package](https://mag1cfrog.github.io/timeseries-table-format/contributing/)
for local setup, tests, quality checks, documentation builds, benchmarks, and
release guidance.

## License

MIT. See the [repository license](https://github.com/mag1cfrog/timeseries-table-format/blob/main/LICENSE).
