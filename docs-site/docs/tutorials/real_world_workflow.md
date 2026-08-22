# Append files incrementally

Use this pattern when new Parquet files arrive on a schedule and each file
covers a new time window.

Before you start, complete [Create, append, and query your first table](create_append_query.md).

## Open or create the table

Create the table on the first run. Open it on later runs:

```python
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import timeseries_table_format as ttf

TABLE_ROOT = Path("prices_table")


def open_or_create_table() -> ttf.TimeSeriesTable:
    if TABLE_ROOT.exists():
        return ttf.TimeSeriesTable.open(str(TABLE_ROOT))

    return ttf.TimeSeriesTable.create(
        table_root=str(TABLE_ROOT),
        index_column="ts",
        index_type="timestamp",
        bucket="1h",
        entity_columns=["symbol"],
    )
```

Checking the path first avoids hiding permission errors, damaged metadata, or
other failures from `TimeSeriesTable.open(...)`. An existing invalid table root
should fail instead of being silently replaced.

## Append the new files

Append files in a stable order so job logs and table versions are predictable:

```python
def ingest_files(new_files: list[Path]) -> None:
    table = open_or_create_table()

    for segment in sorted(new_files):
        parquet_file = pq.ParquetFile(segment)
        reader = pa.RecordBatchReader.from_batches(
            parquet_file.schema_arrow,
            parquet_file.iter_batches(),
        )
        version = table.append(reader)
        print(f"appended {segment.name} at table version {version}")
```

If a file overlaps existing coverage, `append(...)` raises
`CoverageOverlapError` and leaves that append uncommitted.

!!! warning "Do not blindly ignore overlap errors"
    An overlap can mean a fully duplicated file or a file containing both old
    and new time windows. Skipping every overlap could silently discard new
    data. Inspect the input and resolve the overlap before continuing.

## Verify the result

Query a summary after the append loop:

```python
def print_summary() -> None:
    session = ttf.Session()
    session.register_tstable("prices", str(TABLE_ROOT))
    print(
        session.sql(
            "SELECT min(ts), max(ts), count(*) AS row_count FROM prices"
        )
    )
```

Call `ingest_files(...)` and then `print_summary()` from your scheduled job.

For overlap behavior and recovery details, see
[Buckets and overlap](../concepts/bucketing_and_overlap.md) and
[Exceptions](../reference/exceptions.md).
