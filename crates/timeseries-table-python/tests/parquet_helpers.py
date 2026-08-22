from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


def parquet_reader(path: str | Path) -> pa.RecordBatchReader:
    parquet_file = pq.ParquetFile(path)
    return pa.RecordBatchReader.from_batches(
        parquet_file.schema_arrow,
        parquet_file.iter_batches(),
    )
