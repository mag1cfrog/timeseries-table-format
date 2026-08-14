#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path


MIB = 1024 * 1024
DEFAULT_MAX_RSS_DELTA_BYTES = 128 * MIB
MAX_RSS_LABEL = "Maximum resident set size (kbytes)"
DEFAULT_ROW_COUNT = 1024
DEFAULT_ROW_GROUP_COUNT = 32


def generate_parquet_fixture(
    path: Path,
    target_payload_bytes: int,
    row_count: int = DEFAULT_ROW_COUNT,
    row_group_count: int = DEFAULT_ROW_GROUP_COUNT,
) -> None:
    """Write an uncompressed Parquet file without buffering its full payload."""
    if target_payload_bytes < row_count:
        raise ValueError("target payload must provide at least one byte per row")
    if row_count <= 0 or row_group_count <= 0 or row_count % row_group_count:
        raise ValueError("row count must be positive and divisible by row-group count")

    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ModuleNotFoundError as error:
        raise RuntimeError(
            "PyArrow is required; run this script through the Python project environment"
        ) from error

    path.parent.mkdir(parents=True, exist_ok=True)
    rows_per_group = row_count // row_group_count
    payload = b"x" * (target_payload_bytes // row_count)
    schema = pa.schema(
        [
            ("ts", pa.timestamp("ms")),
            ("entity", pa.string()),
            ("payload", pa.binary()),
        ]
    )

    with pq.ParquetWriter(
        path,
        schema,
        compression="NONE",
        use_dictionary=False,
        write_statistics=["ts", "entity"],
    ) as writer:
        for group in range(row_group_count):
            first_row = group * rows_per_group
            table = pa.Table.from_arrays(
                [
                    pa.array(
                        range(first_row, first_row + rows_per_group),
                        type=pa.timestamp("ms"),
                    ),
                    pa.array(["benchmark"] * rows_per_group, type=pa.string()),
                    pa.array([payload] * rows_per_group, type=pa.binary()),
                ],
                schema=schema,
            )
            writer.write_table(table, row_group_size=rows_per_group)


def generate_fixture_pair(
    directory: Path,
    small_target_bytes: int,
    large_target_bytes: int,
    row_count: int = DEFAULT_ROW_COUNT,
    row_group_count: int = DEFAULT_ROW_GROUP_COUNT,
) -> tuple[Path, Path]:
    if small_target_bytes >= large_target_bytes:
        raise ValueError("small target must be less than large target")

    small_path = directory / "small.parquet"
    large_path = directory / "large.parquet"
    generate_parquet_fixture(small_path, small_target_bytes, row_count, row_group_count)
    generate_parquet_fixture(large_path, large_target_bytes, row_count, row_group_count)
    return small_path, large_path


def parse_max_rss_bytes(time_output: str) -> int:
    """Extract GNU time's peak RSS value and convert KiB to bytes."""
    for line in time_output.splitlines():
        label, separator, raw_value = line.partition(":")
        if label.strip() != MAX_RSS_LABEL:
            continue
        if not separator:
            break
        try:
            rss_kib = int(raw_value.strip())
        except ValueError as error:
            raise ValueError(
                f"invalid {MAX_RSS_LABEL}: {raw_value.strip()!r}"
            ) from error
        if rss_kib <= 0:
            raise ValueError(f"invalid {MAX_RSS_LABEL}: {rss_kib}")
        return rss_kib * 1024

    raise ValueError(f"missing {MAX_RSS_LABEL}")


def require_bounded_rss(
    small_rss_bytes: int,
    large_rss_bytes: int,
    max_delta_bytes: int = DEFAULT_MAX_RSS_DELTA_BYTES,
) -> int:
    """Return the signed RSS delta, failing only when it exceeds the limit."""
    if min(small_rss_bytes, large_rss_bytes, max_delta_bytes) < 0:
        raise ValueError("RSS values and maximum delta must be non-negative")

    delta_bytes = large_rss_bytes - small_rss_bytes
    if delta_bytes > max_delta_bytes:
        raise RuntimeError(
            f"peak RSS grew by {delta_bytes} bytes; limit is {max_delta_bytes} bytes"
        )
    return delta_bytes
