#!/usr/bin/env python3
from __future__ import annotations


MIB = 1024 * 1024
DEFAULT_MAX_RSS_DELTA_BYTES = 128 * MIB
MAX_RSS_LABEL = "Maximum resident set size (kbytes)"


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
            raise ValueError(f"invalid {MAX_RSS_LABEL}: {raw_value.strip()!r}") from error
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
