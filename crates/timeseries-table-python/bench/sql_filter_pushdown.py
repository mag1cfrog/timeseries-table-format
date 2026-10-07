#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
from statistics import median
import subprocess
import sys
import tempfile
import time

import pyarrow as pa
import timeseries_table_format as ttf

from sql_conversion import (
    _fmt_optional_bytes,
    _prepare_wide_table,
    _try_peak_rss_bytes,
    _write_results,
)


def _query_case(name: str, rows: int) -> tuple[str, list[int]]:
    index = 2 * rows + rows // 2
    key = hashlib.sha256(str(index).encode()).hexdigest()
    if name == "selective":
        return f"SELECT * FROM prices WHERE record_id = '{key}'", [index]
    if name == "nonselective":
        return (
            f"SELECT * FROM prices WHERE record_id <> '{key}'",
            [i for i in range(rows * 4) if i != index],
        )
    return "SELECT * FROM prices", list(range(rows * 4))


def _measure_query(args: argparse.Namespace) -> dict[str, object]:
    query, expected_indices = _query_case(args.query_case, args.rows_per_segment)
    session = ttf.Session()
    session.sql("SET datafusion.execution.parquet.pushdown_filters = " + args.pushdown)
    session.register_tstable("prices", args.table_root)
    before = _try_peak_rss_bytes()
    started = time.perf_counter()
    result = session.sql(query)
    elapsed = time.perf_counter() - started
    peak = _try_peak_rss_bytes()
    out: dict[str, object] = {
        "total_s": elapsed,
        "row_count": result.num_rows,
        "result_arrow_bytes": result.nbytes,
        "rows_per_second": result.num_rows / elapsed,
        "process_peak_before_query_bytes": before,
        "peak_rss_bytes": peak,
    }

    # Validate every returned key and payload after recording timing and peak memory.
    entities = result["entity"].to_pylist()
    ticks = result["tick"].to_pylist()
    assert all(0 <= entity < 4 for entity in entities)
    assert all(0 <= tick < args.rows_per_segment for tick in ticks)
    indices = [
        entity * args.rows_per_segment + tick for entity, tick in zip(entities, ticks)
    ]
    assert sorted(indices) == expected_indices
    digests = [hashlib.sha256(str(index).encode()).digest() for index in indices]
    assert result["record_id"].to_pylist() == [digest.hex() for digest in digests]
    payload = pa.chunked_array([pa.array([digest * 64 for digest in digests])])
    for column in range(8):
        assert result[f"payload_{column}"].equals(payload)
    assert result.nbytes == len(expected_indices) * 16500
    del result, payload

    # EXPLAIN ANALYZE executes the query again, outside the measured sample.
    out["explain_analyze"] = session.sql("EXPLAIN ANALYZE " + query).to_pylist()
    session.sql("SET datafusion.catalog.information_schema = true")
    for name in ("batch_size", "target_partitions", "parquet.pushdown_filters"):
        value = session.sql(f"SHOW datafusion.execution.{name}")["value"][0].as_py()
        out[f"effective_{name}"] = value
    assert out["effective_parquet.pushdown_filters"] == args.pushdown
    return out


def _worker(args: argparse.Namespace, mode: str, *options: str) -> dict[str, object]:
    completed = subprocess.run(
        [
            sys.executable,
            str(Path(__file__).resolve()),
            "--worker-mode",
            mode,
            "--table-root",
            args.table_root,
            "--rows-per-segment",
            str(args.rows_per_segment),
            *options,
        ],
        check=True,
        stdout=subprocess.PIPE,
        text=True,
        timeout=60,
    )
    return json.loads(completed.stdout)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compare Parquet filter pushdown on selective and nonselective wide-row SQL queries."
    )
    parser.add_argument("--rows-per-segment", type=int, default=8192)
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument("--json", default="")
    parser.add_argument("--print-json", action="store_true")
    parser.add_argument("--summary", action="store_true")
    parser.add_argument(
        "--worker-mode", choices=("prepare", "read"), help=argparse.SUPPRESS
    )
    parser.add_argument("--table-root", default="", help=argparse.SUPPRESS)
    parser.add_argument(
        "--query-case",
        choices=("selective", "nonselective", "unfiltered"),
        help=argparse.SUPPRESS,
    )
    parser.add_argument("--pushdown", choices=("false", "true"), help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.rows_per_segment <= 0 or args.runs <= 0 or args.warmups < 0:
        parser.error(
            "rows-per-segment and runs must be positive; warmups must be nonnegative"
        )
    if args.worker_mode:
        if not args.table_root:
            parser.error("--table-root is required for a worker")
        if args.worker_mode == "prepare":
            out = _prepare_wide_table(Path(args.table_root), args.rows_per_segment)
        else:
            if args.query_case is None or args.pushdown is None:
                parser.error(
                    "--query-case and --pushdown are required for a read worker"
                )
            out = _measure_query(args)
        print(json.dumps(out))
        return

    with tempfile.TemporaryDirectory() as directory:
        args.table_root = str(Path(directory) / "wide_table")
        dataset = _worker(args, "prepare")
        results = []
        for case in ("selective", "nonselective", "unfiltered"):
            for pushdown in ("false", "true"):
                samples = []
                for run in range(args.warmups + args.runs):
                    sample = _worker(
                        args, "read", "--query-case", case, "--pushdown", pushdown
                    )
                    if run >= args.warmups:
                        samples.append(sample)
                results.append(
                    {
                        "case": case,
                        "pushdown_filters": pushdown == "true",
                        "samples": samples,
                    }
                )
                if args.summary:
                    elapsed = median(sample["total_s"] for sample in samples)
                    peak = _fmt_optional_bytes(
                        [sample["peak_rss_bytes"] for sample in samples]
                    )
                    print(
                        f"{case} pushdown={pushdown}: total={elapsed * 1000:.1f} ms peak={peak}",
                        file=sys.stderr,
                    )
        _write_results(
            {
                "env": {
                    "python": sys.version.replace("\n", " "),
                    "platform": platform.platform(),
                    "cpus": os.cpu_count(),
                    "ttf_version": ttf.__version__,
                    "pyarrow_version": pa.__version__,
                },
                "params": {"runs": args.runs, "warmups": args.warmups},
                "dataset": dataset,
                "results": results,
                "notes": [
                    "Fixture preparation and every read run in separate processes; filesystem caches are not cleared.",
                    "Each query selects all columns. Selective matches one hashed record_id; nonselective excludes that record_id; unfiltered has no predicate.",
                    "Timing and process peak memory are recorded before validating every returned key and payload or running EXPLAIN ANALYZE.",
                    "EXPLAIN ANALYZE executes the query a second time in the worker; its metrics are separate from the timed sample.",
                    "Peak RSS includes interpreter and runtime allocations (Windows: peak working set); the compressible fixture and local results are not portable performance thresholds.",
                ],
            },
            args,
        )


if __name__ == "__main__":
    main()
