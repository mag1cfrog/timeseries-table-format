#!/usr/bin/env python3
from __future__ import annotations

import argparse
import gc
import json
import os
import platform
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa
import pyarrow.ipc as pa_ipc
import pyarrow.parquet as pq

import timeseries_table_format as ttf


def _require_testing_module():
    testing = getattr(ttf._native, "_testing", None)
    if testing is None:
        raise RuntimeError(
            "Missing ttf._native._testing (bench hook). Rebuild the extension with --features test-utils."
        )
    return testing


def _require_numpy():
    try:
        import numpy as np  # type: ignore
    except ModuleNotFoundError as e:  # pragma: no cover
        raise RuntimeError(
            "This benchmark requires numpy for efficient Parquet generation. "
            "Install it (e.g. `uv pip install numpy` or `pip install numpy`) and retry."
        ) from e
    return np


def _now() -> float:
    return time.perf_counter()


@dataclass(frozen=True)
class DatasetSpec:
    target_ipc_bytes: int
    entities: int
    float_cols: int
    chunk_rows: int
    seed: int

    @property
    def bytes_per_row_est(self) -> int:
        # Fixed-width estimate: ts(i64) + entity_id(i32) + float_cols * f64
        # (ignores small per-batch IPC metadata overhead)
        return 8 + 4 + 8 * self.float_cols

    @property
    def total_rows_est(self) -> int:
        bpr = self.bytes_per_row_est
        return max(1, int((self.target_ipc_bytes + bpr - 1) // bpr))


def _write_parquet_generated(seg_path: Path, spec: DatasetSpec) -> dict[str, int]:
    np = _require_numpy()

    seg_path.parent.mkdir(parents=True, exist_ok=True)

    schema = pa.schema(
        [
            ("ts", pa.timestamp("ns")),
            ("entity_id", pa.int32()),
            *[(f"f{i}", pa.float64()) for i in range(spec.float_cols)],
        ]
    )

    rng = np.random.default_rng(spec.seed)
    total_rows = spec.total_rows_est

    with pq.ParquetWriter(str(seg_path), schema, compression="snappy") as writer:
        start = 0
        while start < total_rows:
            n = min(spec.chunk_rows, total_rows - start)

            ts = (np.arange(start, start + n, dtype=np.int64) * 1_000_000_000).astype(
                np.int64, copy=False
            )
            entity_id = (
                np.arange(start, start + n, dtype=np.int64) % spec.entities
            ).astype(np.int32, copy=False)

            cols: dict[str, pa.Array] = {
                "ts": pa.array(ts, type=pa.timestamp("ns")),
                "entity_id": pa.array(entity_id, type=pa.int32()),
            }

            for i in range(spec.float_cols):
                # Use a stable distribution but keep generation simple and deterministic.
                data = rng.standard_normal(n).astype(np.float64, copy=False)
                cols[f"f{i}"] = pa.array(data, type=pa.float64())

            writer.write_table(pa.table(cols, schema=schema), row_group_size=n)
            start += n

    return {"rows": total_rows, "parquet_bytes": seg_path.stat().st_size}


def _timed(fn):
    t0 = _now()
    out = fn()
    t1 = _now()
    return (t1 - t0), out


def _summarize_seconds(xs: list[float]) -> dict[str, object]:
    xs_sorted = sorted(xs)
    mid = xs_sorted[len(xs_sorted) // 2]
    return {"min_s": xs_sorted[0], "median_s": mid, "runs_s": xs}


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser(
        description="Micro-benchmark: SQL -> Arrow IPC -> pyarrow.Table"
    )
    ap.add_argument("--target-ipc-gb", type=float, default=2.0)
    ap.add_argument("--ipc-compression", choices=["none", "zstd"], default="none")
    ap.add_argument("--entities", type=int, default=1000)
    ap.add_argument("--float-cols", type=int, default=24)
    ap.add_argument("--chunk-rows", type=int, default=500_000)
    ap.add_argument("--seed", type=int, default=0)
    ap.add_argument("--warmups", type=int, default=1)
    ap.add_argument("--runs", type=int, default=3)
    ap.add_argument("--medium-ipc-mb", type=int, default=64)
    ap.add_argument("--no-gc-disable", action="store_true")
    ap.add_argument("--json", type=str, default="")
    args = ap.parse_args(argv)

    if args.target_ipc_gb <= 0:
        raise SystemExit("--target-ipc-gb must be > 0")
    if args.entities <= 0:
        raise SystemExit("--entities must be > 0")
    if args.float_cols <= 0:
        raise SystemExit("--float-cols must be > 0")
    if args.chunk_rows <= 0:
        raise SystemExit("--chunk-rows must be > 0")
    if args.warmups < 0 or args.runs <= 0:
        raise SystemExit("--warmups must be >= 0 and --runs must be > 0")

    testing = _require_testing_module()

    target_ipc_bytes = int(args.target_ipc_gb * (1024**3))
    spec = DatasetSpec(
        target_ipc_bytes=target_ipc_bytes,
        entities=args.entities,
        float_cols=args.float_cols,
        chunk_rows=args.chunk_rows,
        seed=args.seed,
    )

    if not args.no_gc_disable:
        gc_was_enabled = gc.isenabled()
        gc.disable()
    else:
        gc_was_enabled = False

    try:
        with tempfile.TemporaryDirectory() as d:
            base = Path(d)
            table_root = base / "prices_tbl"
            seg_path = table_root / "incoming" / "seg.parquet"

            tbl = ttf.TimeSeriesTable.create(
                table_root=str(table_root),
                time_column="ts",
                bucket="1h",
                entity_columns=["entity_id"],
                timezone=None,
            )

            gen_info = _write_parquet_generated(seg_path, spec)
            tbl.append_parquet(str(seg_path), copy_if_outside=False)

            sess = ttf.Session()
            sess.register_tstable("prices", str(table_root))

            medium_limit_rows = max(
                1,
                int((args.medium_ipc_mb * 1024 * 1024) // spec.bytes_per_row_est),
            )

            queries = [
                ("small_count", "select count(*) as n from prices"),
                ("medium_limit", f"select * from prices limit {medium_limit_rows}"),
                ("large_all", "select * from prices"),
            ]

            out: dict[str, object] = {
                "env": {
                    "python": sys.version.replace("\n", " "),
                    "platform": platform.platform(),
                    "ttf_version": getattr(ttf, "__version__", "unknown"),
                    "pyarrow_version": pa.__version__,
                    "pid": os.getpid(),
                },
                "params": {
                    "target_ipc_gb": args.target_ipc_gb,
                    "ipc_compression": args.ipc_compression,
                    "entities": args.entities,
                    "float_cols": args.float_cols,
                    "chunk_rows": args.chunk_rows,
                    "seed": args.seed,
                    "warmups": args.warmups,
                    "runs": args.runs,
                    "medium_ipc_mb": args.medium_ipc_mb,
                    "bytes_per_row_est": spec.bytes_per_row_est,
                    "rows_est": spec.total_rows_est,
                },
                "dataset": gen_info,
                "results": [],
                "notes": [
                    "decode_only measures pyarrow.ipc.open_stream(bytes).read_all() time.",
                    "bench_sql_ipc measures query planning+execution+collect plus IPC encoding on the Rust side.",
                    "session_sql measures end-to-end Session.sql(...) (includes Rust + Python decode).",
                    "Large targets can require high peak RAM (IPC bytes + decoded Table + intermediate buffers).",
                ],
            }

            for name, sql in queries:
                # Warmup: run both paths once to populate OS page cache and DataFusion internal caches.
                for _ in range(args.warmups):
                    _t, table = _timed(lambda: sess.sql(sql))
                    del table

                    _t, (ipc_bytes, _m) = _timed(
                        lambda: testing._bench_sql_ipc(
                            sess, sql, ipc_compression=args.ipc_compression
                        )
                    )
                    _t, table = _timed(lambda: pa_ipc.open_stream(ipc_bytes).read_all())
                    del table
                    del ipc_bytes
                    gc.collect()

                session_sql_times: list[float] = []
                bench_sql_ipc_times: list[float] = []
                decode_only_times: list[float] = []
                ipc_bytes_lens: list[int] = []
                arrow_mem_bytes: list[int] = []
                row_counts: list[int] = []
                batch_counts: list[int] = []
                rust_total_ms: list[float] = []
                rust_plan_ms: list[float] = []
                rust_collect_ms: list[float] = []
                rust_ipc_encode_ms: list[float] = []

                for _ in range(args.runs):
                    t_sess, table = _timed(lambda: sess.sql(sql))
                    session_sql_times.append(t_sess)
                    del table
                    gc.collect()

                for _ in range(args.runs):
                    t_bench, (ipc_bytes, m) = _timed(
                        lambda: testing._bench_sql_ipc(
                            sess, sql, ipc_compression=args.ipc_compression
                        )
                    )
                    bench_sql_ipc_times.append(t_bench)

                    t_decode, table = _timed(
                        lambda: pa_ipc.open_stream(ipc_bytes).read_all()
                    )
                    decode_only_times.append(t_decode)

                    # Rust-side metrics (per run).
                    ipc_bytes_lens.append(int(m["ipc_bytes_len"]))
                    arrow_mem_bytes.append(int(m["arrow_mem_bytes"]))
                    row_counts.append(int(m["row_count"]))
                    batch_counts.append(int(m["batch_count"]))
                    rust_total_ms.append(float(m["total_ms"]))
                    rust_plan_ms.append(float(m["plan_ms"]))
                    rust_collect_ms.append(float(m["collect_ms"]))
                    rust_ipc_encode_ms.append(float(m["ipc_encode_ms"]))

                    del table
                    del ipc_bytes
                    gc.collect()

                out["results"].append(
                    {
                        "name": name,
                        "sql": sql,
                        "session_sql": _summarize_seconds(session_sql_times),
                        "bench_sql_ipc": _summarize_seconds(bench_sql_ipc_times),
                        "decode_only": _summarize_seconds(decode_only_times),
                        "ipc_bytes_len": ipc_bytes_lens,
                        "arrow_mem_bytes": arrow_mem_bytes,
                        "ipc_to_arrow_ratio": [
                            (b / m) if m else None
                            for b, m in zip(ipc_bytes_lens, arrow_mem_bytes)
                        ],
                        "row_count": row_counts,
                        "batch_count": batch_counts,
                        "rust_ms": {
                            "total_ms": rust_total_ms,
                            "plan_ms": rust_plan_ms,
                            "collect_ms": rust_collect_ms,
                            "ipc_encode_ms": rust_ipc_encode_ms,
                        },
                    }
                )

            payload = json.dumps(out, indent=2, sort_keys=False)
            if args.json:
                Path(args.json).write_text(payload, encoding="utf-8")
            else:
                print(payload)
            return 0
    finally:
        if not args.no_gc_disable and gc_was_enabled:
            gc.enable()


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
