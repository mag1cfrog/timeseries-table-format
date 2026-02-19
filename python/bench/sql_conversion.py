#!/usr/bin/env python3
from __future__ import annotations

import argparse
import errno
import gc
import importlib
import json
import os
import platform
import shutil
import sys
import tempfile
import time
from contextlib import contextmanager
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
        return importlib.import_module("numpy")
    except ModuleNotFoundError as e:  # pragma: no cover
        raise RuntimeError(
            "This benchmark requires numpy for efficient Parquet generation. "
            "Install it (e.g. `uv pip install numpy` or `pip install numpy`) and retry."
        ) from e


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


@contextmanager
def _temp_env_var(key: str, value: str | None):
    old = os.environ.get(key)
    if value is None:
        os.environ.pop(key, None)
    else:
        os.environ[key] = value
    try:
        yield
    finally:
        if old is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = old


def _median(xs: list[float]) -> float:
    xs_sorted = sorted(xs)
    return xs_sorted[len(xs_sorted) // 2]


def _fmt_seconds(s: float) -> str:
    if s < 1e-3:
        return f"{s * 1e6:.0f}µs"
    if s < 1.0:
        return f"{s * 1e3:.1f}ms"
    return f"{s:.3f}s"


def _print_summary(out: dict[str, object]) -> None:
    print("SQL conversion benchmark summary", file=sys.stderr)
    print(
        f"pyarrow={out['env']['pyarrow_version']} ttf={out['env']['ttf_version']} runs={out['params']['runs']} warmups={out['params']['warmups']}",  # type: ignore[index]
        file=sys.stderr,
    )

    for r in out["results"]:  # type: ignore[index]
        name = r["name"]

        ipc = float(r["session_sql_ipc"]["median_s"])
        cs = float(r["session_sql_c_stream"]["median_s"])
        delta = ipc - cs
        pct_abs = (abs(delta) / ipc * 100.0) if ipc else float("nan")
        relation = "faster" if delta >= 0 else "slower"
        time_word = "saved" if delta >= 0 else "overhead"

        print(
            f"- {name}: session_sql median ipc={_fmt_seconds(ipc)} c_stream={_fmt_seconds(cs)} ({pct_abs:.1f}% {relation}, {_fmt_seconds(abs(delta))} {time_word})",
            file=sys.stderr,
        )

        # Rust-side export breakdown (best-effort).
        try:
            ipc_encode_ms = _median([float(x) for x in r["rust_ms"]["ipc_encode_ms"]])
            c_export_ms = _median(
                [float(x) for x in r["rust_ms_c_stream"]["c_stream_export_ms"]]
            )
            print(
                f"  rust export median: ipc_encode={ipc_encode_ms:.1f}ms c_stream_export={c_export_ms:.1f}ms",
                file=sys.stderr,
            )
        except Exception:
            pass


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser(
        description="Micro-benchmark: SQL -> (IPC vs Arrow C Stream) -> pyarrow.Table"
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
    ap.add_argument(
        "--print-json",
        action="store_true",
        help="Print the JSON payload to stdout even when --json is set.",
    )
    ap.add_argument(
        "--summary",
        action="store_true",
        help="Print a human-friendly summary to stderr (JSON still printed/saved as usual).",
    )
    ap.add_argument(
        "--tmpdir",
        type=str,
        default="",
        help="Directory to place the temporary benchmark dataset (Parquet + table).",
    )
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
        tmpdir = args.tmpdir.strip() or None
        if tmpdir is not None:
            Path(tmpdir).mkdir(parents=True, exist_ok=True)

        with tempfile.TemporaryDirectory(dir=tmpdir) as d:
            base = Path(d)
            table_root = base / "prices_tbl"
            seg_path = table_root / "incoming" / "seg.parquet"

            tbl = ttf.TimeSeriesTable.create(
                table_root=str(table_root),
                time_column="ts",
                bucket="1h",
                # v0 entity identity extraction currently only supports string entity columns and
                # requires them to be constant within a segment. For this conversion benchmark we
                # keep entity identity disabled (empty list) and treat `entity_id` as a normal
                # column used only for query shape/bytes.
                entity_columns=[],
                timezone=None,
            )

            try:
                gen_info = _write_parquet_generated(seg_path, spec)
            except OSError as e:
                # EDQUOT is not consistent across platforms (e.g. Linux: 122, macOS: 69).
                # Also handle ENOSPC for "no space left on device".
                quota_errnos = {getattr(errno, "EDQUOT", None)}
                space_errnos = {getattr(errno, "ENOSPC", None)}
                if e.errno in (quota_errnos | space_errnos):
                    usage = shutil.disk_usage(str(base))
                    raise RuntimeError(
                        "Disk space/quota exceeded while generating the benchmark Parquet dataset.\n"
                        f"tmp_dir={base}\n"
                        f"oserror_errno={e.errno}\n"
                        f"free_bytes={usage.free}\n"
                        f"target_ipc_gb={args.target_ipc_gb}\n"
                        "Try a smaller --target-ipc-gb, or run with --tmpdir pointing to a filesystem with more space "
                        "(or set TMPDIR to such a directory)."
                    ) from e
                raise
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
                    "c_stream_decode_only measures pyarrow.RecordBatchReader.from_stream(obj_with___arrow_c_stream__) + .read_all() + .close() time.",
                    "bench_sql_ipc measures query planning+execution+collect plus IPC encoding on the Rust side.",
                    "bench_sql_c_stream measures query planning+execution+collect plus Arrow C Stream export on the Rust side.",
                    "session_sql measures end-to-end Session.sql(...) using current export mode (usually 'auto').",
                    "session_sql_ipc / session_sql_c_stream measure end-to-end Session.sql(...) under forced modes.",
                    "Large targets can require high peak RAM (IPC bytes + decoded Table + intermediate buffers).",
                ],
            }

            def _decode_c_stream(capsule: object) -> pa.Table:
                class _Wrapper:
                    def __init__(self, c: object):
                        self._c = c

                    def __arrow_c_stream__(self, requested_schema=None) -> object:
                        return self._c

                reader = pa.RecordBatchReader.from_stream(_Wrapper(capsule))
                try:
                    return reader.read_all()
                finally:
                    reader.close()

            def _session_sql_forced(mode: str, sql: str) -> pa.Table:
                with _temp_env_var("TTF_SQL_EXPORT_MODE", mode):
                    return sess.sql(sql)

            for name, sql in queries:
                # Warmup: run both paths once to populate OS page cache and DataFusion internal caches.
                for _ in range(args.warmups):
                    _t, table = _timed(lambda: sess.sql(sql))
                    del table
                    _t, table = _timed(lambda: _session_sql_forced("ipc", sql))
                    del table
                    _t, table = _timed(lambda: _session_sql_forced("c_stream", sql))
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

                    _t, (capsule, _m) = _timed(
                        lambda: testing._bench_sql_c_stream(sess, sql)
                    )
                    _t, table = _timed(lambda: _decode_c_stream(capsule))
                    del table
                    del capsule
                    gc.collect()

                session_sql_times: list[float] = []
                session_sql_ipc_times: list[float] = []
                session_sql_c_stream_times: list[float] = []
                bench_sql_ipc_times: list[float] = []
                decode_only_times: list[float] = []
                bench_sql_c_stream_times: list[float] = []
                c_stream_decode_only_times: list[float] = []
                ipc_bytes_lens: list[int] = []
                arrow_mem_bytes: list[int] = []
                c_stream_arrow_mem_bytes: list[int] = []
                row_counts: list[int] = []
                batch_counts: list[int] = []
                c_stream_row_counts: list[int] = []
                c_stream_batch_counts: list[int] = []
                rust_total_ms: list[float] = []
                rust_plan_ms: list[float] = []
                rust_collect_ms: list[float] = []
                rust_ipc_encode_ms: list[float] = []
                rust_c_stream_export_ms: list[float] = []
                rust_c_stream_total_ms: list[float] = []
                rust_c_stream_plan_ms: list[float] = []
                rust_c_stream_collect_ms: list[float] = []

                for _ in range(args.runs):
                    t_sess, table = _timed(lambda: _session_sql_forced("ipc", sql))
                    session_sql_ipc_times.append(t_sess)
                    del table
                    gc.collect()

                for _ in range(args.runs):
                    t_sess, table = _timed(lambda: _session_sql_forced("c_stream", sql))
                    session_sql_c_stream_times.append(t_sess)
                    del table
                    gc.collect()

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

                for _ in range(args.runs):
                    t_bench, (capsule, m) = _timed(
                        lambda: testing._bench_sql_c_stream(sess, sql)
                    )
                    bench_sql_c_stream_times.append(t_bench)

                    t_decode, table = _timed(lambda: _decode_c_stream(capsule))
                    c_stream_decode_only_times.append(t_decode)

                    c_stream_arrow_mem_bytes.append(int(m["arrow_mem_bytes"]))
                    c_stream_row_counts.append(int(m["row_count"]))
                    c_stream_batch_counts.append(int(m["batch_count"]))
                    rust_c_stream_total_ms.append(float(m["total_ms"]))
                    rust_c_stream_plan_ms.append(float(m["plan_ms"]))
                    rust_c_stream_collect_ms.append(float(m["collect_ms"]))
                    rust_c_stream_export_ms.append(float(m["c_stream_export_ms"]))

                    del table
                    del capsule
                    gc.collect()

                out["results"].append(
                    {
                        "name": name,
                        "sql": sql,
                        "session_sql": _summarize_seconds(session_sql_times),
                        "session_sql_ipc": _summarize_seconds(session_sql_ipc_times),
                        "session_sql_c_stream": _summarize_seconds(
                            session_sql_c_stream_times
                        ),
                        "bench_sql_ipc": _summarize_seconds(bench_sql_ipc_times),
                        "decode_only": _summarize_seconds(decode_only_times),
                        "bench_sql_c_stream": _summarize_seconds(
                            bench_sql_c_stream_times
                        ),
                        "c_stream_decode_only": _summarize_seconds(
                            c_stream_decode_only_times
                        ),
                        "ipc_bytes_len": ipc_bytes_lens,
                        "arrow_mem_bytes": arrow_mem_bytes,
                        "c_stream_arrow_mem_bytes": c_stream_arrow_mem_bytes,
                        "ipc_to_arrow_ratio": [
                            (b / m) if m else None
                            for b, m in zip(ipc_bytes_lens, arrow_mem_bytes)
                        ],
                        "row_count": row_counts,
                        "batch_count": batch_counts,
                        "c_stream_row_count": c_stream_row_counts,
                        "c_stream_batch_count": c_stream_batch_counts,
                        "rust_ms": {
                            "total_ms": rust_total_ms,
                            "plan_ms": rust_plan_ms,
                            "collect_ms": rust_collect_ms,
                            "ipc_encode_ms": rust_ipc_encode_ms,
                        },
                        "rust_ms_c_stream": {
                            "total_ms": rust_c_stream_total_ms,
                            "plan_ms": rust_c_stream_plan_ms,
                            "collect_ms": rust_c_stream_collect_ms,
                            "c_stream_export_ms": rust_c_stream_export_ms,
                        },
                    }
                )

            payload = json.dumps(out, indent=2, sort_keys=False)
            if args.summary:
                _print_summary(out)
            if args.json:
                Path(args.json).write_text(payload, encoding="utf-8")
                if args.print_json:
                    print(payload)
            else:
                print(payload)
            return 0
    finally:
        if not args.no_gc_disable and gc_was_enabled:
            gc.enable()


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
