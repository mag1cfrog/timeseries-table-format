#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import platform
import shlex
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import TypedDict

from append_rss_regression import (
    GNU_TIME,
    MIB,
    REPO_ROOT,
    parse_max_rss_bytes,
    positive_int,
    require_bounded_rss,
    write_summary,
)


DEFAULT_SMALL_ROW_GROUPS = 32
DEFAULT_LARGE_ROW_GROUPS = 256
DEFAULT_ROWS_PER_GROUP = 4_096
DEFAULT_PAYLOAD_BYTES = 1_024
SCAN_BATCH_SIZE = 8_192
MAX_BATCH_BYTES = 8 * MIB
MAX_RSS_DELTA_BYTES = 64 * MIB
FILE_SIZE_TOLERANCE_PERCENT = 10
MIN_FILE_SIZE_TOLERANCE_BYTES = 64 * 1024


class CaseMeasurement(TypedDict):
    preparation: dict[str, object]
    scan: dict[str, object]
    peak_rss_bytes: int
    command: str


def require_linux_dependencies() -> None:
    if not sys.platform.startswith("linux"):
        raise RuntimeError("scan RSS regression requires Linux")
    if not GNU_TIME.is_file():
        raise RuntimeError("scan RSS regression requires /usr/bin/time")


def repository_state() -> tuple[str, bool]:
    commit_sha = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=REPO_ROOT, text=True
    ).strip()
    status = subprocess.check_output(
        ["git", "status", "--porcelain"], cwd=REPO_ROOT, text=True
    )
    return commit_sha, bool(status.strip())


def resolve_benchmark(explicit_path: Path | None) -> tuple[Path, str, str]:
    if explicit_path is not None:
        binary = explicit_path.expanduser().resolve()
        profile = (
            binary.parent.parent.name
            if binary.parent.name == "examples"
            and binary.parent.parent.name in {"debug", "release"}
            else "external"
        )
        build_command = f"prebuilt binary: {binary}"
    else:
        command = [
            "cargo",
            "build",
            "--locked",
            "--release",
            "-p",
            "timeseries-table-format",
            "--features",
            "cli",
            "--example",
            "scan_range_bench",
        ]
        subprocess.run(command, cwd=REPO_ROOT, check=True)
        metadata = json.loads(
            subprocess.check_output(
                ["cargo", "metadata", "--format-version", "1", "--no-deps"],
                cwd=REPO_ROOT,
                text=True,
            )
        )
        binary = (
            Path(metadata["target_directory"])
            / "release"
            / "examples"
            / "scan_range_bench"
        )
        profile = "release"
        build_command = shlex.join(command)

    if not binary.is_file() or not os.access(binary, os.X_OK):
        raise RuntimeError(f"benchmark binary is not executable: {binary}")
    return binary, profile, build_command


def run_json(
    command: list[str], env: dict[str, str] | None = None
) -> dict[str, object]:
    result = subprocess.run(command, text=True, capture_output=True, env=env)
    if result.returncode:
        detail = result.stderr.strip() or result.stdout.strip() or "no command output"
        raise RuntimeError(f"command failed ({shlex.join(command)}): {detail}")
    try:
        report = json.loads(result.stdout)
    except json.JSONDecodeError as error:
        raise ValueError(f"malformed JSON from {shlex.join(command)}") from error
    if not isinstance(report, dict):
        raise ValueError(f"expected JSON object from {shlex.join(command)}")
    return report


def integer_field(report: dict[str, object], key: str) -> int:
    value = report.get(key)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"invalid {key}: {value!r}")
    return value


def require_mode(report: dict[str, object], mode: str) -> None:
    if report.get("mode") != mode:
        raise ValueError(f"expected {mode} report, found {report.get('mode')!r}")


def require_target_size(actual_bytes: int, target_bytes: int) -> None:
    allowance = max(
        target_bytes * FILE_SIZE_TOLERANCE_PERCENT // 100,
        MIN_FILE_SIZE_TOLERANCE_BYTES,
    )
    if not target_bytes - allowance <= actual_bytes <= target_bytes + allowance:
        raise RuntimeError(
            f"segment size {actual_bytes} is outside target {target_bytes} +/- {allowance} bytes"
        )


def measure_case(
    binary: Path,
    table_root: Path,
    timing_path: Path,
    row_groups: int,
    rows_per_group: int,
    payload_bytes: int,
) -> CaseMeasurement:
    prepare_command = [
        str(binary),
        "prepare",
        "--table",
        str(table_root),
        "--row-groups",
        str(row_groups),
        "--rows-per-group",
        str(rows_per_group),
        "--payload-bytes",
        str(payload_bytes),
    ]
    preparation = run_json(prepare_command)
    require_mode(preparation, "prepare")

    expected_rows = row_groups * rows_per_group
    expected_payload_bytes = expected_rows * payload_bytes
    expected_preparation = {
        "row_group_count": row_groups,
        "rows_per_row_group": rows_per_group,
        "total_rows": expected_rows,
        "payload_bytes_per_row": payload_bytes,
    }
    for key, expected in expected_preparation.items():
        actual = integer_field(preparation, key)
        if actual != expected:
            raise RuntimeError(f"expected {key}={expected}, found {actual}")
    require_target_size(
        integer_field(preparation, "segment_file_bytes"), expected_payload_bytes
    )
    if integer_field(preparation, "max_generated_batch_memory_bytes") > MAX_BATCH_BYTES:
        raise RuntimeError("generated batch exceeded the 8 MiB memory limit")
    if integer_field(preparation, "max_row_group_bytes") > MAX_BATCH_BYTES:
        raise RuntimeError("Parquet row group exceeded the 8 MiB size limit")

    scan_command = [
        str(GNU_TIME),
        "-v",
        "-o",
        str(timing_path),
        str(binary),
        "scan",
        "--table",
        str(table_root),
    ]
    env = os.environ.copy()
    env["LC_ALL"] = "C"
    scan = run_json(scan_command, env)
    require_mode(scan, "scan")

    expected_scan = {
        "scan_batch_size": SCAN_BATCH_SIZE,
        "returned_batch_count": row_groups,
        "returned_row_count": expected_rows,
    }
    for key, expected in expected_scan.items():
        actual = integer_field(scan, key)
        if actual != expected:
            raise RuntimeError(f"expected {key}={expected}, found {actual}")
    if integer_field(scan, "max_returned_batch_memory_bytes") > MAX_BATCH_BYTES:
        raise RuntimeError("returned batch exceeded the 8 MiB memory limit")
    first_batch_ns = integer_field(scan, "time_to_first_batch_ns")
    total_elapsed_ns = integer_field(scan, "total_elapsed_ns")
    if first_batch_ns <= 0 or first_batch_ns >= total_elapsed_ns:
        raise RuntimeError("first batch was not observed before scan completion")
    if integer_field(preparation, "process_id") == integer_field(scan, "process_id"):
        raise RuntimeError("preparation and scan ran in the same process")

    return {
        "preparation": preparation,
        "scan": scan,
        "peak_rss_bytes": parse_max_rss_bytes(timing_path.read_text(encoding="utf-8")),
        "command": shlex.join(scan_command),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Prove core scan_range RSS is bounded as segment row groups increase."
    )
    parser.add_argument(
        "--benchmark", type=Path, help="existing scan_range_bench binary"
    )
    parser.add_argument(
        "--small-row-groups", type=positive_int, default=DEFAULT_SMALL_ROW_GROUPS
    )
    parser.add_argument(
        "--large-row-groups", type=positive_int, default=DEFAULT_LARGE_ROW_GROUPS
    )
    parser.add_argument(
        "--rows-per-group", type=positive_int, default=DEFAULT_ROWS_PER_GROUP
    )
    parser.add_argument(
        "--payload-bytes", type=positive_int, default=DEFAULT_PAYLOAD_BYTES
    )
    parser.add_argument(
        "--json-out", type=Path, help="also write the JSON summary here"
    )
    parser.add_argument(
        "--keep-data",
        action="store_true",
        help="retain generated tables and timing files for inspection",
    )
    return parser.parse_args()


def run_benchmark(args: argparse.Namespace) -> None:
    require_linux_dependencies()
    if args.large_row_groups < args.small_row_groups * 2:
        raise ValueError("--large-row-groups must be at least twice --small-row-groups")
    if args.rows_per_group > SCAN_BATCH_SIZE:
        raise ValueError(f"--rows-per-group must not exceed {SCAN_BATCH_SIZE}")
    if args.rows_per_group * args.payload_bytes > MAX_BATCH_BYTES:
        raise ValueError("requested row-group payload exceeds the 8 MiB limit")

    commit_sha, worktree_dirty = repository_state()
    benchmark_provenance = (
        "prebuilt" if args.benchmark is not None else "repository_release_build"
    )
    if benchmark_provenance == "repository_release_build" and worktree_dirty:
        raise RuntimeError("default recorded comparison requires a clean Git worktree")

    binary, build_profile, build_command = resolve_benchmark(args.benchmark)
    work_dir = Path(tempfile.mkdtemp(prefix="tstable-scan-rss-"))
    try:
        measurements: dict[str, CaseMeasurement] = {}
        for name, row_groups in (
            ("small", args.small_row_groups),
            ("large", args.large_row_groups),
        ):
            print(f"Preparing and measuring {name} scan", file=sys.stderr)
            measurements[name] = measure_case(
                binary,
                work_dir / "tables" / name,
                work_dir / f"{name}.time.txt",
                row_groups,
                args.rows_per_group,
                args.payload_bytes,
            )

        small_rss = measurements["small"]["peak_rss_bytes"]
        large_rss = measurements["large"]["peak_rss_bytes"]
        delta_bytes = large_rss - small_rss
        passed = delta_bytes <= MAX_RSS_DELTA_BYTES
        if benchmark_provenance == "repository_release_build":
            final_commit_sha, final_worktree_dirty = repository_state()
            if final_commit_sha != commit_sha or final_worktree_dirty:
                raise RuntimeError("Git worktree changed during recorded comparison")
        summary: dict[str, object] = {
            "schema_version": 1,
            "git_commit_sha": commit_sha,
            "git_worktree_dirty": worktree_dirty,
            "operating_system": platform.platform(),
            "architecture": platform.machine(),
            "benchmark_provenance": benchmark_provenance,
            "build_profile": build_profile,
            "build_command": build_command,
            "benchmark_binary": str(binary),
            "workload": {
                "small_row_groups": args.small_row_groups,
                "large_row_groups": args.large_row_groups,
                "rows_per_row_group": args.rows_per_group,
                "payload_bytes_per_row": args.payload_bytes,
                "scan_batch_size": SCAN_BATCH_SIZE,
                "max_batch_bytes": MAX_BATCH_BYTES,
                "file_size_tolerance_percent": FILE_SIZE_TOLERANCE_PERCENT,
            },
            "measurements": measurements,
            "rss_delta_bytes": delta_bytes,
            "max_rss_delta_bytes": MAX_RSS_DELTA_BYTES,
            "passed": passed,
            "artifacts_directory": str(work_dir) if args.keep_data else None,
        }
        write_summary(summary, args.json_out)
        require_bounded_rss(small_rss, large_rss, MAX_RSS_DELTA_BYTES)
    finally:
        if args.keep_data:
            print(f"Kept benchmark data at {work_dir}", file=sys.stderr)
        else:
            shutil.rmtree(work_dir)


def main() -> int:
    args = parse_args()
    try:
        run_benchmark(args)
    except (
        KeyError,
        OSError,
        RuntimeError,
        TypeError,
        ValueError,
        subprocess.CalledProcessError,
    ) as error:
        sys.stderr.write(f"scan RSS regression failed: {error}\n")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
