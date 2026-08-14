#!/usr/bin/env python3
from __future__ import annotations

import argparse
import importlib.util
import json
import os
import platform
import shlex
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path


MIB = 1024 * 1024
DEFAULT_MAX_RSS_DELTA_BYTES = 128 * MIB
MAX_RSS_LABEL = "Maximum resident set size (kbytes)"
DEFAULT_ROW_COUNT = 1024
DEFAULT_ROW_GROUP_COUNT = 32
REPO_ROOT = Path(__file__).resolve().parents[1]
GNU_TIME = Path("/usr/bin/time")


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


def require_linux_dependencies() -> None:
    if not sys.platform.startswith("linux"):
        raise RuntimeError("append RSS regression requires Linux")
    if not GNU_TIME.is_file():
        raise RuntimeError("append RSS regression requires /usr/bin/time")
    if importlib.util.find_spec("pyarrow") is None:
        raise RuntimeError(
            "PyArrow is required; run `uv sync --project "
            "crates/timeseries-table-python --no-install-project`, then use "
            "`crates/timeseries-table-python/.venv/bin/python`"
        )


def resolve_tstable(explicit_path: Path | None) -> tuple[Path, str]:
    if explicit_path is not None:
        binary = explicit_path.expanduser().resolve()
        profile = (
            binary.parent.name
            if binary.parent.name in {"debug", "release"}
            else "external"
        )
    else:
        build_command = [
            "cargo",
            "build",
            "--locked",
            "--release",
            "-p",
            "timeseries-table-format",
            "--features",
            "cli",
            "--bin",
            "tstable",
        ]
        subprocess.run(build_command, cwd=REPO_ROOT, check=True)
        metadata = json.loads(
            subprocess.check_output(
                ["cargo", "metadata", "--format-version", "1", "--no-deps"],
                cwd=REPO_ROOT,
                text=True,
            )
        )
        binary = Path(metadata["target_directory"]) / "release" / "tstable"
        profile = "release"

    if not binary.is_file() or not os.access(binary, os.X_OK):
        raise RuntimeError(f"tstable binary is not executable: {binary}")
    return binary, profile


def run_checked(command: list[str], env: dict[str, str] | None = None) -> None:
    result = subprocess.run(command, text=True, capture_output=True, env=env)
    if result.returncode:
        detail = result.stderr.strip() or result.stdout.strip() or "no command output"
        raise RuntimeError(f"command failed ({shlex.join(command)}): {detail}")


def measure_append(
    tstable: Path,
    input_path: Path,
    table_root: Path,
    timing_path: Path,
) -> dict[str, object]:
    create_command = [
        str(tstable),
        "create",
        "--table",
        str(table_root),
        "--time-column",
        "ts",
        "--bucket",
        "1s",
        "--entity",
        "entity",
    ]
    run_checked(create_command)

    append_command = [
        str(GNU_TIME),
        "-v",
        "-o",
        str(timing_path),
        str(tstable),
        "append",
        "--table",
        str(table_root),
        "--parquet",
        str(input_path),
    ]
    env = os.environ.copy()
    env["LC_ALL"] = "C"
    run_checked(append_command, env)

    staged_path = table_root / "data" / input_path.name
    if not staged_path.is_file():
        raise RuntimeError(f"append did not stage the external input at {staged_path}")

    return {
        "input_file_bytes": input_path.stat().st_size,
        "peak_rss_bytes": parse_max_rss_bytes(timing_path.read_text(encoding="utf-8")),
        "command": shlex.join(append_command),
    }


def available_parallelism() -> int:
    if hasattr(os, "sched_getaffinity"):
        return len(os.sched_getaffinity(0))
    return os.cpu_count() or 1


def write_summary(summary: dict[str, object], output_path: Path | None) -> None:
    encoded = json.dumps(summary, indent=2, sort_keys=True) + "\n"
    if output_path is not None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(encoded, encoding="utf-8")
    sys.stdout.write(encoded)


def positive_int(raw: str) -> int:
    value = int(raw)
    if value <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return value


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Prove tstable append RSS is independent of unprojected payload size."
    )
    parser.add_argument("--tstable", type=Path, help="existing tstable binary")
    parser.add_argument("--small-mib", type=positive_int, default=128)
    parser.add_argument("--large-mib", type=positive_int, default=1024)
    parser.add_argument("--max-rss-delta-mib", type=positive_int, default=128)
    parser.add_argument("--row-count", type=positive_int, default=DEFAULT_ROW_COUNT)
    parser.add_argument(
        "--row-groups", type=positive_int, default=DEFAULT_ROW_GROUP_COUNT
    )
    parser.add_argument(
        "--json-out", type=Path, help="also write the JSON summary here"
    )
    parser.add_argument(
        "--keep-artifacts",
        action="store_true",
        help="retain generated files and tables for inspection",
    )
    return parser.parse_args()


def run_benchmark(args: argparse.Namespace) -> None:
    require_linux_dependencies()
    if args.small_mib >= args.large_mib:
        raise ValueError("--small-mib must be less than --large-mib")
    if args.row_count % args.row_groups:
        raise ValueError("--row-count must be divisible by --row-groups")

    tstable, build_profile = resolve_tstable(args.tstable)
    work_dir = Path(tempfile.mkdtemp(prefix="tstable-append-rss-"))
    try:
        print(f"Generating fixtures under {work_dir}", file=sys.stderr)
        small_path, large_path = generate_fixture_pair(
            work_dir / "inputs",
            args.small_mib * MIB,
            args.large_mib * MIB,
            args.row_count,
            args.row_groups,
        )

        measurements = {}
        for name, input_path in (("small", small_path), ("large", large_path)):
            print(f"Measuring {name} input", file=sys.stderr)
            measurements[name] = measure_append(
                tstable,
                input_path,
                work_dir / "tables" / name,
                work_dir / f"{name}.time.txt",
            )

        small_rss = int(measurements["small"]["peak_rss_bytes"])
        large_rss = int(measurements["large"]["peak_rss_bytes"])
        max_delta_bytes = args.max_rss_delta_mib * MIB
        delta_bytes = large_rss - small_rss
        passed = delta_bytes <= max_delta_bytes
        summary: dict[str, object] = {
            "schema_version": 1,
            "operating_system": platform.platform(),
            "build_profile": build_profile,
            "tstable_binary": str(tstable),
            "worker_configuration": {
                "policy": "library_auto",
                "available_parallelism": available_parallelism(),
                "tokio_worker_threads": os.environ.get("TOKIO_WORKER_THREADS", "auto"),
                "row_groups": args.row_groups,
            },
            "row_count": args.row_count,
            "small_target_bytes": args.small_mib * MIB,
            "large_target_bytes": args.large_mib * MIB,
            "measurements": measurements,
            "rss_delta_bytes": delta_bytes,
            "max_rss_delta_bytes": max_delta_bytes,
            "passed": passed,
            "artifacts_directory": str(work_dir) if args.keep_artifacts else None,
        }
        write_summary(summary, args.json_out)
        require_bounded_rss(small_rss, large_rss, max_delta_bytes)
    finally:
        if args.keep_artifacts:
            print(f"Kept benchmark artifacts at {work_dir}", file=sys.stderr)
        else:
            shutil.rmtree(work_dir)


def main() -> int:
    args = parse_args()
    try:
        run_benchmark(args)
    except (OSError, RuntimeError, ValueError, subprocess.CalledProcessError) as error:
        sys.stderr.write(f"append RSS regression failed: {error}\n")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
