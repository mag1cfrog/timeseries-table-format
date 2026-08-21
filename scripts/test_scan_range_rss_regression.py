from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from scan_range_rss_regression import (
    GNU_TIME,
    MIB,
    REPO_ROOT,
    MAX_BATCH_BYTES,
    SCAN_BATCH_SIZE,
    parse_max_rss_bytes,
    require_bounded_rss,
)


RUNNER = REPO_ROOT / "scripts" / "scan_range_rss_regression.py"


def benchmark_binary() -> Path:
    subprocess.run(
        [
            "cargo",
            "build",
            "--locked",
            "-p",
            "timeseries-table-format",
            "--features",
            "cli",
            "--example",
            "scan_range_bench",
        ],
        cwd=REPO_ROOT,
        check=True,
    )
    metadata = json.loads(
        subprocess.check_output(
            ["cargo", "metadata", "--format-version", "1", "--no-deps"],
            cwd=REPO_ROOT,
            text=True,
        )
    )
    return (
        Path(metadata["target_directory"]) / "debug" / "examples" / "scan_range_bench"
    )


class RssHelperTests(unittest.TestCase):
    def test_parses_gnu_time_peak_rss_as_bytes(self) -> None:
        output = """\
Command being timed: "benchmark"
\tUser time (seconds): 1.23
\tMaximum resident set size (kbytes): 131072
\tExit status: 0
"""
        self.assertEqual(parse_max_rss_bytes(output), 128 * MIB)

    def test_rejects_missing_or_invalid_peak_rss(self) -> None:
        for output in (
            "User time (seconds): 1.23",
            "Maximum resident set size (kbytes): unknown",
            "Maximum resident set size (kbytes): 0",
            "Maximum resident set size (kbytes): -1",
        ):
            with self.subTest(output=output), self.assertRaises(ValueError):
                parse_max_rss_bytes(output)

    def test_rss_delta_allows_limit_and_rejects_one_byte_more(self) -> None:
        small = 64 * MIB
        limit = 128 * MIB

        self.assertEqual(require_bounded_rss(small, small + limit, limit), limit)
        with self.assertRaises(RuntimeError):
            require_bounded_rss(small, small + limit + 1, limit)

    def test_rss_delta_may_be_negative(self) -> None:
        self.assertEqual(require_bounded_rss(128 * MIB, 64 * MIB, 128 * MIB), -64 * MIB)


class ScanRangeRssRegressionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.benchmark = benchmark_binary()

    def runner_command(
        self,
        benchmark: Path,
        output: Path,
        rows_per_group: int = 16,
        payload_bytes: int = 32,
    ) -> list[str]:
        return [
            sys.executable,
            str(RUNNER),
            "--benchmark",
            str(benchmark),
            "--small-row-groups",
            "2",
            "--large-row-groups",
            "4",
            "--rows-per-group",
            str(rows_per_group),
            "--payload-bytes",
            str(payload_bytes),
            "--json-out",
            str(output),
        ]

    def test_small_process_smoke(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            table = root / "table"
            row_groups = 2
            rows_per_group = SCAN_BATCH_SIZE + 1
            preparation = json.loads(
                subprocess.check_output(
                    [
                        str(self.benchmark),
                        "prepare",
                        "--table",
                        str(table),
                        "--row-groups",
                        str(row_groups),
                        "--rows-per-group",
                        str(rows_per_group),
                        "--payload-bytes",
                        "32",
                    ],
                    cwd=REPO_ROOT,
                    text=True,
                )
            )
            scan = json.loads(
                subprocess.check_output(
                    [str(self.benchmark), "scan", "--table", str(table)],
                    cwd=REPO_ROOT,
                    text=True,
                )
            )

            self.assertEqual(preparation["mode"], "prepare")
            self.assertEqual(scan["mode"], "scan")
            self.assertEqual(preparation["row_group_count"], row_groups)
            self.assertEqual(scan["scan_batch_size"], SCAN_BATCH_SIZE)
            self.assertEqual(scan["returned_batch_count"], row_groups * 2)
            self.assertEqual(scan["returned_row_count"], row_groups * rows_per_group)
            self.assertGreater(scan["time_to_first_batch_ns"], 0)
            self.assertLess(scan["time_to_first_batch_ns"], scan["total_elapsed_ns"])
            self.assertNotEqual(preparation["process_id"], scan["process_id"])

    @unittest.skipUnless(
        sys.platform.startswith("linux") and GNU_TIME.is_file(),
        "GNU time is unavailable",
    )
    def test_rejects_oversized_row_group_before_running_benchmark(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "summary.json"
            result = subprocess.run(
                self.runner_command(
                    self.benchmark,
                    output,
                    payload_bytes=MAX_BATCH_BYTES,
                ),
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(result.returncode, 0)
            self.assertIn("requested row-group payload exceeds", result.stderr)
            self.assertFalse(output.exists())

    @unittest.skipUnless(
        sys.platform.startswith("linux") and GNU_TIME.is_file(),
        "GNU time is unavailable",
    )
    def test_cleans_up_after_scan_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            scratch = root / "scratch"
            scratch.mkdir()
            fake = root / "scan_range_bench"
            fake.write_text(
                """#!/usr/bin/env python3
import json
import os
import sys

if sys.argv[1] == "scan":
    raise SystemExit(9)

def argument(name):
    return int(sys.argv[sys.argv.index(name) + 1])

groups = argument("--row-groups")
rows = argument("--rows-per-group")
payload = argument("--payload-bytes")
total = groups * rows
print(json.dumps({
    "mode": "prepare",
    "segment_file_bytes": total * payload,
    "row_group_count": groups,
    "rows_per_row_group": rows,
    "total_rows": total,
    "payload_bytes_per_row": payload,
    "max_generated_batch_memory_bytes": rows * payload,
    "max_row_group_bytes": rows * payload,
    "process_id": os.getpid(),
}))
""",
                encoding="utf-8",
            )
            fake.chmod(0o755)
            env = os.environ.copy()
            env["TMPDIR"] = str(scratch)

            result = subprocess.run(
                self.runner_command(fake, root / "summary.json"),
                cwd=REPO_ROOT,
                env=env,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(result.returncode, 0)
            self.assertIn("scan RSS regression failed", result.stderr)
            self.assertEqual(list(scratch.iterdir()), [])


if __name__ == "__main__":
    unittest.main()
