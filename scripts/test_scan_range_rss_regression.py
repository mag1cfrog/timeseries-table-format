from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from append_rss_regression import GNU_TIME, REPO_ROOT


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


@unittest.skipUnless(
    sys.platform.startswith("linux") and GNU_TIME.is_file(),
    "GNU time is unavailable",
)
class ScanRangeRssRegressionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.benchmark = benchmark_binary()

    def runner_command(self, benchmark: Path, output: Path) -> list[str]:
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
            "16",
            "--payload-bytes",
            "32",
            "--json-out",
            str(output),
        ]

    def test_small_process_smoke(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            scratch = root / "scratch"
            scratch.mkdir()
            output = root / "summary.json"
            env = os.environ.copy()
            env["TMPDIR"] = str(scratch)

            result = subprocess.run(
                self.runner_command(self.benchmark, output),
                cwd=REPO_ROOT,
                env=env,
                text=True,
                capture_output=True,
                check=True,
            )
            summary = json.loads(result.stdout)

            self.assertEqual(json.loads(output.read_text(encoding="utf-8")), summary)
            self.assertTrue(summary["passed"])
            for name, row_groups in (("small", 2), ("large", 4)):
                measurement = summary["measurements"][name]
                preparation = measurement["preparation"]
                scan = measurement["scan"]
                self.assertEqual(preparation["row_group_count"], row_groups)
                self.assertEqual(scan["returned_batch_count"], row_groups)
                self.assertEqual(scan["returned_row_count"], row_groups * 16)
                self.assertGreater(scan["time_to_first_batch_ns"], 0)
                self.assertLess(
                    scan["time_to_first_batch_ns"], scan["total_elapsed_ns"]
                )
                self.assertNotEqual(preparation["process_id"], scan["process_id"])
            self.assertEqual(list(scratch.iterdir()), [])

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
