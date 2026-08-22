from __future__ import annotations

import argparse
import copy
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from typing import cast

from append_pipeline_benchmark import (
    GNU_TIME,
    REPO_ROOT,
    WORKLOADS,
    calculate_median,
    measured_execution_order,
    parse_driver_stdout,
    parse_max_rss_bytes,
    require_equivalent_results,
    run_process,
    sample_count,
    validate_driver_report,
)

RUNNER = REPO_ROOT / "scripts" / "append_pipeline_benchmark.py"


def valid_report(mode: str = "path-first") -> dict[str, object]:
    workload = dict(WORKLOADS["smoke"])
    path_first = mode == "path-first"
    segment_bytes = 100
    external_bytes = 50 if path_first else None
    return {
        "schema_version": 1,
        "mode": mode,
        "process_id": 123,
        "table_path": "/tmp/table",
        "external_parquet_path": "/tmp/source.parquet" if path_first else None,
        "workload": workload,
        "table_definition": {
            "index_column": "ts",
            "index_type": "int64",
            "bucket_width": 1,
            "entity_columns": [],
        },
        "writer_properties": {
            "compression": "SNAPPY",
            "data_page_size_bytes": 1_048_576,
            "dictionary_enabled": True,
            "max_row_group_rows": 1_048_576,
            "statistics": "Page",
            "write_batch_rows": 1_024,
            "writer_version": "PARQUET_1_0",
        },
        "timing": {
            "external_parquet_generation_ns": 10 if path_first else None,
            "path_append_copy_commit_ns": 20 if path_first else None,
            "streaming_append_ns": None if path_first else 30,
            "end_to_end_pipeline_ns": 30,
        },
        "artifacts": {
            "external_source_parquet_bytes": external_bytes,
            "table_owned_segment_bytes": segment_bytes,
            "total_retained_ingestion_bytes": segment_bytes + (external_bytes or 0),
        },
        "committed_version": 2,
        "validation": {
            "column_checksums": {
                "ts": "a" * 64,
                "sequence": "b" * 64,
                "payload": "c" * 64,
            },
            "coverage_ratio": 1.0,
            "index_max": workload["row_count"] - 1,
            "index_min": 0,
            "ordered_full_scan_matches_generated": True,
            "row_count": workload["row_count"],
            "schema_matches_generated": True,
            "segment_file_bytes": segment_bytes,
            "segment_path": "data/segment.parquet",
            "segment_row_group_count": 2,
        },
    }


def write_fake_benchmark(path: Path) -> None:
    path.write_text(
        """#!/usr/bin/env python3
import json
import os
import pathlib
import sys

def argument(name):
    return sys.argv[sys.argv.index(name) + 1]

mode = argument("--mode")
table = pathlib.Path(argument("--table"))
table.mkdir(parents=True)
(table / "segment.parquet").write_bytes(b"x" * 100)
external = None
if mode == "path-first":
    external = pathlib.Path(argument("--external-parquet"))
    external.parent.mkdir(parents=True)
    external.write_bytes(b"x" * 50)
row_count = int(argument("--row-count"))
workload = {
    "row_count": row_count,
    "batch_rows": int(argument("--batch-rows")),
    "payload_bytes_per_row": int(argument("--payload-bytes")),
    "seed": int(argument("--seed")),
}
path_first = mode == "path-first"
print(json.dumps({
    "schema_version": 1,
    "mode": mode,
    "process_id": os.getpid(),
    "table_path": str(table),
    "external_parquet_path": str(external) if external else None,
    "workload": workload,
    "table_definition": {
        "index_column": "ts",
        "index_type": "int64",
        "bucket_width": 1,
        "entity_columns": [],
    },
    "writer_properties": {
        "compression": "SNAPPY",
        "data_page_size_bytes": 1048576,
        "dictionary_enabled": True,
        "max_row_group_rows": 1048576,
        "statistics": "Page",
        "write_batch_rows": 1024,
        "writer_version": "PARQUET_1_0",
    },
    "timing": {
        "external_parquet_generation_ns": 10 if path_first else None,
        "path_append_copy_commit_ns": 20 if path_first else None,
        "streaming_append_ns": None if path_first else 30,
        "end_to_end_pipeline_ns": 30,
    },
    "artifacts": {
        "external_source_parquet_bytes": 50 if path_first else None,
        "table_owned_segment_bytes": 100,
        "total_retained_ingestion_bytes": 150 if path_first else 100,
    },
    "committed_version": 2,
    "validation": {
        "column_checksums": {
            "ts": "a" * 64,
            "sequence": "b" * 64,
            "payload": "c" * 64,
        },
        "coverage_ratio": 1.0,
        "index_max": row_count - 1,
        "index_min": 0,
        "ordered_full_scan_matches_generated": True,
        "row_count": row_count,
        "schema_matches_generated": True,
        "segment_file_bytes": 100,
        "segment_path": "segment.parquet",
        "segment_row_group_count": 2,
    },
}))
""",
        encoding="utf-8",
    )
    path.chmod(0o755)


class AppendPipelineHelperTests(unittest.TestCase):
    def test_parses_rss_and_rejects_invalid_output(self) -> None:
        self.assertEqual(
            parse_max_rss_bytes("Maximum resident set size (kbytes): 2048"),
            2 * 1024 * 1024,
        )
        for output in (
            "no RSS here",
            "Maximum resident set size (kbytes): nope",
            "Maximum resident set size (kbytes): 0",
        ):
            with self.subTest(output=output), self.assertRaises(ValueError):
                parse_max_rss_bytes(output)

    def test_calculates_odd_and_even_medians(self) -> None:
        self.assertEqual(calculate_median([9, 1, 5]), 5)
        self.assertEqual(calculate_median([9, 1, 5, 3]), 4.0)
        with self.assertRaises(ValueError):
            calculate_median([])

    def test_alternates_measured_mode_order(self) -> None:
        self.assertEqual(
            measured_execution_order(3),
            [
                ["path-first", "streaming"],
                ["streaming", "path-first"],
                ["path-first", "streaming"],
            ],
        )
        self.assertEqual(sample_count("1"), 1)
        self.assertEqual(sample_count("3"), 3)
        with self.assertRaises(argparse.ArgumentTypeError):
            sample_count("2")

    def test_validates_required_driver_json_fields(self) -> None:
        report = valid_report()
        validate_driver_report(report, "path-first", WORKLOADS["smoke"])

        del report["timing"]
        with self.assertRaisesRegex(ValueError, "timing"):
            validate_driver_report(report, "path-first", WORKLOADS["smoke"])
        with self.assertRaisesRegex(ValueError, "malformed driver JSON"):
            parse_driver_stdout("not json", ["benchmark"])

    def test_rejects_mismatched_parameters_and_validation(self) -> None:
        path_first = valid_report("path-first")
        streaming = valid_report("streaming")
        require_equivalent_results(path_first, streaming)

        mismatched_parameters = copy.deepcopy(streaming)
        mismatched_parameters["writer_properties"] = {"compression": "different"}
        with self.assertRaisesRegex(RuntimeError, "logical results differ"):
            require_equivalent_results(path_first, mismatched_parameters)

        mismatched_validation = copy.deepcopy(streaming)
        validation = cast(dict[str, object], mismatched_validation["validation"])
        checksums = cast(dict[str, object], validation["column_checksums"])
        checksums["payload"] = "d" * 64
        with self.assertRaisesRegex(RuntimeError, "logical results differ"):
            require_equivalent_results(path_first, mismatched_validation)

    def test_propagates_subprocess_failure(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "command failed.*deliberate failure"):
            run_process(
                [
                    sys.executable,
                    "-c",
                    "import sys; print('deliberate failure', file=sys.stderr); sys.exit(7)",
                ],
                os.environ.copy(),
            )


@unittest.skipUnless(
    sys.platform.startswith("linux") and GNU_TIME.is_file(),
    "GNU time is unavailable",
)
class AppendPipelineRunnerTests(unittest.TestCase):
    def run_runner(
        self, benchmark: Path, scratch: Path, output: Path, *, keep_data: bool
    ) -> subprocess.CompletedProcess[str]:
        command = [
            sys.executable,
            str(RUNNER),
            "--benchmark",
            str(benchmark),
            "--workload",
            "smoke",
            "--samples",
            "1",
            "--json-out",
            str(output),
        ]
        if keep_data:
            command.append("--keep-data")
        env = os.environ.copy()
        env["TMPDIR"] = str(scratch)
        return subprocess.run(
            command,
            cwd=REPO_ROOT,
            env=env,
            text=True,
            capture_output=True,
        )

    def test_cleans_or_keeps_each_fresh_invocation_directory(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            fake = root / "append_pipeline_bench"
            write_fake_benchmark(fake)

            cleanup_scratch = root / "cleanup-scratch"
            cleanup_scratch.mkdir()
            cleanup_output = root / "cleanup.json"
            cleaned = self.run_runner(
                fake, cleanup_scratch, cleanup_output, keep_data=False
            )
            self.assertEqual(cleaned.returncode, 0, cleaned.stderr)
            self.assertIsNone(
                json.loads(cleanup_output.read_text())["artifacts_directory"]
            )
            self.assertEqual(list(cleanup_scratch.iterdir()), [])

            keep_scratch = root / "keep-scratch"
            keep_scratch.mkdir()
            keep_output = root / "keep.json"
            kept = self.run_runner(fake, keep_scratch, keep_output, keep_data=True)
            self.assertEqual(kept.returncode, 0, kept.stderr)
            report = json.loads(keep_output.read_text())
            artifacts = Path(report["artifacts_directory"])
            self.assertTrue(artifacts.is_dir())
            self.assertEqual(len(report["warmups"]), 2)
            self.assertEqual(len(report["measured_samples"]), 2)
            self.assertTrue(report["validation"]["all_mode_pairs_equivalent"])


if __name__ == "__main__":
    unittest.main()
