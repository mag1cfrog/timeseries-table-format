from __future__ import annotations

import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path

from append_rss_regression import (
    GNU_TIME,
    MIB,
    generate_fixture_pair,
    measure_append,
    parse_max_rss_bytes,
    require_bounded_rss,
)


class AppendRssRegressionTests(unittest.TestCase):
    def test_parses_gnu_time_peak_rss_as_bytes(self) -> None:
        output = """\
Command being timed: "tstable append"
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
        self.assertEqual(require_bounded_rss(128 * MIB, 64 * MIB), -64 * MIB)

    @unittest.skipUnless(
        sys.platform.startswith("linux") and GNU_TIME.is_file(),
        "GNU time is unavailable",
    )
    def test_measure_append_times_external_staging(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            fake_tstable = root / "tstable"
            fake_tstable.write_text(
                """#!/usr/bin/env python3
import shutil
import sys
from pathlib import Path

table = Path(sys.argv[sys.argv.index("--table") + 1])
if sys.argv[1] == "create":
    table.mkdir(parents=True)
elif sys.argv[1] == "append":
    source = Path(sys.argv[sys.argv.index("--parquet") + 1])
    destination = table / "data" / source.name
    destination.parent.mkdir(parents=True)
    shutil.copyfile(source, destination)
else:
    raise SystemExit(2)
""",
                encoding="utf-8",
            )
            fake_tstable.chmod(0o755)
            input_path = root / "outside" / "fixture.parquet"
            input_path.parent.mkdir()
            input_path.write_bytes(b"parquet")

            result = measure_append(
                fake_tstable,
                input_path,
                root / "table",
                root / "time.txt",
            )

            self.assertEqual(result["input_file_bytes"], len(b"parquet"))
            self.assertGreater(result["peak_rss_bytes"], 0)
            self.assertIn(" append ", result["command"])

    @unittest.skipUnless(
        importlib.util.find_spec("pyarrow"), "PyArrow is not installed"
    )
    def test_generates_comparable_uncompressed_parquet_fixtures(self) -> None:
        import pyarrow.parquet as pq

        with tempfile.TemporaryDirectory() as tmp:
            small_path, large_path = generate_fixture_pair(
                Path(tmp),
                small_target_bytes=1 * MIB,
                large_target_bytes=2 * MIB,
                row_count=64,
                row_group_count=4,
            )

            small = pq.ParquetFile(small_path)
            large = pq.ParquetFile(large_path)
            self.assertEqual(small.schema_arrow, large.schema_arrow)
            self.assertEqual(small.metadata.num_rows, large.metadata.num_rows)
            self.assertEqual(small.metadata.num_row_groups, 4)
            self.assertEqual(large.metadata.num_row_groups, 4)
            self.assertTrue(
                small.read(columns=["ts", "entity"]).equals(
                    large.read(columns=["ts", "entity"])
                )
            )

            for parquet_file in (small, large):
                payload_column = parquet_file.metadata.row_group(0).column(2)
                self.assertEqual(payload_column.compression, "UNCOMPRESSED")
                self.assertNotIn("RLE_DICTIONARY", payload_column.encodings)
                self.assertNotIn("PLAIN_DICTIONARY", payload_column.encodings)
                self.assertIsNone(payload_column.statistics)

            self.assertLess(abs(small_path.stat().st_size - 1 * MIB), 64 * 1024)
            self.assertLess(abs(large_path.stat().st_size - 2 * MIB), 64 * 1024)


if __name__ == "__main__":
    unittest.main()
