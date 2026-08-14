from __future__ import annotations

import unittest

from append_rss_regression import MIB, parse_max_rss_bytes, require_bounded_rss


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


if __name__ == "__main__":
    unittest.main()
