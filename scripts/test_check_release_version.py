from __future__ import annotations

import unittest

from check_release_version import validate_tag


class ValidateTagTests(unittest.TestCase):
    def test_requires_exact_canonical_tag(self) -> None:
        validate_tag("timeseries-table-format-v0.3.0", "0.3.0")

        for tag in (
            "",
            "v0.3.0",
            "timeseries-table-cli-v0.3.0",
            "timeseries-table-format-v0.3.1",
        ):
            with self.subTest(tag=tag), self.assertRaises(RuntimeError):
                validate_tag(tag, "0.3.0")


if __name__ == "__main__":
    unittest.main()
