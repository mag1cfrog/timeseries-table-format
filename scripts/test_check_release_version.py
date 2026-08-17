from __future__ import annotations

import unittest
from unittest.mock import Mock, patch

from check_release_version import (
    parse_workspace_version,
    tag_matches_head_tree,
    validate_tag,
)


class ValidateTagTests(unittest.TestCase):
    def test_reads_only_workspace_package_version(self) -> None:
        manifest = """\
[package]
version = "9.9.9"

[workspace.package]
version = "0.3.0" # canonical version
"""
        self.assertEqual(parse_workspace_version(manifest), "0.3.0")

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

    @patch("check_release_version.subprocess.check_output")
    def test_compares_release_tag_and_head_trees(self, check_output: Mock) -> None:
        tag = "timeseries-table-format-v0.4.0"
        check_output.side_effect = ["shared-tree\n", "shared-tree\n"]
        tag_matches_head_tree(tag)
        self.assertEqual(
            [call.args[0] for call in check_output.call_args_list],
            [
                ["git", "rev-parse", f"refs/tags/{tag}^{{tree}}"],
                ["git", "rev-parse", "HEAD^{tree}"],
            ],
        )

        check_output.side_effect = ["tag-tree\n", "head-tree\n"]
        with self.assertRaises(RuntimeError):
            tag_matches_head_tree(tag)


if __name__ == "__main__":
    unittest.main()
