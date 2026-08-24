from __future__ import annotations

import json
import unittest
from typing import Any

from migrate_table_v6_to_protocol_v7 import MigrationError, transform_commits


def _index(kind: str) -> dict[str, Any]:
    if kind == "timestamp":
        return {"type": kind, "bucket": {"Minutes": 5}, "timezone": "UTC"}
    return {"type": kind, "bucket_width": 10}


def _metadata(kind: str) -> dict[str, Any]:
    return {
        "kind": {
            "TimeSeries": {
                "column": "ts",
                "entity_columns": [],
                "kind": _index(kind),
            }
        },
        "logical_schema": {
            "fields": [
                {
                    "name": "bucket",
                    "data_type": "Utf8",
                    "nullable": False,
                    "metadata": {"description": "bucket_width"},
                }
            ]
        },
        "created_at": "2026-08-23T12:34:56.123Z",
        "format_version": 6,
    }


def _commit(*actions: dict[str, Any], version: int = 1) -> dict[str, Any]:
    return {
        "version": version,
        "base_version": version - 1,
        "timestamp": "2026-08-23T12:35:00Z",
        "actions": list(actions),
    }


class TransformCommitsTests(unittest.TestCase):
    def test_transforms_all_index_domains_and_only_recognized_paths(self) -> None:
        cases = (
            ("timestamp", "timestamp", {"Minutes": 5}),
            ("int64", "int64", 10),
            ("u_int64", "uint64", 10),
        )
        for source_tag, target_tag, granularity in cases:
            with self.subTest(source_tag=source_tag):
                source = [
                    _commit(
                        {"UpdateTableMeta": _metadata(source_tag)},
                        {
                            "UpdateTableCoverage": {
                                "index_kind": _index(source_tag),
                                "coverage_path": "_coverage/table.bin",
                            }
                        },
                    )
                ]

                transformed, count, referenced = transform_commits(source)

                metadata = transformed[0]["actions"][0]["UpdateTableMeta"]
                metadata_kind = metadata["kind"]["TimeSeries"]["kind"]
                coverage_kind = transformed[0]["actions"][1]["UpdateTableCoverage"][
                    "index_kind"
                ]
                self.assertEqual(metadata["protocol_version"], 7)
                self.assertEqual(metadata["required_reader_features"], [])
                self.assertEqual(metadata["required_writer_features"], [])
                self.assertNotIn("format_version", metadata)
                for transformed_kind in (metadata_kind, coverage_kind):
                    self.assertEqual(transformed_kind["type"], target_tag)
                    self.assertEqual(transformed_kind["index_granularity"], granularity)
                    self.assertNotIn("bucket", transformed_kind)
                    self.assertNotIn("bucket_width", transformed_kind)
                self.assertEqual(
                    metadata["logical_schema"]["fields"][0]["name"], "bucket"
                )
                self.assertEqual(
                    metadata["logical_schema"]["fields"][0]["metadata"]["description"],
                    "bucket_width",
                )
                self.assertEqual(count, 2)
                self.assertEqual(referenced, {"_coverage/table.bin"})
                self.assertEqual(
                    source[0]["actions"][0]["UpdateTableMeta"]["format_version"], 6
                )

    def test_preserves_history_and_is_deterministic(self) -> None:
        segment = {
            "path": "data/segment.parquet",
            "format": "parquet",
            "entity_layout": "NotApplicable",
            "index_min": {"type": "int64", "value": -2},
            "index_max": {"type": "int64", "value": 8},
            "row_count": 3,
            "file_size": 99,
            "coverage_path": "_coverage/segment.bin",
        }
        source = [
            _commit(
                {"UpdateTableMeta": _metadata("int64")},
                {"AddSegment": segment},
            ),
            _commit(
                {"RemoveSegment": {"path": "data/segment.parquet"}},
                {"UpdateTableMeta": _metadata("int64")},
                version=2,
            ),
        ]

        first = transform_commits(source)
        second = transform_commits(json.loads(json.dumps(source)))

        self.assertEqual(first, second)
        self.assertEqual(first[0][1]["actions"][0], source[1]["actions"][0])
        self.assertEqual(first[2], {"data/segment.parquet", "_coverage/segment.bin"})

    def test_rejects_ambiguous_or_unknown_wire_shapes_with_location(self) -> None:
        invalid_cases = (
            (
                {
                    "type": "timestamp",
                    "bucket": {"Minutes": 1},
                    "index_granularity": {"Minutes": 1},
                },
                "unexpected fields",
            ),
            ({"type": "int64", "bucket_width": 0}, "nonzero"),
            (
                {"type": "uint64", "bucket_width": 1},
                "expected timestamp, int64, or u_int64",
            ),
        )
        for kind, message in invalid_cases:
            with self.subTest(kind=kind):
                metadata = _metadata("int64")
                metadata["kind"]["TimeSeries"]["kind"] = kind
                with self.assertRaisesRegex(
                    MigrationError, rf"commit 1, action 0, .*{message}"
                ):
                    transform_commits([_commit({"UpdateTableMeta": metadata})])

        with self.assertRaisesRegex(MigrationError, "unknown action 'FutureAction'"):
            transform_commits(
                [
                    _commit(
                        {"UpdateTableMeta": _metadata("int64")},
                        {"FutureAction": {}},
                    )
                ]
            )


if __name__ == "__main__":
    unittest.main()
