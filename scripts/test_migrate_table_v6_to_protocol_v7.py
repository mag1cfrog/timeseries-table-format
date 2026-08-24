from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path
from typing import Any
from unittest.mock import patch

import migrate_table_v6_to_protocol_v7 as migration
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


def _write_v6_table(root: Path) -> None:
    log = root / "_timeseries_log"
    (root / "data").mkdir(parents=True)
    (root / "_coverage" / "segments").mkdir(parents=True)
    (root / "_coverage" / "table").mkdir(parents=True)
    (root / "notes").mkdir(parents=True)
    log.mkdir()
    (root / "data" / "segment.parquet").write_bytes(b"PAR1payloadPAR1")
    (root / "_coverage" / "segments" / "segment.roar").write_bytes(b"segment coverage")
    (root / "_coverage" / "table" / "2.roar").write_bytes(b"table coverage")
    (root / "notes" / "readme.txt").write_text("preserve bucket_width\n")

    first_metadata = _metadata("int64")
    first_metadata["logical_schema"] = None
    final_metadata = _metadata("int64")
    final_metadata["logical_schema"] = {
        "columns": [
            {"name": "ts", "data_type": "Int64", "nullable": False},
            {"name": "bucket", "data_type": "Utf8", "nullable": False},
        ]
    }
    segment = {
        "path": "data/segment.parquet",
        "format": "parquet",
        "entity_layout": "NotApplicable",
        "index_min": {"type": "int64", "value": -2},
        "index_max": {"type": "int64", "value": 8},
        "row_count": 3,
        "file_size": 15,
        "coverage_path": "_coverage/segments/segment.roar",
    }
    commits = (
        _commit({"UpdateTableMeta": first_metadata}),
        _commit(
            {"UpdateTableMeta": final_metadata},
            {"AddSegment": segment},
            {
                "UpdateTableCoverage": {
                    "index_kind": _index("int64"),
                    "coverage_path": "_coverage/table/2.roar",
                }
            },
            version=2,
        ),
    )
    for version, commit in enumerate(commits, start=1):
        (log / f"{version:010d}.json").write_text(json.dumps(commit) + "\n")
    (log / "CURRENT").write_text("2\n")


def _tree_bytes(root: Path) -> dict[str, bytes]:
    return {
        path.relative_to(root).as_posix(): path.read_bytes()
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


def _temporary_paths(parent: Path, destination: Path) -> list[Path]:
    return list(parent.glob(f".{destination.name}.v6-to-v7-*"))


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
            ({"type": "timestamp"}, "missing fields"),
            ({"type": "timestamp", "bucket": None}, "expected a JSON object"),
            (
                {
                    "type": "timestamp",
                    "bucket": {"Minutes": 1},
                    "index_granularity": {"Minutes": 1},
                },
                "unexpected fields",
            ),
            ({"type": "int64", "bucket_width": 0}, "nonzero"),
            ({"type": "int64", "bucket_width": "1"}, "unsigned integer"),
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

        protocol_metadata = _metadata("int64")
        protocol_metadata["protocol_version"] = 7
        with self.assertRaisesRegex(MigrationError, "protocol-v7 field in v6 metadata"):
            transform_commits([_commit({"UpdateTableMeta": protocol_metadata})])

        with self.assertRaisesRegex(MigrationError, "unknown action 'FutureAction'"):
            transform_commits(
                [
                    _commit(
                        {"UpdateTableMeta": _metadata("int64")},
                        {"FutureAction": {}},
                    )
                ]
            )


class FilesystemMigrationTests(unittest.TestCase):
    def test_copies_validates_and_publishes_independent_files(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory)
            source = parent / "source"
            destination = parent / "destination"
            _write_v6_table(source)
            source_before = _tree_bytes(source)

            report = migration.migrate_table(source, destination)

            self.assertEqual(_tree_bytes(source), source_before)
            self.assertTrue(destination.is_dir())
            self.assertEqual(
                (destination / "_timeseries_log" / "CURRENT").read_text(), "2\n"
            )
            transformed = json.loads(
                (destination / "_timeseries_log" / "0000000002.json").read_text()
            )
            metadata = transformed["actions"][0]["UpdateTableMeta"]
            self.assertEqual(metadata["protocol_version"], 7)
            self.assertNotIn("format_version", metadata)
            self.assertEqual(
                metadata["kind"]["TimeSeries"]["kind"],
                {"type": "int64", "index_granularity": 10},
            )
            for relative, contents in source_before.items():
                if relative.startswith("_timeseries_log/"):
                    continue
                self.assertEqual((destination / relative).read_bytes(), contents)
                self.assertNotEqual(
                    (source / relative).stat().st_ino,
                    (destination / relative).stat().st_ino,
                )
            self.assertEqual(report.current_version, 2)
            self.assertEqual(report.transformed_action_count, 3)
            self.assertEqual(
                (report.parquet_files, report.coverage_files, report.other_files),
                (1, 2, 1),
            )
            self.assertEqual(_temporary_paths(parent, destination), [])

    def test_rejects_equal_nested_existing_and_symlinked_paths(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory)
            source = parent / "source"
            _write_v6_table(source)
            existing = parent / "existing"
            existing.mkdir()
            linked_source = parent / "linked-source"
            linked_source.symlink_to(source, target_is_directory=True)

            cases = (
                (source, source, "same path"),
                (source, source / "destination", "inside source"),
                (source, parent, "source may not be inside destination"),
                (source, existing, "already exists"),
                (linked_source, parent / "linked-output", "symlink"),
            )
            for source_arg, destination_arg, message in cases:
                with (
                    self.subTest(message=message),
                    self.assertRaisesRegex(MigrationError, message),
                ):
                    migration.migrate_table(source_arg, destination_arg)

    def test_rejects_invalid_log_states_without_publishing(self) -> None:
        def malformed_current(source: Path) -> None:
            (source / "_timeseries_log" / "CURRENT").write_text("02\n")

        def missing_commit(source: Path) -> None:
            (source / "_timeseries_log" / "0000000001.json").unlink()

        def extra_commit(source: Path) -> None:
            (source / "_timeseries_log" / "0000000003.json").write_text("{}\n")

        def unexpected_log_file(source: Path) -> None:
            (source / "_timeseries_log" / "0000000002.json.tmp").write_text("tmp")

        def malformed_json(source: Path) -> None:
            (source / "_timeseries_log" / "0000000002.json").write_text("{\n")

        def duplicate_json_member(source: Path) -> None:
            path = source / "_timeseries_log" / "0000000002.json"
            payload = path.read_text().replace(
                '"version": 2', '"version": 2, "version": 2'
            )
            path.write_text(payload)

        def noncanonical_numeric_name(source: Path) -> None:
            (source / "_timeseries_log" / "00000000001.json").write_text("{}\n")

        def mismatched_payload(source: Path) -> None:
            path = source / "_timeseries_log" / "0000000002.json"
            commit = json.loads(path.read_text())
            commit["version"] = 9
            path.write_text(json.dumps(commit))

        def noncanonical_persisted_path(source: Path) -> None:
            path = source / "_timeseries_log" / "0000000002.json"
            commit = json.loads(path.read_text())
            commit["actions"][1]["AddSegment"]["path"] = "../segment.parquet"
            path.write_text(json.dumps(commit))

        cases = (
            (malformed_current, "CURRENT"),
            (missing_commit, "commits but CURRENT"),
            (extra_commit, "commits but CURRENT"),
            (unexpected_log_file, "unexpected temporary table entry"),
            (malformed_json, "malformed JSON"),
            (duplicate_json_member, "duplicate JSON member"),
            (noncanonical_numeric_name, "noncanonical numeric commit filename"),
            (mismatched_payload, "expected 2"),
            (noncanonical_persisted_path, "canonical table-relative path"),
        )
        for mutate, message in cases:
            with (
                self.subTest(message=message),
                tempfile.TemporaryDirectory() as directory,
            ):
                parent = Path(directory)
                source = parent / "source"
                destination = parent / "destination"
                _write_v6_table(source)
                mutate(source)
                source_before = _tree_bytes(source)

                with self.assertRaisesRegex(MigrationError, message):
                    migration.migrate_table(source, destination)

                self.assertEqual(_tree_bytes(source), source_before)
                self.assertFalse(destination.exists())
                self.assertEqual(_temporary_paths(parent, destination), [])

    def test_rejects_missing_references_symlinks_and_special_files(self) -> None:
        def missing_reference(source: Path) -> None:
            (source / "_coverage" / "table" / "2.roar").unlink()

        def symlink(source: Path) -> None:
            (source / "notes" / "link").symlink_to(source / "notes" / "readme.txt")

        def temporary_entry(source: Path) -> None:
            (source / "data" / "_staged").mkdir()
            (source / "data" / "_staged" / "orphan.parquet").write_bytes(b"orphan")

        cases = (
            (missing_reference, "referenced file is missing"),
            (symlink, "symlink is not allowed"),
            (temporary_entry, "unexpected temporary table entry"),
        )
        if hasattr(os, "mkfifo"):

            def special_file(source: Path) -> None:
                os.mkfifo(source / "notes" / "pipe")

            cases += ((special_file, "special filesystem entry"),)

        for mutate, message in cases:
            with (
                self.subTest(message=message),
                tempfile.TemporaryDirectory() as directory,
            ):
                parent = Path(directory)
                source = parent / "source"
                destination = parent / "destination"
                _write_v6_table(source)
                mutate(source)

                with self.assertRaisesRegex(MigrationError, message):
                    migration.migrate_table(source, destination)

                self.assertFalse(destination.exists())
                self.assertEqual(_temporary_paths(parent, destination), [])

    def test_current_change_and_publication_failure_clean_up_only_temporary_output(
        self,
    ) -> None:
        for failure in (
            "copy",
            "current_after_copy",
            "current_after_transform",
            "publish",
        ):
            with (
                self.subTest(failure=failure),
                tempfile.TemporaryDirectory() as directory,
            ):
                parent = Path(directory)
                source = parent / "source"
                destination = parent / "destination"
                _write_v6_table(source)
                source_before = _tree_bytes(source)

                if failure == "copy":
                    context = patch.object(
                        migration,
                        "_copy_table",
                        side_effect=OSError("injected copy failure"),
                    )
                elif failure == "current_after_copy":
                    context = patch.object(
                        migration, "_read_current", side_effect=(2, 3)
                    )
                elif failure == "current_after_transform":
                    context = patch.object(
                        migration, "_read_current", side_effect=(2, 2, 3)
                    )
                else:
                    context = patch.object(
                        migration.os,
                        "rename",
                        side_effect=OSError("injected publication failure"),
                    )
                with context, self.assertRaises(MigrationError):
                    migration.migrate_table(source, destination)

                self.assertEqual(_tree_bytes(source), source_before)
                self.assertFalse(destination.exists())
                self.assertEqual(_temporary_paths(parent, destination), [])


if __name__ == "__main__":
    unittest.main()
