#!/usr/bin/env python3
"""One-time format-v6 to protocol-v7 transaction-log migration."""

from __future__ import annotations

import copy
import re
from datetime import datetime
from pathlib import PurePosixPath
from typing import Any

FORMAT_V6 = 6
PROTOCOL_V7 = 7
_U32_MAX = 2**32 - 1
_U64_MAX = 2**64 - 1
_UTC_TIMESTAMP = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z\Z")
_ACTION_NAMES = {
    "AddSegment",
    "RemoveSegment",
    "UpdateTableMeta",
    "UpdateTableCoverage",
}


class MigrationError(ValueError):
    """The source history cannot be proven safe to migrate."""


def _fail(commit: int, action: int | None, path: str, requirement: str) -> None:
    location = f"commit {commit}"
    if action is not None:
        location += f", action {action}"
    raise MigrationError(f"{location}, {path}: {requirement}")


def _object(value: Any, commit: int, action: int | None, path: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        _fail(commit, action, path, "expected a JSON object")
    return value


def _keys(
    value: dict[str, Any],
    required: set[str],
    optional: set[str],
    commit: int,
    action: int | None,
    path: str,
) -> None:
    missing = required - value.keys()
    unexpected = value.keys() - required - optional
    if missing:
        _fail(commit, action, path, f"missing fields {sorted(missing)}")
    if unexpected:
        _fail(commit, action, path, f"unexpected fields {sorted(unexpected)}")


def _unsigned(value: Any, maximum: int, commit: int, action: int, path: str) -> int:
    if type(value) is not int or not 0 <= value <= maximum:
        _fail(commit, action, path, f"expected an unsigned integer <= {maximum}")
    return value


def _positive(value: Any, maximum: int, commit: int, action: int, path: str) -> int:
    value = _unsigned(value, maximum, commit, action, path)
    if value == 0:
        _fail(commit, action, path, "expected a nonzero integer")
    return value


def _timestamp(value: Any, commit: int, action: int | None, path: str) -> None:
    if not isinstance(value, str) or not _UTC_TIMESTAMP.fullmatch(value):
        _fail(commit, action, path, "expected an RFC3339 UTC timestamp ending in Z")
    try:
        datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError:
        _fail(commit, action, path, "expected a valid RFC3339 UTC timestamp")


def _persisted_path(value: Any, commit: int, action: int, path: str) -> str:
    if not isinstance(value, str) or not value:
        _fail(commit, action, path, "expected a non-empty table-relative path")
    normalized = PurePosixPath(value)
    components = value.split("/")
    if (
        normalized.is_absolute()
        or str(normalized) != value
        or "\\" in value
        or any(
            component in {"", ".", ".."}
            or (len(component) >= 2 and component[0].isalpha() and component[1] == ":")
            for component in components
        )
    ):
        _fail(commit, action, path, "expected a canonical table-relative path")
    return value


def _transform_index_kind(value: Any, commit: int, action: int, path: str) -> None:
    kind = _object(value, commit, action, path)
    tag = kind.get("type")
    if tag == "timestamp":
        _keys(kind, {"type", "bucket"}, {"timezone"}, commit, action, path)
        bucket_path = f"{path}.bucket"
        bucket = _object(kind["bucket"], commit, action, bucket_path)
        if len(bucket) != 1 or next(iter(bucket), None) not in {
            "Seconds",
            "Minutes",
            "Hours",
            "Days",
        }:
            _fail(commit, action, bucket_path, "expected one recognized time bucket")
        _positive(next(iter(bucket.values())), _U32_MAX, commit, action, bucket_path)
        if "timezone" in kind and not isinstance(kind["timezone"], str):
            _fail(commit, action, f"{path}.timezone", "expected a string")
        kind["index_granularity"] = kind.pop("bucket")
        return

    if tag in {"int64", "u_int64"}:
        _keys(kind, {"type", "bucket_width"}, set(), commit, action, path)
        _positive(
            kind["bucket_width"],
            _U64_MAX,
            commit,
            action,
            f"{path}.bucket_width",
        )
        kind["index_granularity"] = kind.pop("bucket_width")
        if tag == "u_int64":
            kind["type"] = "uint64"
        return

    _fail(commit, action, f"{path}.type", "expected timestamp, int64, or u_int64")


def _transform_metadata(value: Any, commit: int, action: int, path: str) -> None:
    metadata = _object(value, commit, action, path)
    for field in (
        "protocol_version",
        "required_reader_features",
        "required_writer_features",
    ):
        if field in metadata:
            _fail(commit, action, f"{path}.{field}", "protocol-v7 field in v6 metadata")
    _keys(
        metadata,
        {"kind", "logical_schema", "created_at", "format_version"},
        set(),
        commit,
        action,
        path,
    )
    if (
        type(metadata["format_version"]) is not int
        or metadata["format_version"] != FORMAT_V6
    ):
        _fail(commit, action, f"{path}.format_version", "expected exactly 6")
    _timestamp(metadata["created_at"], commit, action, f"{path}.created_at")
    if metadata["logical_schema"] is not None and not isinstance(
        metadata["logical_schema"], dict
    ):
        _fail(commit, action, f"{path}.logical_schema", "expected an object or null")

    table_kind = _object(metadata["kind"], commit, action, f"{path}.kind")
    _keys(table_kind, {"TimeSeries"}, set(), commit, action, f"{path}.kind")
    index = _object(table_kind["TimeSeries"], commit, action, f"{path}.kind.TimeSeries")
    _keys(
        index,
        {"column", "entity_columns", "kind"},
        set(),
        commit,
        action,
        f"{path}.kind.TimeSeries",
    )
    if not isinstance(index["column"], str) or not index["column"]:
        _fail(
            commit,
            action,
            f"{path}.kind.TimeSeries.column",
            "expected a non-empty string",
        )
    entities = index["entity_columns"]
    if (
        not isinstance(entities, list)
        or any(not isinstance(entity, str) or not entity for entity in entities)
        or len(set(entities)) != len(entities)
        or index["column"] in entities
    ):
        _fail(
            commit,
            action,
            f"{path}.kind.TimeSeries.entity_columns",
            "expected unique non-empty strings distinct from the index column",
        )
    _transform_index_kind(index["kind"], commit, action, f"{path}.kind.TimeSeries.kind")

    del metadata["format_version"]
    metadata["protocol_version"] = PROTOCOL_V7
    metadata["required_reader_features"] = []
    metadata["required_writer_features"] = []


def _validate_index_value(value: Any, commit: int, action: int, path: str) -> None:
    index = _object(value, commit, action, path)
    _keys(index, {"type", "value"}, set(), commit, action, path)
    if index["type"] == "timestamp":
        _timestamp(index["value"], commit, action, f"{path}.value")
    elif index["type"] == "int64":
        if type(index["value"]) is not int or not -(2**63) <= index["value"] < 2**63:
            _fail(commit, action, f"{path}.value", "expected an i64 integer")
    elif index["type"] == "u_int64":
        _unsigned(index["value"], _U64_MAX, commit, action, f"{path}.value")
    else:
        _fail(commit, action, f"{path}.type", "unknown format-v6 index value type")


def _validate_segment(
    value: Any, commit: int, action: int, path: str, referenced: set[str]
) -> None:
    segment = _object(value, commit, action, path)
    _keys(
        segment,
        {"path", "format", "entity_layout", "index_min", "index_max", "row_count"},
        {"file_size", "coverage_path"},
        commit,
        action,
        path,
    )
    referenced.add(_persisted_path(segment["path"], commit, action, f"{path}.path"))
    if segment["format"] != "parquet":
        _fail(commit, action, f"{path}.format", "expected parquet")
    _validate_index_value(segment["index_min"], commit, action, f"{path}.index_min")
    _validate_index_value(segment["index_max"], commit, action, f"{path}.index_max")
    _unsigned(segment["row_count"], _U64_MAX, commit, action, f"{path}.row_count")
    if "file_size" in segment:
        _unsigned(segment["file_size"], _U64_MAX, commit, action, f"{path}.file_size")
    if "coverage_path" in segment:
        referenced.add(
            _persisted_path(
                segment["coverage_path"], commit, action, f"{path}.coverage_path"
            )
        )


def transform_commits(
    source_commits: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], int, frozenset[str]]:
    """Validate and structurally transform a complete ordered v6 commit history."""
    if not source_commits:
        raise MigrationError("commit 1, $: expected at least one commit")

    commits = copy.deepcopy(source_commits)
    transformed_actions = 0
    referenced: set[str] = set()
    first_has_metadata = False

    for expected_version, raw_commit in enumerate(commits, start=1):
        commit = _object(raw_commit, expected_version, None, "$")
        _keys(
            commit,
            {"version", "base_version", "timestamp", "actions"},
            set(),
            expected_version,
            None,
            "$",
        )
        if type(commit["version"]) is not int or commit["version"] != expected_version:
            _fail(expected_version, None, "$.version", f"expected {expected_version}")
        if (
            type(commit["base_version"]) is not int
            or commit["base_version"] != expected_version - 1
        ):
            _fail(
                expected_version,
                None,
                "$.base_version",
                f"expected {expected_version - 1}",
            )
        _timestamp(commit["timestamp"], expected_version, None, "$.timestamp")
        if not isinstance(commit["actions"], list):
            _fail(expected_version, None, "$.actions", "expected an array")

        for action_index, raw_action in enumerate(commit["actions"]):
            action_path = f"$.actions[{action_index}]"
            action = _object(raw_action, expected_version, action_index, action_path)
            if len(action) != 1:
                _fail(
                    expected_version,
                    action_index,
                    action_path,
                    "expected one action variant",
                )
            name, payload = next(iter(action.items()))
            if name not in _ACTION_NAMES:
                _fail(
                    expected_version,
                    action_index,
                    action_path,
                    f"unknown action {name!r}",
                )
            payload_path = f"{action_path}.{name}"

            if name == "UpdateTableMeta":
                _transform_metadata(
                    payload, expected_version, action_index, payload_path
                )
                first_has_metadata |= expected_version == 1
                transformed_actions += 1
            elif name == "UpdateTableCoverage":
                coverage = _object(
                    payload, expected_version, action_index, payload_path
                )
                _keys(
                    coverage,
                    {"index_kind", "coverage_path"},
                    set(),
                    expected_version,
                    action_index,
                    payload_path,
                )
                _transform_index_kind(
                    coverage["index_kind"],
                    expected_version,
                    action_index,
                    f"{payload_path}.index_kind",
                )
                referenced.add(
                    _persisted_path(
                        coverage["coverage_path"],
                        expected_version,
                        action_index,
                        f"{payload_path}.coverage_path",
                    )
                )
                transformed_actions += 1
            elif name == "AddSegment":
                _validate_segment(
                    payload, expected_version, action_index, payload_path, referenced
                )
            else:
                remove = _object(payload, expected_version, action_index, payload_path)
                _keys(
                    remove,
                    {"path"},
                    set(),
                    expected_version,
                    action_index,
                    payload_path,
                )
                _persisted_path(
                    remove["path"],
                    expected_version,
                    action_index,
                    f"{payload_path}.path",
                )

    if not first_has_metadata:
        _fail(1, None, "$.actions", "first commit must contain UpdateTableMeta")
    return commits, transformed_actions, frozenset(referenced)
