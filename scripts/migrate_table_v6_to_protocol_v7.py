#!/usr/bin/env python3
"""One-time format-v6 to protocol-v7 transaction-log migration."""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import os
import re
import shutil
import stat
import sys
import tempfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import Any

FORMAT_V6 = 6
PROTOCOL_V7 = 7
_U32_MAX = 2**32 - 1
_U64_MAX = 2**64 - 1
_UTC_TIMESTAMP = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?Z\Z")
_CURRENT = re.compile(rb"[1-9][0-9]*[ \t\r\n\f\v]*\Z")
_COMMIT_FILE = re.compile(r"([0-9]{10,})\.json\Z")
_LOG_DIR = "_timeseries_log"
_CURRENT_FILE = "CURRENT"
_COPY_CHUNK_SIZE = 1024 * 1024
_ACTION_NAMES = {
    "AddSegment",
    "RemoveSegment",
    "UpdateTableMeta",
    "UpdateTableCoverage",
}


class MigrationError(ValueError):
    """The source history cannot be proven safe to migrate."""


@dataclass(frozen=True)
class MigrationReport:
    """Concise facts from one successful migration."""

    source: Path
    destination: Path
    current_version: int
    commit_count: int
    transformed_action_count: int
    parquet_files: int
    parquet_bytes: int
    coverage_files: int
    coverage_bytes: int
    other_files: int
    other_bytes: int


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


def _reject_symlink_components(path: Path) -> None:
    absolute = Path(os.path.abspath(path))
    current = Path(absolute.anchor)
    for component in absolute.parts[1:]:
        current /= component
        if os.path.lexists(current) and current.is_symlink():
            raise MigrationError(f"path traverses symlink: {current}")


def _resolve_paths(
    source_arg: str | Path, destination_arg: str | Path
) -> tuple[Path, Path]:
    source_input = Path(source_arg).expanduser()
    destination_input = Path(destination_arg).expanduser()
    _reject_symlink_components(source_input)
    _reject_symlink_components(destination_input)

    try:
        source = source_input.resolve(strict=True)
    except OSError as error:
        raise MigrationError(f"source does not exist: {source_input}") from error
    if not source.is_dir():
        raise MigrationError(f"source is not a local filesystem directory: {source}")

    destination_absolute = Path(os.path.abspath(destination_input))
    destination_exists = os.path.lexists(destination_absolute)
    try:
        destination_parent = destination_absolute.parent.resolve(strict=True)
    except OSError as error:
        raise MigrationError(
            f"destination parent does not exist: {destination_absolute.parent}"
        ) from error
    if not destination_parent.is_dir():
        raise MigrationError(
            f"destination parent is not a local filesystem directory: {destination_parent}"
        )
    destination = destination_parent / destination_absolute.name
    destination_resolved = (
        destination_absolute.resolve(strict=True) if destination_exists else destination
    )

    if source == destination_resolved:
        raise MigrationError("source and destination resolve to the same path")
    if destination_resolved.is_relative_to(source):
        raise MigrationError("destination may not be inside source")
    if source.is_relative_to(destination_resolved):
        raise MigrationError("source may not be inside destination")
    if destination_exists:
        raise MigrationError(f"destination already exists: {destination_resolved}")
    return source, destination


def _scan_regular_files(root: Path) -> list[Path]:
    files: list[Path] = []
    pending = [root]
    while pending:
        directory = pending.pop()
        try:
            entries = sorted(os.scandir(directory), key=lambda entry: entry.name)
        except OSError as error:
            raise MigrationError(
                f"cannot traverse directory {directory}: {error}"
            ) from error
        for entry in entries:
            path = Path(entry.path)
            if entry.is_symlink():
                raise MigrationError(f"symlink is not allowed in table: {path}")
            if (
                entry.name == "_staged"
                or entry.name.startswith(".tmp")
                or entry.name.endswith(".tmp")
            ):
                raise MigrationError(f"unexpected temporary table entry: {path}")
            if entry.is_dir(follow_symlinks=False):
                pending.append(path)
            elif entry.is_file(follow_symlinks=False):
                files.append(path)
            else:
                raise MigrationError(f"special filesystem entry is not allowed: {path}")
    return sorted(files)


def _open_regular(path: Path):
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as error:
        raise MigrationError(f"cannot open regular file {path}: {error}") from error
    metadata = os.fstat(descriptor)
    if not stat.S_ISREG(metadata.st_mode):
        os.close(descriptor)
        raise MigrationError(f"expected regular file: {path}")
    return os.fdopen(descriptor, "rb")


def _read_bytes(path: Path) -> bytes:
    with _open_regular(path) as file:
        return file.read()


def _read_current(root: Path) -> int:
    path = root / _LOG_DIR / _CURRENT_FILE
    contents = _read_bytes(path)
    if not _CURRENT.fullmatch(contents):
        raise MigrationError(
            f"{path}: CURRENT must be canonical unsigned decimal greater than zero"
        )
    version = int(contents)
    if version > _U64_MAX:
        raise MigrationError(f"{path}: CURRENT exceeds u64")
    return version


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON member {key!r}")
        result[key] = value
    return result


def _load_commits(root: Path, current_version: int) -> list[dict[str, Any]]:
    log_dir = root / _LOG_DIR
    if not log_dir.is_dir() or log_dir.is_symlink():
        raise MigrationError(f"missing regular transaction log directory: {log_dir}")

    commit_paths: dict[int, Path] = {}
    for entry in sorted(log_dir.iterdir()):
        if entry.name == _CURRENT_FILE:
            continue
        match = _COMMIT_FILE.fullmatch(entry.name)
        if match is None:
            raise MigrationError(f"unexpected transaction-log entry: {entry}")
        version = int(match.group(1))
        if version == 0 or entry.name != f"{version:010d}.json":
            raise MigrationError(f"noncanonical numeric commit filename: {entry.name}")
        if version in commit_paths:
            raise MigrationError(f"duplicate numeric commit version: {version}")
        commit_paths[version] = entry

    versions = sorted(commit_paths)
    if len(versions) != current_version:
        raise MigrationError(
            f"transaction log has {len(versions)} commits but CURRENT is {current_version}"
        )
    for expected, actual in enumerate(versions, start=1):
        if actual != expected:
            raise MigrationError(
                f"non-contiguous transaction log: expected commit {expected}, found {actual}"
            )

    commits: list[dict[str, Any]] = []
    for version in versions:
        path = commit_paths[version]
        try:
            commit = json.loads(
                _read_bytes(path).decode("utf-8"),
                object_pairs_hook=_reject_duplicate_keys,
            )
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as error:
            raise MigrationError(
                f"commit {version}, $: malformed JSON: {error}"
            ) from error
        if not isinstance(commit, dict):
            raise MigrationError(
                f"commit {version}, $: expected exactly one JSON object"
            )
        commits.append(commit)
    return commits


def _manifest(root: Path) -> dict[str, tuple[int, str]]:
    manifest: dict[str, tuple[int, str]] = {}
    for path in _scan_regular_files(root):
        digest = hashlib.sha256()
        size = 0
        with _open_regular(path) as file:
            while chunk := file.read(_COPY_CHUNK_SIZE):
                digest.update(chunk)
                size += len(chunk)
        manifest[path.relative_to(root).as_posix()] = (size, digest.hexdigest())
    return manifest


def _non_log(manifest: dict[str, tuple[int, str]]) -> dict[str, tuple[int, str]]:
    return {
        path: details
        for path, details in manifest.items()
        if PurePosixPath(path).parts[0] != _LOG_DIR
    }


def _require_referenced_files(root: Path, referenced: frozenset[str]) -> None:
    for relative in sorted(referenced):
        path = root.joinpath(*PurePosixPath(relative).parts)
        try:
            metadata = path.lstat()
        except OSError as error:
            raise MigrationError(f"referenced file is missing: {relative}") from error
        if not stat.S_ISREG(metadata.st_mode):
            raise MigrationError(f"referenced path is not a regular file: {relative}")


def _write_commits(root: Path, commits: list[dict[str, Any]]) -> None:
    log_dir = root / _LOG_DIR
    for version, commit in enumerate(commits, start=1):
        path = log_dir / f"{version:010d}.json"
        payload = (
            json.dumps(commit, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
        )
        path.write_text(payload, encoding="utf-8")


def _index_value(value: dict[str, Any], target_kind: str) -> tuple[Any, ...]:
    source_kind = value["type"]
    expected_kind = "u_int64" if target_kind == "uint64" else target_kind
    if source_kind != expected_kind:
        raise MigrationError(
            f"segment index value type {source_kind!r} does not match {target_kind!r}"
        )
    raw = value["value"]
    if source_kind == "timestamp":
        whole, separator, fraction = raw[:-1].partition(".")
        return datetime.fromisoformat(whole), int(
            (fraction if separator else "").ljust(9, "0")
        )
    return (raw,)


def _validate_v7_replay(commits: list[dict[str, Any]]) -> None:
    metadata: dict[str, Any] | None = None
    live_segments: dict[str, dict[str, Any]] = {}
    all_segments: list[dict[str, Any]] = []
    coverage: dict[str, Any] | None = None

    for commit in commits:
        for action in commit["actions"]:
            name, payload = next(iter(action.items()))
            if name == "UpdateTableMeta":
                if payload.get("protocol_version") != PROTOCOL_V7:
                    raise MigrationError(
                        "destination metadata does not require protocol 7"
                    )
                if payload.get("required_reader_features") != []:
                    raise MigrationError("destination reader feature set is not empty")
                if payload.get("required_writer_features") != []:
                    raise MigrationError("destination writer feature set is not empty")
                if "format_version" in payload:
                    raise MigrationError("destination metadata retains format_version")
                metadata = payload
            elif name == "AddSegment":
                path = payload["path"]
                if path in live_segments:
                    raise MigrationError(f"duplicate live segment path: {path}")
                live_segments[path] = payload
                all_segments.append(payload)
            elif name == "RemoveSegment":
                live_segments.pop(payload["path"], None)
            elif name == "UpdateTableCoverage":
                coverage = payload

    if metadata is None:
        raise MigrationError("destination replay found no table metadata")
    table = metadata["kind"]["TimeSeries"]
    kind = table["kind"]
    if coverage is not None and coverage["index_kind"] != kind:
        raise MigrationError(
            "destination table coverage index kind does not match metadata"
        )
    if all_segments and metadata["logical_schema"] is None:
        raise MigrationError("destination has segments but no logical schema")

    entity_count = len(table["entity_columns"])
    for segment in all_segments:
        layout = segment["entity_layout"]
        valid_layout = (entity_count == 0 and layout == "NotApplicable") or (
            entity_count > 0
            and (
                layout == "Mixed"
                or (
                    isinstance(layout, dict)
                    and set(layout) == {"Single"}
                    and isinstance(layout["Single"], list)
                    and len(layout["Single"]) == entity_count
                )
            )
        )
        if not valid_layout:
            raise MigrationError(
                f"segment {segment['path']} has invalid entity layout for metadata"
            )

    for segment in live_segments.values():
        minimum = _index_value(segment["index_min"], kind["type"])
        maximum = _index_value(segment["index_max"], kind["type"])
        if minimum > maximum:
            raise MigrationError(f"segment {segment['path']} has reversed index bounds")


def _validate_destination(
    root: Path,
    current_version: int,
    expected_commits: list[dict[str, Any]],
    source_non_log: dict[str, tuple[int, str]],
    referenced: frozenset[str],
) -> None:
    if _read_current(root) != current_version:
        raise MigrationError("destination CURRENT does not match source")
    commits = _load_commits(root, current_version)
    if commits != expected_commits:
        raise MigrationError(
            "destination commits differ from the structured transformation"
        )
    _validate_v7_replay(commits)
    _require_referenced_files(root, referenced)
    destination_manifest = _manifest(root)
    destination_non_log = _non_log(destination_manifest)
    if destination_non_log != source_non_log:
        raise MigrationError("destination non-log SHA-256 manifest differs from source")


def _copy_table(source: Path, destination: Path) -> None:
    shutil.copytree(
        source,
        destination,
        dirs_exist_ok=True,
        symlinks=True,
    )


def _publish(temporary: Path, destination: Path) -> None:
    if os.path.lexists(destination):
        raise MigrationError(f"destination appeared before publication: {destination}")
    os.rename(temporary, destination)


def _cleanup_temporary(temporary: Path, parent: Path, prefix: str) -> None:
    if temporary.parent != parent or not temporary.name.startswith(prefix):
        raise MigrationError(f"refusing to remove unowned temporary path: {temporary}")
    if temporary.is_symlink():
        raise MigrationError(
            f"refusing to remove replaced temporary symlink: {temporary}"
        )
    shutil.rmtree(temporary)


def _preserved_counts(
    manifest: dict[str, tuple[int, str]],
) -> tuple[int, int, int, int, int, int]:
    coverage = [
        details
        for path, details in manifest.items()
        if PurePosixPath(path).parts[0] == "_coverage"
    ]
    parquet = [
        details
        for path, details in manifest.items()
        if path.endswith(".parquet") and PurePosixPath(path).parts[0] != "_coverage"
    ]
    other = [
        details
        for path, details in manifest.items()
        if not path.endswith(".parquet") and PurePosixPath(path).parts[0] != "_coverage"
    ]
    return (
        len(parquet),
        sum(size for size, _ in parquet),
        len(coverage),
        sum(size for size, _ in coverage),
        len(other),
        sum(size for size, _ in other),
    )


def migrate_table(
    source_arg: str | Path, destination_arg: str | Path
) -> MigrationReport:
    """Copy, transform, validate, and atomically publish one local v6 table."""
    source, destination = _resolve_paths(source_arg, destination_arg)
    _scan_regular_files(source)
    current_version = _read_current(source)
    source_commits = _load_commits(source, current_version)
    transformed, transformed_actions, referenced = transform_commits(source_commits)
    _require_referenced_files(source, referenced)
    source_manifest = _manifest(source)
    source_non_log = _non_log(source_manifest)

    prefix = f".{destination.name}.v6-to-v7-"
    temporary: Path | None = Path(
        tempfile.mkdtemp(prefix=prefix, dir=destination.parent)
    )
    try:
        _copy_table(source, temporary)
        if _read_current(source) != current_version:
            raise MigrationError("source CURRENT changed during copy")
        _write_commits(temporary, transformed)
        if _read_current(source) != current_version:
            raise MigrationError("source CURRENT changed during log transformation")
        _validate_destination(
            temporary,
            current_version,
            transformed,
            source_non_log,
            referenced,
        )
        if _manifest(source) != source_manifest:
            raise MigrationError("source table changed during migration")
        for relative in source_non_log:
            source_stat = (source / relative).stat()
            destination_stat = (temporary / relative).stat()
            if (
                source_stat.st_dev == destination_stat.st_dev
                and source_stat.st_ino == destination_stat.st_ino
            ):
                raise MigrationError(
                    f"copied file is hard-linked to source: {relative}"
                )
        _publish(temporary, destination)
        temporary = None
    except Exception as error:
        if temporary is not None and os.path.lexists(temporary):
            try:
                _cleanup_temporary(temporary, destination.parent, prefix)
            except Exception as cleanup_error:
                raise MigrationError(
                    f"migration failed ({error}); temporary cleanup failed: {cleanup_error}"
                ) from error
        if isinstance(error, MigrationError):
            raise
        raise MigrationError(f"migration failed: {error}") from error

    counts = _preserved_counts(source_non_log)
    return MigrationReport(
        source=source,
        destination=destination,
        current_version=current_version,
        commit_count=len(source_commits),
        transformed_action_count=transformed_actions,
        parquet_files=counts[0],
        parquet_bytes=counts[1],
        coverage_files=counts[2],
        coverage_bytes=counts[3],
        other_files=counts[4],
        other_bytes=counts[5],
    )


def _print_report(report: MigrationReport) -> None:
    print(f"source: {report.source}")
    print(f"destination: {report.destination}")
    print("protocol: format-v6 -> protocol-v7")
    print(f"CURRENT: {report.current_version}")
    print(
        f"transformed: {report.commit_count} commits, "
        f"{report.transformed_action_count} actions"
    )
    print(
        f"preserved parquet: {report.parquet_files} files, {report.parquet_bytes} bytes"
    )
    print(
        f"preserved coverage: {report.coverage_files} files, "
        f"{report.coverage_bytes} bytes"
    )
    print(f"preserved other: {report.other_files} files, {report.other_bytes} bytes")
    print("non-log SHA-256: identical")
    print("publication: atomic sibling rename completed")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Migrate one offline local format-v6 table to protocol-v7."
    )
    parser.add_argument("source_table")
    parser.add_argument("destination_table")
    arguments = parser.parse_args(argv)
    try:
        report = migrate_table(arguments.source_table, arguments.destination_table)
    except MigrationError as error:
        print(f"migration failed: {error}", file=sys.stderr)
        return 1
    _print_report(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
