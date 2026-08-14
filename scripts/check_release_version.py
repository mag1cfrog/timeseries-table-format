#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import pathlib
import re
import subprocess
import sys


REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
ACTIVE_PACKAGES = {
    "timeseries-table-format",
    "timeseries-table-python",
}
TAG_PREFIX = "timeseries-table-format-v"


def parse_workspace_version(manifest: str) -> str:
    section = ""
    for line in manifest.splitlines():
        value = line.partition("#")[0].strip()
        if value.startswith("[") and value.endswith("]"):
            section = value
        elif section == "[workspace.package]":
            match = re.fullmatch(r'version\s*=\s*"([^"]+)"', value)
            if match:
                return match.group(1)
    raise RuntimeError("[workspace.package].version is missing")


def workspace_version() -> str:
    version = parse_workspace_version(
        (REPO_ROOT / "Cargo.toml").read_text(encoding="utf-8")
    )

    metadata = json.loads(
        subprocess.check_output(
            ["cargo", "metadata", "--locked", "--format-version", "1", "--no-deps"],
            cwd=REPO_ROOT,
            text=True,
        )
    )
    versions = {
        package["name"]: package["version"]
        for package in metadata["packages"]
    }
    if versions.keys() != ACTIVE_PACKAGES:
        missing = sorted(ACTIVE_PACKAGES - versions.keys())
        unexpected = sorted(versions.keys() - ACTIVE_PACKAGES)
        raise RuntimeError(
            f"Cargo package graph mismatch: missing={missing}, unexpected={unexpected}"
        )
    mismatched = {name: value for name, value in versions.items() if value != version}
    if mismatched:
        raise RuntimeError(
            f"Cargo package versions do not match workspace version {version}: {mismatched}"
        )
    return version


def validate_tag(tag: str, version: str) -> None:
    expected = f"{TAG_PREFIX}{version}"
    if tag != expected:
        raise RuntimeError(f"expected release tag {expected}, found {tag or '<missing>'}")


def tag_points_at_head(tag: str) -> None:
    try:
        tag_commit = subprocess.check_output(
            ["git", "rev-list", "-n", "1", f"refs/tags/{tag}"],
            cwd=REPO_ROOT,
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except subprocess.CalledProcessError as error:
        raise RuntimeError(f"release tag does not exist: {tag}") from error
    head = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=REPO_ROOT, text=True
    ).strip()
    if tag_commit != head:
        raise RuntimeError(f"release tag {tag} does not point at HEAD")


def main() -> int:
    parser = argparse.ArgumentParser()
    tag_group = parser.add_mutually_exclusive_group()
    tag_group.add_argument("--tag", help="canonical release tag to verify")
    tag_group.add_argument(
        "--require-tag",
        action="store_true",
        help="require the canonical tag for the workspace version on HEAD",
    )
    args = parser.parse_args()

    try:
        version = workspace_version()
        tag = f"{TAG_PREFIX}{version}" if args.require_tag else args.tag
        if tag is not None:
            validate_tag(tag, version)
            tag_points_at_head(tag)
    except (RuntimeError, subprocess.CalledProcessError) as error:
        sys.stderr.write(f"release version check failed: {error}\n")
        return 1

    print(version)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
