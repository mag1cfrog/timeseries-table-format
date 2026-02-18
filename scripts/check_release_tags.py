#!/usr/bin/env python3
from __future__ import annotations

import os
import pathlib
import re
import subprocess
import sys


REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]


def _run(args: list[str], *, quiet: bool = False) -> str:
    stderr = subprocess.DEVNULL if quiet else None
    return subprocess.check_output(args, cwd=REPO_ROOT, text=True, stderr=stderr).strip()


def _changed_cargo_tomls_in_head_commit() -> list[pathlib.Path]:
    # If HEAD has no parent (rare in CI), treat as no-op.
    try:
        _run(["git", "rev-parse", "HEAD^"])
    except Exception:
        return []

    out = _run(["git", "diff", "--name-only", "HEAD^..HEAD"])
    paths = [p for p in out.splitlines() if p.endswith("Cargo.toml")]

    cargo_paths: list[pathlib.Path] = []
    for p in paths:
        if p.startswith("crates/") and p.count("/") == 2 and p.endswith("/Cargo.toml"):
            cargo_paths.append(REPO_ROOT / p)
    return cargo_paths


def _cargo_package_name_and_version(cargo_toml: pathlib.Path) -> tuple[str, str]:
    text = cargo_toml.read_text(encoding="utf-8")
    m_name = re.search(r'(?m)^name\s*=\s*"([^"]+)"\s*$', text)
    m_ver = re.search(r'(?m)^version\s*=\s*"([^"]+)"\s*$', text)
    if not m_name or not m_ver:
        raise RuntimeError(f"Could not parse name/version from {cargo_toml}")
    return m_name.group(1), m_ver.group(1)


def _cargo_package_version_at_rev(cargo_toml_rel: str, rev: str) -> str | None:
    try:
        text = _run(["git", "show", f"{rev}:{cargo_toml_rel}"])
    except Exception:
        return None
    m_ver = re.search(r'(?m)^version\s*=\s*"([^"]+)"\s*$', text)
    if not m_ver:
        return None
    return m_ver.group(1)


def _released_cargo_tomls_in_head_commit() -> list[pathlib.Path]:
    """
    Return Cargo.toml paths for packages that were *released* in HEAD.

    release-plz release commits bump `version = "..."` in crate Cargo.toml files.
    Only require tags for crates whose version changed in HEAD (not arbitrary Cargo.toml edits).
    """

    cargo_paths = _changed_cargo_tomls_in_head_commit()
    if not cargo_paths:
        return []

    released: list[pathlib.Path] = []
    for p in cargo_paths:
        rel = p.relative_to(REPO_ROOT).as_posix()
        head_ver = _cargo_package_version_at_rev(rel, "HEAD")
        prev_ver = _cargo_package_version_at_rev(rel, "HEAD^")
        if head_ver is None:
            continue
        if prev_ver != head_ver:
            released.append(p)
    return released


def _tag_points_at_head(tag: str) -> bool:
    # Avoid noisy "fatal: ambiguous argument ..." output when a tag is missing.
    try:
        _run(["git", "rev-parse", "-q", "--verify", f"refs/tags/{tag}^{{}}"], quiet=True)
    except Exception:
        return False

    tag_commit = _run(["git", "rev-list", "-n", "1", tag], quiet=True)
    head = _run(["git", "rev-parse", "HEAD"])
    return tag_commit == head


def main() -> int:
    # release-plz can create/push tags during this workflow run; refresh local tags before checking.
    # Best-effort: if this fails, the check still runs and will produce actionable output.
    if os.environ.get("GITHUB_ACTIONS") or os.environ.get("CI"):
        try:
            _run(["git", "fetch", "--force", "--tags", "origin"], quiet=True)
        except Exception:
            pass

    cargo_tomls = _released_cargo_tomls_in_head_commit()
    if not cargo_tomls:
        return 0

    missing: list[str] = []
    wrong_target: list[str] = []

    for p in cargo_tomls:
        name, version = _cargo_package_name_and_version(p)
        tag = f"{name}-v{version}"

        if not _tag_points_at_head(tag):
            # Distinguish "missing" from "points somewhere else" for debugging.
            try:
                _run(["git", "rev-parse", "-q", "--verify", f"refs/tags/{tag}"])
            except Exception:
                missing.append(tag)
            else:
                wrong_target.append(tag)

    if not missing and not wrong_target:
        return 0

    sys.stderr.write("release tag check failed.\n\n")
    if missing:
        sys.stderr.write("Missing tags (expected to point at HEAD):\n")
        for t in missing:
            sys.stderr.write(f"  - {t}\n")
        sys.stderr.write("\n")
    if wrong_target:
        sys.stderr.write("Tags exist but do not point at HEAD:\n")
        for t in wrong_target:
            sys.stderr.write(f"  - {t}\n")
        sys.stderr.write("\n")

    sys.stderr.write("This usually indicates a release commit was merged but tag creation did not happen.\n")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
