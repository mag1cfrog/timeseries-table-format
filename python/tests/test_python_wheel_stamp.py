from __future__ import annotations

import importlib.util
import subprocess
from pathlib import Path


def _load_stamp_module():
    script = Path(__file__).resolve().parents[2] / "scripts/update_python_wheel_stamp.py"
    spec = importlib.util.spec_from_file_location("update_python_wheel_stamp", script)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"could not load {script}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_stamp_hash_includes_unstaged_and_untracked_files(tmp_path):
    repo = tmp_path / "repo"
    source = repo / "python/src/example/__init__.py"
    source.parent.mkdir(parents=True)
    source.write_text("value = 1\n", encoding="utf-8")
    subprocess.run(["git", "init", "-q", str(repo)], check=True)
    subprocess.run(["git", "-C", str(repo), "add", "python/src"], check=True)

    module = _load_stamp_module()
    hash_inputs = getattr(module, "_hash_inputs")
    original = hash_inputs(repo, ("python/src",))[0]

    source.write_text("value = 2\n", encoding="utf-8")
    unstaged = hash_inputs(repo, ("python/src",))[0]
    assert unstaged != original

    (source.parent / "new.py").write_text("value = 3\n", encoding="utf-8")
    untracked = hash_inputs(repo, ("python/src",))[0]
    assert untracked != unstaged
