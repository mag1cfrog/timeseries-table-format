from __future__ import annotations

import importlib
from pathlib import Path
from typing import Any


def _parse_toml_bytes(data: bytes) -> dict[str, Any] | None:
    toml: Any
    try:
        toml = importlib.import_module("tomllib")  # py>=3.11
    except Exception:
        try:
            toml = importlib.import_module("tomli")  # optional backport for py3.10
        except Exception:
            return None

    try:
        loads = getattr(toml, "loads", None)
        if not callable(loads):
            return None
        out = loads(data.decode("utf-8"))
    except Exception:
        return None
    return out if isinstance(out, dict) else None


def _extract_notebook_display_section(data: dict[str, Any]) -> dict[str, Any]:
    direct = data.get("notebook_display")
    if isinstance(direct, dict):
        return direct

    ttf = data.get("timeseries_table_format")
    if isinstance(ttf, dict):
        nested = ttf.get("notebook_display")
        if isinstance(nested, dict):
            return nested

    return {}


def load_notebook_display_config_file(path: str) -> dict[str, Any] | None:
    """
    Load a TOML config file and return the `notebook_display` section.

    Supported shapes:

    - `[notebook_display]`
    - `[timeseries_table_format.notebook_display]`

    Returns `None` if the file can't be read or parsed (including when TOML support
    isn't available, e.g. Python 3.10 without `tomli` installed).
    """
    try:
        p = Path(path).expanduser()
        if not p.is_absolute():
            p = Path.cwd() / p
        raw = p.read_bytes()
    except Exception:
        return None

    parsed = _parse_toml_bytes(raw)
    if parsed is None:
        return None

    section = _extract_notebook_display_section(parsed)
    return section
