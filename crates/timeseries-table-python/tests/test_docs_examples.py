from __future__ import annotations

import runpy
from pathlib import Path

import pyarrow as pa

EXAMPLES_DIR = Path(__file__).resolve().parents[1] / "examples"


def test_docs_example_quickstart_runs(tmp_path):
    mod = runpy.run_path(
        str(EXAMPLES_DIR / "quickstart_create_append_query.py"),
        run_name="__doc_example__",
    )
    out = mod["run"](table_root=tmp_path / "table")
    assert isinstance(out, pa.Table)
    assert out.column_names == ["ts", "symbol", "close"]
    assert out.num_rows == 3


def test_docs_example_join_runs(tmp_path):
    mod = runpy.run_path(
        str(EXAMPLES_DIR / "register_and_join_two_tables.py"),
        run_name="__doc_example__",
    )
    out = mod["run"](base_dir=tmp_path)
    assert isinstance(out, pa.Table)
    assert out.column_names == ["ts", "symbol", "close", "volume"]
    assert out.num_rows == 2


def test_docs_example_parameterized_queries_runs():
    mod = runpy.run_path(
        str(EXAMPLES_DIR / "parameterized_queries.py"),
        run_name="__doc_example__",
    )
    outs = mod["run"]()
    assert isinstance(outs, list)
    assert len(outs) == 2
    assert all(isinstance(t, pa.Table) for t in outs)
