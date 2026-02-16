from __future__ import annotations

import types
from dataclasses import dataclass
from typing import Any, Callable

import pyarrow as pa
import timeseries_table_format.notebook_display as nd


@dataclass
class FakeHTMLFormatter:
    type_printers: dict[type, Callable[[Any], str]]

    def for_type(self, typ: type, func: Callable[[Any], str]) -> None:
        self.type_printers[typ] = func


@dataclass
class FakeDisplayFormatter:
    formatters: dict[str, Any]


@dataclass
class FakeShell:
    display_formatter: FakeDisplayFormatter


def test_render_html_escapes_and_truncates():
    t = pa.table({"x": ["<b>hello</b>"], "y": [123]})
    html = nd.render_arrow_table_html(t, max_rows=20, max_cols=20, max_cell_chars=5)
    assert "text-overflow: ellipsis" in html
    assert "&lt;b&gt;h" in html
    assert "hello" not in html
    assert "ttf-align-right" in html


def test_render_html_align_modes_set_container_class():
    t = pa.table({"x": ["a"], "y": [1]})
    assert "ttf-align-right" in nd.render_arrow_table_html(t, align="right")
    assert "ttf-align-left" in nd.render_arrow_table_html(t, align="left")
    assert "ttf-align-auto" in nd.render_arrow_table_html(t, align="auto")
    assert "ttf-align-right" in nd.render_arrow_table_html(t, align="nonsense")


def test_render_html_adds_vertical_scroll_class_for_many_rows():
    t = pa.table({"x": list(range(20))})
    html = nd.render_arrow_table_html(t, max_rows=20, max_cols=20, max_cell_chars=30)
    assert "ttf-scroll-y" in html


def test_render_html_truncates_columns():
    t = pa.table({f"c{i}": [i] for i in range(60)})
    html = nd.render_arrow_table_html(t, max_rows=20, max_cols=50, max_cell_chars=2000)
    assert html.count("<th ") == 51
    assert 'class="ttf-gap"' in html
    assert ", <b>50</b> of <b>60</b> columns" in html
    assert "(25 left + 25 right)" in html


def test_render_html_rows_head_tail_preview():
    t = pa.table({"x": [f"r{i}" for i in range(30)]})
    html = nd.render_arrow_table_html(t, max_rows=20, max_cols=50, max_cell_chars=2000)
    assert ">r0</span>" in html
    assert ">r29</span>" in html
    assert "r15" not in html
    assert 'class="ttf-gap-row"' in html
    assert "Showing <b>20</b> of <b>30</b> rows" in html
    assert "(10 head + 10 tail)" in html


def test_render_html_rows_odd_split_is_balanced():
    t = pa.table({"x": [f"r{i}" for i in range(10)]})
    html = nd.render_arrow_table_html(t, max_rows=5, max_cols=50, max_cell_chars=2000)
    assert ">r0</span>" in html
    assert ">r9</span>" in html
    assert "r5" not in html
    assert "(3 head + 2 tail)" in html


def test_render_html_cols_head_tail_preview():
    t = pa.table({f"c{i}": [i] for i in range(30)})
    html = nd.render_arrow_table_html(t, max_rows=20, max_cols=20, max_cell_chars=2000)
    assert 'class="ttf-colname">c0</span>' in html
    assert 'class="ttf-colname">c29</span>' in html
    assert "c15" not in html
    assert 'class="ttf-gap"' in html
    assert ", <b>20</b> of <b>30</b> columns" in html
    assert "(10 left + 10 right)" in html


def test_render_html_cols_odd_split_is_balanced():
    t = pa.table({f"c{i}": [i] for i in range(10)})
    html = nd.render_arrow_table_html(t, max_rows=20, max_cols=5, max_cell_chars=2000)
    assert 'class="ttf-colname">c0</span>' in html
    assert 'class="ttf-colname">c9</span>' in html
    assert "c5" not in html
    assert "(3 left + 2 right)" in html


def test_render_html_duplicate_column_names_preserved():
    t = pa.Table.from_arrays([pa.array([1]), pa.array([2])], names=["x", "x"])
    html = nd.render_arrow_table_html(t, max_rows=20, max_cols=50, max_cell_chars=2000)
    assert html.count('class="ttf-colname">x</span>') == 2
    assert html.find('class="ttf-num"><span class="ttf-cell">1</span>') < html.find(
        'class="ttf-num"><span class="ttf-cell">2</span>'
    )


def test_render_html_duplicate_column_names_with_truncation_does_not_error():
    t = pa.Table.from_arrays([pa.array([1]), pa.array([2])], names=["x", "x"])
    html = nd.render_arrow_table_html(t, max_rows=20, max_cols=1, max_cell_chars=2000)
    assert html.count("<th ") == 1
    assert ">1</span>" in html
    assert ">2</span>" not in html


def test_render_html_escapes_quotes_in_cells():
    t = pa.table({"x": ['" onclick="alert(1)']})
    html = nd.render_arrow_table_html(t, max_rows=20, max_cols=50, max_cell_chars=2000)
    assert 'onclick="' not in html
    assert "&quot; onclick=&quot;" in html


def test_render_html_escapes_column_names():
    t = pa.table({'x"><img src=x onerror=alert(1)>': [1]})
    html = nd.render_arrow_table_html(t, max_rows=20, max_cols=50, max_cell_chars=2000)
    assert "<img" not in html
    assert "&lt;img" in html


def test_render_html_bytes_render_as_hex():
    t = pa.table({"x": [b"\x00\xff"]})
    html = nd.render_arrow_table_html(t, max_rows=20, max_cols=50, max_cell_chars=2000)
    assert ">00ff</span>" in html


def test_render_html_sets_truncated_flag_on_cell():
    t = pa.table({"x": ["abcdefghijklmnopqrstuvwxyz"]})
    html = nd.render_arrow_table_html(t, max_rows=20, max_cols=50, max_cell_chars=5)
    assert 'data-truncated="1"' in html


def test_render_html_sets_table_width_var_so_long_cells_dont_expand_layout():
    t = pa.table({"id": [0, 1], "text": ["x" * 10, "z" * 400]})
    html = nd.render_arrow_table_html(t, max_rows=20, max_cols=50, max_cell_chars=2000)
    assert "--ttf-table-width:" in html


def test_render_html_uses_preview_values_to_avoid_string_columns_collapsing_to_min_width():
    t = pa.table(
        {"id": list(range(5)), "text": ["x" * 10, "y" * 50, "z" * 400, "a", "b"]}
    )
    html = nd.render_arrow_table_html(t, max_rows=20, max_cols=50, max_cell_chars=2000)
    import re

    widths = [int(x) for x in re.findall(r'<col style="width:([0-9]+)ch" />', html)]
    assert len(widths) >= 2
    assert widths[1] > widths[0]
    assert widths[1] == 64


def test_render_html_null_distinct_from_empty_string():
    t = pa.table({"x": [None, ""], "y": ["", None]})
    html = nd.render_arrow_table_html(t, max_rows=20, max_cols=50, max_cell_chars=2000)
    assert 'class="ttf-null"' in html
    assert ">null</span>" in html


def test_render_html_truncates_very_long_column_names_and_types_in_header():
    long_name = "col_" + ("x" * 500)
    t = pa.table({long_name: ["a"]})
    html = nd.render_arrow_table_html(t, max_rows=20, max_cols=50, max_cell_chars=2000)
    assert long_name not in html
    assert "…" in html

    # Type strings can also be long (nested structs/lists).
    nested_type = pa.struct([(f"f{i}", pa.list_(pa.string())) for i in range(60)])
    t2 = pa.Table.from_arrays([pa.array([None], type=nested_type)], names=["nested"])
    html2 = nd.render_arrow_table_html(
        t2, max_rows=20, max_cols=50, max_cell_chars=2000
    )
    assert str(nested_type) not in html2
    assert "…" in html2


def test_render_html_invalid_bounds_fall_back_to_defaults():
    t = pa.table({"x": [1]})
    html = nd.render_arrow_table_html(t, max_rows=0, max_cols=0, max_cell_chars=0)
    assert "(max_rows=20, max_cols=50, max_cell_chars=2000)" in html


def test_render_html_zero_rows_still_renders_header_and_footer():
    t = pa.table({"x": pa.array([], type=pa.int64())})
    html = nd.render_arrow_table_html(t, max_rows=20, max_cols=20, max_cell_chars=100)
    assert "Showing" in html
    assert "<thead>" in html


def test_install_and_uninstall_restores_previous_printer():
    prev_called = {"n": 0}

    def prev_printer(_: Any) -> str:
        prev_called["n"] += 1
        return "prev"

    fake = FakeHTMLFormatter(type_printers={pa.Table: prev_printer})

    nd._STATE.enabled = False
    nd._STATE.our_printer = None
    nd._STATE.had_prev_printer = False
    nd._STATE.prev_printer = None

    changed = nd._install_into_html_formatter(fake, override_existing=True)
    assert changed is True
    assert fake.type_printers[pa.Table] is nd._STATE.our_printer

    changed2 = nd._uninstall_from_html_formatter(fake)
    assert changed2 is True
    assert fake.type_printers[pa.Table] is prev_printer

    # sanity: previous printer still callable
    out = fake.type_printers[pa.Table](pa.table({"x": [1]}))
    assert out == "prev"
    assert prev_called["n"] == 1


def test_install_then_uninstall_with_no_previous_printer_removes_entry():
    fake = FakeHTMLFormatter(type_printers={})

    nd._STATE.enabled = False
    nd._STATE.our_printer = None
    nd._STATE.had_prev_printer = False
    nd._STATE.prev_printer = None

    assert nd._install_into_html_formatter(fake, override_existing=True) is True
    assert pa.Table in fake.type_printers
    assert nd._uninstall_from_html_formatter(fake) is True
    assert pa.Table not in fake.type_printers


def test_install_when_already_installed_is_noop():
    fake = FakeHTMLFormatter(type_printers={})

    nd._STATE.enabled = False
    nd._STATE.our_printer = None
    nd._STATE.had_prev_printer = False
    nd._STATE.prev_printer = None

    assert nd._install_into_html_formatter(fake, override_existing=True) is True
    assert nd._install_into_html_formatter(fake, override_existing=True) is False


def test_printer_reads_latest_config_without_reregistration():
    nd._STATE.config.max_rows = 5
    nd._STATE.config.max_cols = 7
    nd._STATE.config.max_cell_chars = 9
    nd._STATE.config.align = "left"
    printer = nd._make_printer()

    nd._STATE.config.max_rows = 11
    nd._STATE.config.max_cols = 13
    nd._STATE.config.max_cell_chars = 15
    nd._STATE.config.align = "auto"

    html = printer(pa.table({"x": [1]}))
    assert "(max_rows=11, max_cols=13, max_cell_chars=15)" in html
    assert "ttf-align-auto" in html


def test_auto_install_does_not_override_existing():
    def existing(_: Any) -> str:
        return "existing"

    fake = FakeHTMLFormatter(type_printers={pa.Table: existing})

    nd._STATE.enabled = False
    nd._STATE.our_printer = None
    nd._STATE.had_prev_printer = False
    nd._STATE.prev_printer = None

    changed = nd._install_into_html_formatter(fake, override_existing=False)
    assert changed is False
    assert fake.type_printers[pa.Table] is existing


def test_uninstall_does_not_clobber_other_printer():
    fake = FakeHTMLFormatter(type_printers={})

    nd._STATE.enabled = False
    nd._STATE.our_printer = None
    nd._STATE.had_prev_printer = False
    nd._STATE.prev_printer = None

    assert nd._install_into_html_formatter(fake, override_existing=True) is True
    our = fake.type_printers[pa.Table]

    def other(_: Any) -> str:
        return "other"

    fake.type_printers[pa.Table] = other

    assert nd._uninstall_from_html_formatter(fake) is False
    assert fake.type_printers[pa.Table] is other
    assert our is not other


def test_auto_enable_respects_env_disable(monkeypatch):
    html_formatter = FakeHTMLFormatter(type_printers={})
    shell = FakeShell(
        display_formatter=FakeDisplayFormatter(formatters={"text/html": html_formatter})
    )

    ipy = types.ModuleType("IPython")
    ipy.get_ipython = lambda: shell  # type: ignore[attr-defined]
    monkeypatch.setitem(__import__("sys").modules, "IPython", ipy)

    monkeypatch.setenv("TTF_NOTEBOOK_DISPLAY", "0")

    nd._STATE.enabled = False
    nd._STATE.our_printer = None
    nd._STATE.had_prev_printer = False
    nd._STATE.prev_printer = None

    assert nd._auto_enable_notebook_display() is False
    assert pa.Table not in html_formatter.type_printers


def test_auto_enable_reads_align_env(monkeypatch):
    html_formatter = FakeHTMLFormatter(type_printers={})
    shell = FakeShell(
        display_formatter=FakeDisplayFormatter(formatters={"text/html": html_formatter})
    )

    ipy = types.ModuleType("IPython")
    ipy.get_ipython = lambda: shell  # type: ignore[attr-defined]
    monkeypatch.setitem(__import__("sys").modules, "IPython", ipy)

    monkeypatch.delenv("TTF_NOTEBOOK_DISPLAY", raising=False)
    monkeypatch.setenv("TTF_NOTEBOOK_ALIGN", "auto")

    nd._STATE.enabled = False
    nd._STATE.our_printer = None
    nd._STATE.had_prev_printer = False
    nd._STATE.prev_printer = None

    assert nd._auto_enable_notebook_display() is True
    printer = html_formatter.type_printers[pa.Table]
    out = printer(pa.table({"x": ["a"], "y": [1]}))
    assert "ttf-align-auto" in out


def test_auto_enable_loads_config_file_from_env(monkeypatch, tmp_path):
    html_formatter = FakeHTMLFormatter(type_printers={})
    shell = FakeShell(
        display_formatter=FakeDisplayFormatter(formatters={"text/html": html_formatter})
    )

    ipy = types.ModuleType("IPython")
    ipy.get_ipython = lambda: shell  # type: ignore[attr-defined]
    monkeypatch.setitem(__import__("sys").modules, "IPython", ipy)

    cfg = tmp_path / "ttf.toml"
    cfg.write_text(
        "\n".join(
            [
                "[notebook_display]",
                "max_rows = 5",
                "max_cols = 7",
                "max_cell_chars = 9",
                "align = \"left\"",
                "col_max_ch = 20",
                "",
            ]
        ),
        encoding="utf-8",
    )

    # Env config loads first; align env overrides file.
    monkeypatch.setenv("TTF_NOTEBOOK_CONFIG", str(cfg))
    monkeypatch.setenv("TTF_NOTEBOOK_ALIGN", "auto")

    # Snapshot + restore globals to avoid cross-test pollution.
    config_before = nd._STATE.config
    tuning_before = vars(nd._TUNING).copy()
    try:
        nd._STATE.enabled = False
        nd._STATE.our_printer = None
        nd._STATE.had_prev_printer = False
        nd._STATE.prev_printer = None
        nd._STATE.config = nd._NotebookDisplayConfig()
        for k, v in tuning_before.items():
            setattr(nd._TUNING, k, v)

        assert nd._auto_enable_notebook_display() is True
        printer = html_formatter.type_printers[pa.Table]
        out = printer(pa.table({"id": [0], "text": ["z" * 200]}))
        assert "(max_rows=5, max_cols=7, max_cell_chars=9)" in out
        assert "ttf-align-auto" in out

        import re

        widths = [int(x) for x in re.findall(r'<col style="width:([0-9]+)ch" />', out)]
        assert len(widths) >= 2
        assert widths[1] == 20
    finally:
        nd._STATE.config = config_before
        for k, v in tuning_before.items():
            setattr(nd._TUNING, k, v)
