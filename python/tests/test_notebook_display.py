from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

import pyarrow as pa

import timeseries_table_format.notebook_display as nd


@dataclass
class FakeHTMLFormatter:
    type_printers: dict[type, Callable[[Any], str]]

    def for_type(self, typ: type, func: Callable[[Any], str]) -> None:
        self.type_printers[typ] = func


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


def test_render_html_adds_vertical_scroll_class_for_many_rows():
    t = pa.table({"x": list(range(20))})
    html = nd.render_arrow_table_html(t, max_rows=20, max_cols=20, max_cell_chars=30)
    assert "ttf-scroll-y" in html

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
