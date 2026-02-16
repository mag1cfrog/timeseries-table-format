import html as _html
import builtins
import os
import sys
from typing import Callable, Any
from dataclasses import dataclass, field

import pyarrow as pa


@dataclass
class _NotebookDisplayConfig:
    max_rows: int = 20
    max_cols: int = 50
    max_cell_chars: int = 2000
    align: str = "right"  # "right" | "auto" | "left"


@dataclass
class _NotebookDisplayState:
    enabled: bool = False
    config: _NotebookDisplayConfig = field(default_factory=_NotebookDisplayConfig)
    our_printer: Callable[[pa.Table], str] | None = None
    had_prev_printer: bool = False
    prev_printer: Callable[[Any], str] | None = None


_STATE = _NotebookDisplayState()

_SCROLL_Y_THRESHOLD_ROWS = 15
_SCROLL_Y_MAX_HEIGHT_PX = 420


def _truthy_env(value: str | None) -> bool:
    if value is None:
        return False
    v = value.strip().lower()
    return v in {"1", "true", "yes", "y", "on"}


def _falsey_env(value: str | None) -> bool:
    if value is None:
        return False
    v = value.strip().lower()
    return v in {"0", "false", "no", "n", "off"}


def _auto_enabled_by_default() -> bool:
    # On-by-default in notebooks; allow explicit opt-out.
    # If set to a truthy value, we still enable; if set to falsey, we disable.
    env = os.getenv("TTF_NOTEBOOK_DISPLAY")
    if _falsey_env(env):
        return False
    return True


def _get_ipython_html_formatter() -> Any | None:
    # Avoid importing IPython at import-time in non-notebook contexts.
    # Prefer a builtins-provided `get_ipython` (present when actually running under IPython),
    # then fall back to a pre-imported `IPython` module if it already exists.
    get_ipython = getattr(builtins, "get_ipython", None)
    if get_ipython is None:
        ipy = sys.modules.get("IPython")
        if ipy is None:
            return None
        get_ipython = getattr(ipy, "get_ipython", None)
        if get_ipython is None:
            return None

    try:
        shell = get_ipython()
    except Exception:
        return None
    if shell is None:
        return None

    display_formatter = getattr(shell, "display_formatter", None)
    if display_formatter is None:
        return None

    formatters = getattr(display_formatter, "formatters", None)
    if not isinstance(formatters, dict):
        return None

    return formatters.get("text/html")


def _safe_int(value: int, *, default: int) -> int:
    try:
        iv = int(value)
    except Exception:
        return default
    return iv if iv > 0 else default


def _truncate(s: str, max_chars: int) -> str:
    if max_chars <= 0:
        return ""
    return s if len(s) <= max_chars else s[:max_chars]


def _truncate_with_flag(s: str, max_chars: int) -> tuple[str, bool]:
    if max_chars <= 0:
        return "", bool(s)
    if len(s) <= max_chars:
        return s, False
    return s[:max_chars], True


def _format_cell_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        # Stable + readable; truncation handled later.
        return value.hex()
    if isinstance(value, (dict, list, tuple, set)):
        return repr(value)
    return str(value)


def _is_numeric_arrow_type(t: pa.DataType) -> bool:
    return bool(
        pa.types.is_integer(t) or pa.types.is_floating(t) or pa.types.is_decimal(t)
    )


def _normalize_align(value: str | None) -> str:
    if value is None:
        return "right"
    v = value.strip().lower()
    if v in {"auto", "best", "mixed", "smart"}:
        return "auto"
    if v in {"left", "l"}:
        return "left"
    if v in {"right", "r"}:
        return "right"
    return "right"


def _head_tail_counts(total: int, limit: int) -> tuple[int, int, bool]:
    if total <= limit:
        return total, 0, False
    head = (limit + 1) // 2
    tail = limit // 2
    gap = head > 0 and tail > 0
    return head, tail, gap


def _head_tail_indices(total: int, limit: int) -> tuple[list[int], int, int, bool]:
    head, tail, gap = _head_tail_counts(total, limit)
    idx: list[int] = list(range(head))
    if tail:
        idx.extend(range(total - tail, total))
    return idx, head, tail, gap


_COL_MIN_CH_NUM = 6
_COL_MIN_CH_OTHER = 12
_COL_MAX_CH = 64


_HEADER_DISPLAY_MAX_CHARS = 64
_HEADER_TITLE_MAX_CHARS = 256


def _cell_len_cap(value: Any, *, cap: int) -> int:
    # Return a cheap-ish estimate of the displayed content length, capped.
    # Avoid `repr(...)` for nested containers since it can be arbitrarily large.
    if value is None:
        return 4  # "null"
    if isinstance(value, str):
        return min(len(value), cap)
    if isinstance(value, bytes):
        # Rendered as lowercase hex.
        return min(len(value) * 2, cap)
    if isinstance(value, (list, tuple, dict, set)):
        return cap
    return min(len(str(value)), cap)


def _ellipsize_middle(s: str, *, max_chars: int) -> str:
    if max_chars <= 0:
        return ""
    if len(s) <= max_chars:
        return s
    if max_chars <= 1:
        return "…"
    # Keep both ends; helps distinguish similar prefixes in nested types.
    head = (max_chars - 1) // 2
    tail = max_chars - 1 - head
    return s[:head] + "…" + s[-tail:]


def _column_width_ch(
    name: str, type_str: str, values: list[Any], *, numeric: bool
) -> int:
    # Size columns primarily from header content, but also consider previewed values
    # so long strings don't collapse to the minimum width.
    sample_len = 0
    for v in values:
        sample_len = max(sample_len, _cell_len_cap(v, cap=_COL_MAX_CH))
        if sample_len >= _COL_MAX_CH:
            break
    content_len = max(len(name), len(type_str), sample_len)
    min_w = _COL_MIN_CH_NUM if numeric else _COL_MIN_CH_OTHER
    return max(min_w, min(content_len + 2, _COL_MAX_CH))


def render_arrow_table_html(
    table: pa.Table,
    *,
    max_rows: int = 20,
    max_cols: int = 50,
    max_cell_chars: int = 2000,
    align: str = "right",
) -> str:
    max_rows = _safe_int(max_rows, default=20)
    max_cols = _safe_int(max_cols, default=50)
    max_cell_chars = _safe_int(max_cell_chars, default=2000)
    align = _normalize_align(align)

    total_rows = int(table.num_rows)
    total_cols = int(table.num_columns)

    if total_cols == 0:
        cols_shown = 0
    else:
        cols_shown = min(total_cols, max_cols)

    row_indices, row_head, row_tail, row_gap = _head_tail_indices(total_rows, max_rows)
    col_indices, col_head, col_tail, col_gap = _head_tail_indices(total_cols, max_cols)

    rows_shown = row_head + row_tail

    preview = table
    if col_indices:
        preview = table.select(col_indices)
    elif total_cols == 0:
        preview = table

    schema = preview.schema
    col_names = list(preview.column_names)
    fields = list(schema)
    col_types = [str(f.type) for f in fields]
    row_idx_arr = pa.array(row_indices, type=pa.int64())

    # Convert bounded preview to Python values. Use per-column take/to_pylist to preserve
    # duplicate column names (to_pydict() collapses duplicates) and support head/tail rows.
    col_values: list[list[Any]] = [
        col.take(row_idx_arr).to_pylist() for col in preview.columns
    ]

    # Precompute numeric columns for CSS classing.
    numeric_cols = [_is_numeric_arrow_type(f.type) for f in fields]

    # Build HTML.
    style = """
<style>
.ttf-arrow-preview {
  /* Theme-aware defaults: prefers VS Code/Jupyter variables when available. */
  --ttf-bg: var(--vscode-editor-background, #ffffff);
  --ttf-fg: var(--vscode-editor-foreground, #111827);
  --ttf-muted: var(--vscode-descriptionForeground, #64748b);
  --ttf-border: rgba(127, 127, 127, 0.35);
  --ttf-grid: rgba(127, 127, 127, 0.18);
  --ttf-head-bg: rgba(127, 127, 127, 0.10);
  --ttf-zebra: rgba(127, 127, 127, 0.05);
  --ttf-hover: rgba(127, 127, 127, 0.08);

  font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif;
  font-size: 12px;
  line-height: 1.35;
  color: var(--ttf-fg);
}

@media (prefers-color-scheme: dark) {
  .ttf-arrow-preview {
    --ttf-bg: var(--vscode-editor-background, #0b1220);
    --ttf-fg: var(--vscode-editor-foreground, #e5e7eb);
    --ttf-muted: var(--vscode-descriptionForeground, #94a3b8);
    --ttf-border: rgba(160, 160, 160, 0.35);
    --ttf-grid: rgba(160, 160, 160, 0.18);
    --ttf-head-bg: rgba(255, 255, 255, 0.07);
    --ttf-zebra: rgba(255, 255, 255, 0.04);
    --ttf-hover: rgba(255, 255, 255, 0.06);
  }
}

.ttf-arrow-preview .ttf-wrap {
  display: inline-block;
  max-width: 100%;
  overflow-x: auto;
  overflow-y: hidden;
  border: 1px solid var(--ttf-border);
  border-radius: 10px;
  background: var(--ttf-bg);
  box-shadow: 0 1px 2px rgba(0, 0, 0, 0.08);
  -webkit-overflow-scrolling: touch;
  scrollbar-gutter: stable both-edges;
  /* Give room for overlay scrollbars / rounded corners so the last column doesn't get clipped. */
  padding-bottom: 8px;
  box-sizing: border-box;
}

.ttf-arrow-preview .ttf-wrap.ttf-scroll-y {
  max-height: var(--ttf-max-height, 420px);
  overflow-y: auto;
}

.ttf-arrow-preview table {
  border-collapse: separate;
  border-spacing: 0;
  table-layout: fixed;
  /* Width is set via an inline CSS variable to avoid long cell values expanding the table. */
  width: var(--ttf-table-width, auto);
  margin-right: 12px;
}

.ttf-arrow-preview th,
.ttf-arrow-preview td {
  padding: 6px 10px;
  border-bottom: 1px solid var(--ttf-grid);
  white-space: nowrap;
  vertical-align: top;
  overflow: hidden;
  text-overflow: ellipsis;
}

.ttf-arrow-preview.ttf-align-right th,
.ttf-arrow-preview.ttf-align-right td {
  text-align: right;
}

.ttf-arrow-preview.ttf-align-left th,
.ttf-arrow-preview.ttf-align-left td {
  text-align: left;
}

.ttf-arrow-preview.ttf-align-auto th,
.ttf-arrow-preview.ttf-align-auto td {
  text-align: left;
}

.ttf-arrow-preview.ttf-align-auto th.ttf-num,
.ttf-arrow-preview.ttf-align-auto td.ttf-num {
  text-align: right;
}

.ttf-arrow-preview thead th {
  position: sticky;
  top: 0;
  z-index: 1;
  background: var(--ttf-head-bg);
  border-bottom: 1px solid var(--ttf-border);
  font-weight: 600;
}

.ttf-arrow-preview thead th .ttf-colname {
  display: block;
  white-space: nowrap;
}

.ttf-arrow-preview thead th .ttf-coltype {
  display: block;
  margin-top: 2px;
  font-size: 11px;
  font-weight: 400;
  color: var(--ttf-muted);
  white-space: nowrap;
}

.ttf-arrow-preview th.ttf-gap,
.ttf-arrow-preview td.ttf-gap {
  text-align: center !important;
  color: var(--ttf-muted);
  font-style: italic;
}

.ttf-arrow-preview .ttf-null {
  color: var(--ttf-muted);
  font-style: italic;
}

.ttf-arrow-preview .ttf-cell[data-truncated="1"]::after {
  content: "…";
  padding-left: 0.25ch;
  color: var(--ttf-muted);
  user-select: none;
}

.ttf-arrow-preview tbody tr.ttf-gap-row td {
  text-align: center !important;
  color: var(--ttf-muted);
  font-style: italic;
}

.ttf-arrow-preview tbody tr:nth-child(even) {
  background: var(--ttf-zebra);
}

.ttf-arrow-preview tbody tr:hover {
  background: var(--ttf-hover);
}

.ttf-arrow-preview td.ttf-num {
  font-variant-numeric: tabular-nums;
}

.ttf-arrow-preview .ttf-footer {
  margin-top: 6px;
  color: var(--ttf-muted);
  text-align: left;
}

.ttf-arrow-preview .ttf-meta {
  color: var(--ttf-muted);
  font-size: 11px;
}
</style>
""".strip()

    # Header
    col_widths = [
        _column_width_ch(
            col_names[i], col_types[i], col_values[i], numeric=numeric_cols[i]
        )
        for i in range(len(col_names))
    ]
    table_width_ch = sum(col_widths) + (3 if col_gap else 0)
    table_style = f' style="--ttf-table-width:{table_width_ch}ch"'

    colgroup_parts: list[str] = ["<colgroup>"]
    for i in range(len(col_names)):
        colgroup_parts.append(f'<col style="width:{col_widths[i]}ch" />')
        if col_gap and i == col_head - 1:
            colgroup_parts.append('<col style="width:3ch" />')
    colgroup_parts.append("</colgroup>")
    colgroup = "".join(colgroup_parts)

    header_cells: list[str] = []
    for idx, (name, type_str) in enumerate(zip(col_names, col_types)):
        name_disp = _ellipsize_middle(name, max_chars=_HEADER_DISPLAY_MAX_CHARS)
        type_disp = _ellipsize_middle(type_str, max_chars=_HEADER_DISPLAY_MAX_CHARS)
        title_raw = _ellipsize_middle(
            f"{name} ({type_str})", max_chars=_HEADER_TITLE_MAX_CHARS
        )

        name_esc = _html.escape(name_disp, quote=True)
        type_esc = _html.escape(type_disp, quote=True)
        title = _html.escape(title_raw, quote=True)
        cls = "ttf-num" if numeric_cols[idx] else ""
        cls_attr = f' class="{cls}"' if cls else ""
        header_cells.append(
            f'<th{cls_attr} title="{title}">'
            f'<span class="ttf-colname">{name_esc}</span>'
            f'<span class="ttf-coltype">{type_esc}</span>'
            f"</th>"
        )
        if col_gap and idx == col_head - 1:
            header_cells.append(
                '<th class="ttf-gap" title="(columns omitted)"><span class="ttf-colname">…</span><span class="ttf-coltype"></span></th>'
            )
    header_html = "<tr>" + "".join(header_cells) + "</tr>"

    # Body rows
    body_rows: list[str] = []
    value_i = 0

    def _append_row(*, is_gap: bool) -> None:
        nonlocal value_i
        tds: list[str] = []
        for j in range(cols_shown):
            if is_gap:
                tds.append('<td class="ttf-gap">…</td>')
                if col_gap and j == col_head - 1:
                    tds.append('<td class="ttf-gap">…</td>')
                continue
            raw = col_values[j][value_i] if value_i < len(col_values[j]) else None
            if raw is None:
                s = '<span class="ttf-null">null</span>'
            else:
                cell_text = _format_cell_value(raw)
                cell_text, truncated = _truncate_with_flag(cell_text, max_cell_chars)
                cell_text = _html.escape(cell_text, quote=True)
                trunc_attr = ' data-truncated="1"' if truncated else ""
                s = f'<span class="ttf-cell"{trunc_attr}>{cell_text}</span>'
            cls = "ttf-num" if numeric_cols[j] else ""
            cls_attr = f' class="{cls}"' if cls else ""
            tds.append(f"<td{cls_attr}>{s}</td>")
            if col_gap and j == col_head - 1:
                tds.append('<td class="ttf-gap">…</td>')
        if not is_gap:
            value_i += 1
        tr_cls = ' class="ttf-gap-row"' if is_gap else ""
        body_rows.append(f"<tr{tr_cls}>" + "".join(tds) + "</tr>")

    for _ in range(row_head):
        _append_row(is_gap=False)
    if row_gap:
        _append_row(is_gap=True)
    for _ in range(row_tail):
        _append_row(is_gap=False)
    body_html = "".join(body_rows)

    rows_previewed = min(total_rows, max_rows)
    cols_previewed = min(total_cols, max_cols)

    footer = (
        f'<div class="ttf-footer">'
        f"Showing <b>{rows_previewed}</b> of <b>{total_rows}</b> rows"
        + (f" ({row_head} head + {row_tail} tail)" if row_gap else "")
        + ", "
        f"<b>{cols_previewed}</b> of <b>{total_cols}</b> columns"
        + (f" ({col_head} left + {col_tail} right)" if col_gap else "")
        + "."
        f' <span class="ttf-meta">(max_rows={max_rows}, max_cols={max_cols}, max_cell_chars={max_cell_chars})</span>'
        f"</div>"
    )

    wrap_cls = "ttf-wrap"
    wrap_style = ""
    tall = rows_shown >= _SCROLL_Y_THRESHOLD_ROWS
    if tall:
        wrap_cls += " ttf-scroll-y"
        wrap_style = f' style="--ttf-max-height: {_SCROLL_Y_MAX_HEIGHT_PX}px;"'

    if cols_shown == 0:
        empty_style = ' style="padding:10px;"'
        if tall:
            empty_style = (
                f' style="padding:10px; --ttf-max-height: {_SCROLL_Y_MAX_HEIGHT_PX}px;"'
            )
        empty = (
            f'<div class="ttf-arrow-preview ttf-align-{align}">{style}'
            f'<div class="{wrap_cls}"{empty_style}>(No columns)</div>{footer}</div>'
        )
        return empty

    return (
        f'<div class="ttf-arrow-preview ttf-align-{align}">{style}'
        f'<div class="{wrap_cls}"{wrap_style}><table{table_style}>'
        f"{colgroup}"
        f"<thead>{header_html}</thead>"
        f"<tbody>{body_html}</tbody>"
        f"</table></div>"
        f"{footer}</div>"
    )


def _make_printer() -> Callable[[pa.Table], str]:
    # Intentionally reads from `_STATE.config` at call time so that repeated calls to
    # `enable_notebook_display(...)` update behavior without re-registering a printer.
    def _printer(table: pa.Table) -> str:
        cfg = _STATE.config
        return render_arrow_table_html(
            table,
            max_rows=cfg.max_rows,
            max_cols=cfg.max_cols,
            max_cell_chars=cfg.max_cell_chars,
            align=cfg.align,
        )

    return _printer


def _install_into_html_formatter(
    html_formatter: Any, *, override_existing: bool
) -> bool:
    if html_formatter is None:
        return False

    type_printers = getattr(html_formatter, "type_printers", None)
    if not isinstance(type_printers, dict):
        return False

    current = type_printers.get(pa.Table)

    # If we already installed ours, nothing to do (unless config changed, which is handled elsewhere).
    if _STATE.our_printer is not None and current is _STATE.our_printer:
        _STATE.enabled = True
        return False

    if (current is not None) and (not override_existing):
        return False

    # Stash prior printer (if any) for restore.
    _STATE.had_prev_printer = current is not None
    _STATE.prev_printer = current

    our = _STATE.our_printer or _make_printer()
    _STATE.our_printer = our

    # Prefer IPython's supported API when present.
    for_type = getattr(html_formatter, "for_type", None)
    if callable(for_type):
        for_type(pa.Table, our)
    else:
        type_printers[pa.Table] = our

    _STATE.enabled = True
    return True


def _uninstall_from_html_formatter(html_formatter: Any) -> bool:
    if html_formatter is None:
        return False

    type_printers = getattr(html_formatter, "type_printers", None)
    if not isinstance(type_printers, dict):
        return False

    if _STATE.our_printer is None:
        _STATE.enabled = False
        return False

    current = type_printers.get(pa.Table)
    if current is not _STATE.our_printer:
        # Don't clobber another library's printer if we aren't the active one.
        _STATE.enabled = False
        return False

    if _STATE.had_prev_printer and _STATE.prev_printer is not None:
        type_printers[pa.Table] = _STATE.prev_printer
    else:
        type_printers.pop(pa.Table, None)

    _STATE.enabled = False
    return True


def enable_notebook_display(
    *,
    max_rows: int = 20,
    max_cols: int = 50,
    max_cell_chars: int = 2000,
    align: str = "right",
) -> bool:
    _STATE.config = _NotebookDisplayConfig(
        max_rows=_safe_int(max_rows, default=20),
        max_cols=_safe_int(max_cols, default=50),
        max_cell_chars=_safe_int(max_cell_chars, default=2000),
        align=_normalize_align(align),
    )

    html_formatter = _get_ipython_html_formatter()
    # Explicit enable: override any existing HTML formatter for pa.Table.
    changed = _install_into_html_formatter(html_formatter, override_existing=True)
    # If we were already installed, config may still have changed; in that case we return True.
    if not changed and (_STATE.our_printer is not None):
        current = (
            getattr(html_formatter, "type_printers", {}).get(pa.Table)
            if html_formatter is not None
            else None
        )
        if current is _STATE.our_printer:
            return True
    return changed


def disable_notebook_display() -> bool:
    html_formatter = _get_ipython_html_formatter()
    changed = _uninstall_from_html_formatter(html_formatter)
    return changed


def _auto_enable_notebook_display() -> bool:
    if not _auto_enabled_by_default():
        return False

    align_env = os.getenv("TTF_NOTEBOOK_ALIGN")
    if align_env is not None:
        _STATE.config.align = _normalize_align(align_env)

    html_formatter = _get_ipython_html_formatter()
    if html_formatter is None:
        return False

    return _install_into_html_formatter(html_formatter, override_existing=False)
