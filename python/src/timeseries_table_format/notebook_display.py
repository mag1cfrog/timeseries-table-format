import html as _html
import os
from typing import Callable, Any
from dataclasses import dataclass, field

import pyarrow as pa


@dataclass
class _NotebookDisplayConfig:
    max_rows: int = 20
    max_cols: int = 50
    max_cell_chars: int = 200
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
    try:
        from IPython import get_ipython  # type: ignore[import-not-found]
    except Exception:
        return None

    shell = get_ipython()
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


def _column_width_ch(name: str, type_str: str) -> int:
    # Approximate a sensible column width from the header content length.
    # Keep it bounded so a single long name/type doesn't blow out the layout.
    content_len = max(len(name), len(type_str))
    return max(6, min(content_len + 2, 32))


def render_arrow_table_html(
    table: pa.Table,
    *,
    max_rows: int = 20,
    max_cols: int = 50,
    max_cell_chars: int = 200,
    align: str = "right",
) -> str:
    max_rows = _safe_int(max_rows, default=20)
    max_cols = _safe_int(max_cols, default=50)
    max_cell_chars = _safe_int(max_cell_chars, default=200)
    align = _normalize_align(align)

    total_rows = int(table.num_rows)
    total_cols = int(table.num_columns)

    rows_shown = min(total_rows, max_rows)
    cols_shown = min(total_cols, max_cols)

    preview = table.slice(0, rows_shown)
    if total_cols > cols_shown:
        preview = preview.select(preview.column_names[:cols_shown])

    schema = preview.schema
    col_names = list(preview.column_names)
    col_types = [str(schema.field(n).type) for n in col_names]
    col_widths = [_column_width_ch(n, t) for n, t in zip(col_names, col_types)]

    # Convert bounded preview to Python values.
    data = preview.to_pydict()

    # Precompute numeric columns for CSS classing.
    numeric_cols: set[str] = set()
    for field in schema:
        if _is_numeric_arrow_type(field.type):
            numeric_cols.add(field.name)

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
  width: max-content;
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
    colgroup = (
        "<colgroup>"
        + "".join(f'<col style="width:{w}ch" />' for w in col_widths)
        + "</colgroup>"
    )

    header_cells: list[str] = []
    for name, type_str in zip(col_names, col_types):
        name_esc = _html.escape(name, quote=True)
        type_esc = _html.escape(type_str, quote=True)
        title = _html.escape(f"{name} ({type_str})", quote=True)
        cls = "ttf-num" if name in numeric_cols else ""
        cls_attr = f' class="{cls}"' if cls else ""
        header_cells.append(
            f"<th{cls_attr} title=\"{title}\">"
            f'<span class="ttf-colname">{name_esc}</span>'
            f'<span class="ttf-coltype">{type_esc}</span>'
            f"</th>"
        )
    header_html = "<tr>" + "".join(header_cells) + "</tr>"

    # Body rows
    body_rows: list[str] = []
    for i in range(rows_shown):
        tds: list[str] = []
        for name in col_names:
            raw = data[name][i] if name in data else None
            s = _format_cell_value(raw)
            s = _truncate(s, max_cell_chars)
            s = _html.escape(s, quote=True)
            cls = "ttf-num" if name in numeric_cols else ""
            cls_attr = f' class="{cls}"' if cls else ""
            tds.append(f"<td{cls_attr}>{s}</td>")
        body_rows.append("<tr>" + "".join(tds) + "</tr>")
    body_html = "".join(body_rows)

    footer = (
        f'<div class="ttf-footer">'
        f"Showing <b>{rows_shown}</b> of <b>{total_rows}</b> rows, "
        f"<b>{cols_shown}</b> of <b>{total_cols}</b> columns."
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
            empty_style = f' style="padding:10px; --ttf-max-height: {_SCROLL_Y_MAX_HEIGHT_PX}px;"'
        empty = (
            f'<div class="ttf-arrow-preview ttf-align-{align}">{style}'
            f'<div class="{wrap_cls}"{empty_style}>(No columns)</div>{footer}</div>'
        )
        return empty

    return (
        f'<div class="ttf-arrow-preview ttf-align-{align}">{style}'
        f'<div class="{wrap_cls}"{wrap_style}><table>'
        f"{colgroup}"
        f"<thead>{header_html}</thead>"
        f"<tbody>{body_html}</tbody>"
        f"</table></div>"
        f"{footer}</div>"
    )


def _make_printer() -> Callable[[pa.Table], str]:
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
    max_cell_chars: int = 200,
    align: str = "right",
) -> bool:
    _STATE.config = _NotebookDisplayConfig(
        max_rows=_safe_int(max_rows, default=20),
        max_cols=_safe_int(max_cols, default=50),
        max_cell_chars=_safe_int(max_cell_chars, default=200),
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

    # Auto-enable: do not override an existing pa.Table HTML printer.
    if _STATE.our_printer is None:
        _STATE.our_printer = _make_printer()

    return _install_into_html_formatter(html_formatter, override_existing=False)
