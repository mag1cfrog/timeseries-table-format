# Installation

## Install from PyPI

```bash
pip install timeseries-table-format
```

Requirements:
- Python 3.10+
- Local filesystem (v0 does not support S3/object storage)
- `pyarrow` is required (installed as a dependency)

## Verify installation

```python
import timeseries_table_format as ttf

sess = ttf.Session()
out = sess.sql("select 1 as x")
print(out)
```

If you see a `pyarrow.Table` printed, you’re good to go.

!!! tip "Notebook display"
    In IPython/Jupyter (including VS Code notebooks), `pyarrow.Table` results display as a
    bounded HTML preview by default. The return type is still a real `pyarrow.Table` — the display
    is just a convenience rendering.

??? note "Notebook display options (expand)"
    **Opt-out:**

    ```bash
    TTF_NOTEBOOK_DISPLAY=0  # set before importing
    ```
    or call `timeseries_table_format.disable_notebook_display()` at runtime.

    **Control appearance:**

    ```python
    import timeseries_table_format as ttf
    ttf.enable_notebook_display(max_rows=50, max_cols=20, max_cell_chars=500, align="left")
    ```

    Environment variable shortcuts (set before importing):

    | Variable | Values | Effect |
    |---|---|---|
    | `TTF_NOTEBOOK_DISPLAY` | `0` / `1` | Disable or enable display |
    | `TTF_NOTEBOOK_ALIGN` | `auto` / `left` / `right` | Table alignment |
    | `TTF_NOTEBOOK_CONFIG` | path to a TOML file | Load all defaults from a config file |

    On Python 3.10, install `tomli` to enable TOML config parsing.

!!! note
    If `pip install` tries to compile from source instead of downloading a wheel,
    you’ll need a Rust toolchain available. For most users on supported platforms,
    PyPI wheels should avoid that.
