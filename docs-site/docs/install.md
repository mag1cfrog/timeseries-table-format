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
    In IPython/Jupyter (including VS Code notebooks), `pyarrow.Table` results display as a bounded HTML preview by default (the return type is still a real `pyarrow.Table`).
    You can control alignment with `TTF_NOTEBOOK_ALIGN=auto|left|right` (set before importing `timeseries_table_format`).
    You can also load defaults from a TOML config file by setting `TTF_NOTEBOOK_CONFIG=path/to/ttf.toml` before importing `timeseries_table_format` (on Python 3.10, install `tomli` to enable TOML parsing).

!!! note
    If `pip install` tries to compile from source instead of downloading a wheel,
    you’ll need a Rust toolchain available. For most users on supported platforms,
    PyPI wheels should avoid that.
