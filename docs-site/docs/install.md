# Installation

## Install from PyPI

```bash
pip install timeseries-table-format
```

Requirements:

- Python 3.10+
- `pyarrow` is required (installed as a dependency)

Published wheels use CPython's stable ABI with a Python 3.10 minimum. Release
CI builds wheels for Linux x86_64, macOS arm64, macOS x86_64, and Windows
x86_64, and tests the Linux wheel on CPython 3.10 through 3.14.

The published wheels support ordinary GIL-enabled CPython. Free-threaded
CPython, PyPy, and other Python implementations are not currently supported.

## Verify installation

```python
import timeseries_table_format as ttf

sess = ttf.Session()
out = sess.sql("select 1 as x")
print(out)
```

If you see a `pyarrow.Table`, the package is ready to use.

!!! note
    If `pip install` tries to compile from source instead of downloading a wheel,
    you need a Rust toolchain. See [Troubleshooting](troubleshooting.md#pip-install-tries-to-build-from-source).

Next, [create and query your first table](tutorials/create_append_query.md).
