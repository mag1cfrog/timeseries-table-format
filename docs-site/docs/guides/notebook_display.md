# Configure notebook display

In Jupyter, IPython, and VS Code notebooks, `pyarrow.Table` results use a
bounded HTML preview by default. The result remains a normal `pyarrow.Table`.

## Change the preview

Call `enable_notebook_display` to change the row, column, cell-length, or
alignment limits:

```python
import timeseries_table_format as ttf

ttf.enable_notebook_display(
    max_rows=50,
    max_cols=20,
    max_cell_chars=500,
    align="auto",
)
```

The defaults are 20 rows, 50 columns, 2,000 characters per cell, and right
alignment. Use `align="auto"` to left-align text and right-align numbers.

## Disable the preview

Disable it for the current Python process:

```python
ttf.disable_notebook_display()
```

To disable it before import, set this environment variable:

```bash
export TTF_NOTEBOOK_DISPLAY=0
```

## Load settings from TOML

Create a configuration file:

```toml
[notebook_display]
max_rows = 20
max_cols = 50
max_cell_chars = 2000
align = "auto"
```

Then load it before importing the package:

```bash
export TTF_NOTEBOOK_CONFIG=path/to/ttf.toml
```

Alternatively, load it at runtime with
`ttf.load_notebook_display_config("path/to/ttf.toml")`.

On Python 3.10, TOML configuration requires the optional `tomli` package.
