# timeseries-table-format

Append-only time-series table format with gap/overlap tracking.

This is the main entry point crate that re-exports:
- `timeseries-table-core` — core storage + coverage logic
- `timeseries-table-datafusion` — DataFusion SQL integration (optional)

## Installation

```bash
cargo add timeseries-table-format
```

## Features

| Feature | Default | Description |
|---------|---------|-------------|
| `datafusion` | ✓ | DataFusion TableProvider for SQL queries |

## Usage

```rust
use timeseries_table_format::TimeSeriesTable;
```

For more documentation and examples, see the repository README:
https://github.com/mag1cfrog/timeseries-table-format
