# timeseries-table-format

Append-only time-series table format with gap/overlap tracking.

This is the main entry point crate. It re-exports `timeseries-table-core`,
including its optional DataFusion SQL integration.

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
