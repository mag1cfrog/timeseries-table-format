# timeseries-table-format

Append-only time-series table format with gap/overlap tracking.

This is the main entry point crate that re-exports:
- [`timeseries-table-core`](../timeseries-table-core) — Core storage and coverage logic
- [`timeseries-table-datafusion`](../timeseries-table-datafusion) — DataFusion SQL integration (optional)

## Installation

```bash
cargo add timeseries-table-format
```

## Features

| Feature | Default | Description |
|---------|---------|-------------|
| `datafusion` | ✓ | DataFusion TableProvider for SQL queries |
| `cli` | | Include CLI tool |

## Usage

```rust
use timeseries_table_format::TimeSeriesTable;
```

See the [main README](../../README.md) for full documentation.
