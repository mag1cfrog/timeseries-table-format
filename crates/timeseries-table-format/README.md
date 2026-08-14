# timeseries-table-format

Append-only time-series table format with gap/overlap tracking.

This is the canonical Rust library. It owns the table engine, storage and
coverage modules, and its optional DataFusion SQL integration.

## Installation

```bash
cargo add timeseries-table-format
```

## Features

| Feature | Default | Description |
|---------|---------|-------------|
| `datafusion` | Yes | DataFusion TableProvider for SQL queries |
| `cli` | No | `tstable` command-line interface |
| `test-counters` | No | Test-only transaction-log counters |

## Usage

```rust
use timeseries_table_format::{TableLocation, TimeSeriesTable};
```

The complete engine remains available through canonical module paths such as
`timeseries_table_format::metadata`, `storage`, `table`, and `transaction_log`.

## Source migration

Users of the retired split crates can migrate mechanically:

```text
timeseries-table-core dependency       -> timeseries-table-format dependency
timeseries_table_core::<module>        -> timeseries_table_format::<module>
timeseries-table-datafusion dependency -> timeseries-table-format dependency with the datafusion feature
timeseries_table_datafusion::<module>  -> timeseries_table_format::datafusion::<module>
cargo install timeseries-table-cli     -> cargo install timeseries-table-format --features cli --bin tstable
```

The `datafusion` feature is enabled by default. The historical split packages
remain available on crates.io for existing users, but receive no new releases.

See the [CLI guide](CLI.md), [engine guide](ENGINE.md), and
[DataFusion guide](DATAFUSION.md) for the full command, module, and query APIs.

For more documentation and examples, see the repository README:
https://github.com/mag1cfrog/timeseries-table-format
