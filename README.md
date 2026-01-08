# timeseries-table-format

[![Rust](https://img.shields.io/badge/Developed%20in-Rust-orange?logo=rust)](https://www.rust-lang.org)
![License](https://img.shields.io/badge/license-MIT-informational)
[![](https://github.com/mag1cfrog/timeseries-table-format/actions/workflows/ci.yml/badge.svg)](https://github.com/mag1cfrog/timeseries-table-format/actions/workflows/ci.yml)

<p align="center">
  <img src="docs/assets/ferris-timeseries-3.png" alt="Ferris with timeseries-table-format" width="1920" />
</p>

<h3 align="center">
  <strong>Stop managing Parquet files. Start managing time-series tables.</strong>
</h3>

<p align="center">
  A Rust-native table format that brings Delta Lake/Iceberg-style transactions<br/>
  to time-series data—with built-in coverage tracking for gaps and overlaps.
</p>

---

## ✨ Features

| | |
|---|---|
| 🔄 **ACID-like Transactions** | Append-only commit log with optimistic concurrency control—no more corrupted datasets from failed writes |
| ⏱️ **Time as First-Class Citizen** | Timestamp column, entity partitioning, and configurable bucket granularity baked into the format |
| 🗺️ **Coverage Tracking** | RoaringBitmap indexes answer "where are my gaps?" in milliseconds, not minutes |
| 🚀 **Overlap-Safe Appends** | Automatic detection prevents accidental duplicate data ingestion |
| 🔍 **DataFusion Integration** | SQL queries with time-based segment pruning out of the box |
| 📦 **Zero External Dependencies** | Pure Rust, no JVM, no Python runtime—just cargo install and go |

---

## 🤔 Why not just use Delta Lake / Iceberg?

Great question. You probably *should* use them for general-purpose analytics.

But if you're working with **time-series specifically**, you might have noticed:

| Problem | Delta/Iceberg | This Project |
|---------|---------------|--------------|
| "Do I have data for 2024-01-15 to 2024-03-20?" | Scan metadata or query | coverage.ratio() → instant |
| "Where are the gaps in my dataset?" | Write custom logic | coverage.gaps() → built-in |
| "Will this append overlap existing data?" | Hope for the best | Automatic overlap detection |
| Deployment complexity | JVM/Spark ecosystem | Single Rust binary |

**This project is ideal for:**
- 📈 Backtesting systems that need gap-aware data loading
- 📊 Sensor/IoT data pipelines with strict coverage requirements  
- 💹 Financial data stores where overlap = disaster
- 🧪 Learning how modern table formats work (well-documented internals!)

---

## 🚀 Quick Start

### CLI (Easiest)

```bash
# Install
cargo install --git https://github.com/mag1cfrog/timeseries-table-format timeseries-table-cli

# Create a table with 1-hour buckets
timeseries-table-cli create --table ./my_table --time-column ts --bucket 1h

# Append data (overlap-safe!)
timeseries-table-cli append --table ./my_table --parquet ./data.parquet

# Query with SQL
timeseries-table-cli query --table ./my_table --sql "SELECT * FROM my_table LIMIT 5"
```

See the [CLI documentation](crates/timeseries-table-cli/README.md) for the full command reference.

### Rust API

```toml
[dependencies]
timeseries-table-core = { git = "https://github.com/mag1cfrog/timeseries-table-format" }
```

```rust
use timeseries_table_core::TimeSeriesTable;

// Open and query coverage
let table = TimeSeriesTable::open("./my_table")?;
let coverage = table.coverage()?;

println!("Coverage ratio: {:.1}%", coverage.ratio() * 100.0);
println!("Gaps: {:?}", coverage.gaps());
```

See [timeseries-table-core](crates/timeseries-table-core/README.md) for full API docs.

### DataFusion Integration

```toml
[dependencies]
timeseries-table-datafusion = { git = "https://github.com/mag1cfrog/timeseries-table-format" }
```

See [timeseries-table-datafusion](crates/timeseries-table-datafusion/README.md) for SQL query examples.

---

## 🏗️ Architecture

<p align="center">
  <img src="docs/assets/high-level-architecture-2.png" alt="high level architecture" width="1920" />
</p>

<details>
<summary><strong>Click to expand architecture details</strong></summary>

A time-series table consists of:

- **Parquet segments on disk**  
  Each segment holds a chunk of time-sorted data (e.g., 1h bars for a symbol).

- **Append-only metadata log (`_timeseries_log/`)**  
  - JSON commit files (`0000000001.json`, `0000000002.json`, ...) record segment additions/removals
  - `CURRENT` pointer tracks the latest committed version
  - **Version-guard OCC**: read version N → commit with expected_version=N → succeed only if CURRENT is still N

- **Table metadata with time index**  
  - `TableKind::TimeSeries(TimeIndexSpec)` with timestamp column, entity columns, bucket granularity
  - Schema info and creation timestamp

- **Coverage bitmaps (`_coverage/`)**  
  - Segment- and table-level RoaringBitmap snapshots
  - Enable O(1) overlap checks and gap queries without rescanning Parquet

</details>

---

## 📊 Project Status

**Early MVP** — APIs and on-disk layouts may change until v0.1.

- [x] Log-based metadata layer with version-guard OCC  
- [x] Time-series table abstraction + range scans  
- [x] Coverage snapshots + overlap-safe appends  
- [x] CLI for table management and SQL queries
- [x] DataFusion `TableProvider` integration
- [ ] End-to-end example with synthetic data
- [ ] Compaction / segment merging
- [ ] Time-travel queries

---

## 🤝 Contributing

Contributions welcome! This project is also a learning exercise in building table formats from scratch—if you're curious about the internals, the code is heavily commented.

---

## 📄 License

MIT License — see [LICENSE](LICENSE) for details.
