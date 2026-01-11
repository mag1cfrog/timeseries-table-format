# timeseries-table-format

[![Rust](https://img.shields.io/badge/Developed%20in-Rust-orange?logo=rust)](https://www.rust-lang.org)
![License](https://img.shields.io/badge/license-MIT-informational)
[![](https://github.com/mag1cfrog/timeseries-table-format/actions/workflows/ci.yml/badge.svg)](https://github.com/mag1cfrog/timeseries-table-format/actions/workflows/ci.yml)

<p align="center">
  <img src="docs/assets/ferris-timeseries.png" alt="Ferris with timeseries-table-format" width="1920" />
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
| 📦 **Zero External Dependencies** | Pure Rust, no JVM, no Python runtime—just `cargo install` and go |
| ⚡ **Blazing Fast Ingest** | [7–27× faster](#-performance) than ClickHouse/PostgreSQL on bulk loads and daily appends |

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

## ⚡ Performance

Benchmarked on **73M rows** of NYC taxi data (bulk load + 90 days of daily appends):

<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/assets/benchmark-chart.png">
    <source media="(prefers-color-scheme: light)" srcset="docs/assets/benchmark-chart-light.png">
    <img alt="Benchmark comparison chart" src="docs/assets/benchmark-chart.png" width="900">
  </picture>
</p>

<table>
<tr><td>

| vs ClickHouse | Speedup |
|---------------|---------|
| Bulk ingest | **7.7×** |
| Daily append | **3.3×** |
| Time-range scan | **2.5×** |

</td><td>

| vs PostgreSQL | Speedup |
|---------------|---------|
| Bulk ingest | **27×** |
| Daily append | **5.5×** |
| Time-range scan | **80×** |

</td></tr>
</table>

<sub>Aggregation queries (GROUP BY, filtering) are competitive with ClickHouse. Delta + Spark Q1 is now 964ms with partitioned Delta. See [full benchmark methodology and results](docs/benchmarks/README.md).</sub>

---

## 🚀 Quick Start

### CLI (Easiest)

```bash
# Install
cargo install --git https://github.com/mag1cfrog/timeseries-table-format --bin tstable

# Create a table with 1-hour buckets
tstable create --table ./my_table --time-column ts --bucket 1h

# Append data (overlap-safe!)
tstable append --table ./my_table --parquet ./data.parquet

# Query with SQL
tstable query --table ./my_table --sql "SELECT * FROM my_table LIMIT 5"
```

See the [CLI documentation](crates/timeseries-table-cli/README.md) for the full command reference.

### Rust API

```toml
[dependencies]
timeseries-table-format = { git = "https://github.com/mag1cfrog/timeseries-table-format" }
```

```rust
use timeseries_table_format::TimeSeriesTable;

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
timeseries-table-format = { git = "https://github.com/mag1cfrog/timeseries-table-format" }
```

See [timeseries-table-datafusion](crates/timeseries-table-datafusion/README.md) for SQL query examples.

---

## 🥾 Quickstart Example (NVDA 1h + MA5)

Fastest way to see the format end-to-end (no external services needed):

1) Ingest sample data (creates `examples/nvda_table/`):

```bash
cargo run -p timeseries-table-core --example ingest_nvda
```

2) Query with DataFusion + moving average window:

```bash
cargo run -p timeseries-table-datafusion --example query_nvda_ma
```

Example output:

```
+---------------------+--------+--------------------+
| ts                  | close  | ma_5               |
+---------------------+--------+--------------------+
| 2024-06-01T00:00:00 | 115.22 | 115.22             |
| 2024-06-01T01:00:00 | 115.55 | 115.38499999999999 |
| 2024-06-01T02:00:00 | 115.51 | 115.42666666666666 |
| 2024-06-01T03:00:00 | 114.99 | 115.3175           |
| 2024-06-01T04:00:00 | 114.7  | 115.194            |
+---------------------+--------+--------------------+
```

Sample data lives at `examples/data/nvda_1h_sample.csv` (240 rows of NVDA 1h bars). The ingestion step writes a Parquet segment and appends it via the transaction log using optimistic concurrency.

---

## 🏗️ Architecture

<p align="center">
  <img src="docs/assets/high-level-architecture.png" alt="high level architecture" width="1920" />
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
- [x] End-to-end example with sample data
- [ ] Compaction / segment merging
- [ ] Time-travel queries

---

## 📚 Further Reading

- [Benchmark methodology & results](docs/benchmarks/README.md)
- [CLI reference](crates/timeseries-table-cli/README.md)
- [Core library API](crates/timeseries-table-core/README.md)
- [DataFusion integration](crates/timeseries-table-datafusion/README.md)

---

## 🤝 Contributing

Contributions welcome! This project is also a learning exercise in building table formats from scratch—if you're curious about the internals, the code is heavily commented.

---

## 📄 License

MIT License — see [LICENSE](LICENSE) for details.
