# tstable

A command-line tool for creating, managing, and querying time-series tables—without writing any Rust code.

## What is this?

This CLI lets you work with **time-series tables**: a structured way to store and query time-indexed data (like stock prices, sensor readings, or log events) on top of Parquet files.

Instead of manually managing scattered Parquet files, you get:
- A **table abstraction** that tracks all your data segments
- **SQL queries** powered by [Apache DataFusion](https://datafusion.apache.org/)
- **Automatic schema tracking** and overlap detection
- An **interactive shell** for exploratory analysis

## Installation

### From crates.io

```bash
cargo install timeseries-table-cli --bin tstable
```

### From a local clone

```bash
git clone https://github.com/mag1cfrog/timeseries-table-format.git
cd timeseries-table-format
cargo install --path crates/timeseries-table-cli
```

### Verify installation

```bash
tstable --help
```

## Quick start

```bash
# 1. Create a table for hourly stock bars
tstable create \
  --table ./my_stocks \
  --time-column timestamp \
  --bucket 1h \
  --entity symbol

# 2. Add some data (any Parquet file with a timestamp column)
tstable append \
  --table ./my_stocks \
  --parquet ./data/aapl_bars.parquet

# 3. Query with SQL
tstable query \
  --table ./my_stocks \
  --sql "SELECT symbol, COUNT(*) FROM my_stocks GROUP BY symbol"
```

---

## Commands

### `create` — Create a new table

Creates an empty time-series table. The schema is automatically inferred when you append the first data segment.

```bash
tstable create \
  --table ./data/my_table \
  --time-column timestamp \
  --bucket 1h \
  --timezone America/New_York \
  --entity symbol
```

| Flag | Required | Description |
|------|----------|-------------|
| `--table` | ✓ | Path where the table will be created |
| `--time-column` | ✓ | Name of the timestamp column in your data |
| `--bucket` | ✓ | Time granularity for indexing (see below) |
| `--timezone` | | IANA timezone (e.g., `America/New_York`, `UTC`) |
| `--entity` | | Entity column(s) for partitioning, repeatable |

**Bucket values:** `1s`, `1m`, `5m`, `15m`, `1h`, `1d`

**What are entity columns?**  
If your data has multiple "things" (like stock symbols or sensor IDs), specify them with `--entity`. This helps with coverage tracking and future optimizations.

---

### `append` — Add data to a table

Appends a Parquet file as a new segment. The file must have the timestamp column defined when you created the table.

```bash
tstable append \
  --table ./data/my_table \
  --parquet ./incoming/new_data.parquet
```

| Flag | Required | Description |
|------|----------|-------------|
| `--table` | ✓ | Path to an existing table |
| `--parquet` | ✓ | Path to the Parquet file to append |
| `--time-column` | | Override timestamp column (default: from table metadata) |
| `--timing` | | Print elapsed time |

**Notes:**
- The Parquet file is copied/moved into the table's `data/` directory
- Overlapping time ranges with existing segments will cause an error
- Schema must be compatible with existing data (if any)

---

### `query` — Run SQL queries

Execute SQL queries against your table using DataFusion.

```bash
tstable query \
  --table ./data/my_table \
  --sql "SELECT * FROM my_table WHERE timestamp > '2024-01-01' LIMIT 10"
```

| Flag | Required | Description |
|------|----------|-------------|
| `--table` | ✓ | Path to the table |
| `--sql` | ✓ | SQL query to execute |
| `--max-rows` | | Max rows to display (default: **10**, use `0` for unlimited) |
| `--format` | | Output format: `csv` (default) or `jsonl` |
| `--output` | | Write results to a file instead of stdout |
| `--explain` | | Show the query execution plan |
| `--timing` | | Print elapsed time |
| `--pager` | | Pipe output through `less -S` for horizontal scrolling |

**Table name in SQL:**  
The table is registered under its directory name. For `./data/my_table`, use `my_table` in your SQL.

**Examples:**

```bash
# Show all data (no row limit)
tstable query \
  --table ./stocks \
  --sql "SELECT * FROM stocks" \
  --max-rows 0

# Export to JSON Lines
tstable query \
  --table ./stocks \
  --sql "SELECT symbol, close FROM stocks WHERE symbol = 'AAPL'" \
  --format jsonl \
  --output aapl.jsonl

# See the query plan
tstable query \
  --table ./stocks \
  --sql "SELECT * FROM stocks WHERE timestamp > '2024-06-01'" \
  --explain
```

---

### `shell` — Interactive mode

Opens an interactive shell that keeps the table loaded in memory. Great for exploratory analysis.

```bash
tstable shell --table ./data/my_table
```

If you omit `--table`, the shell will prompt you for a path (and can create a new table interactively).

| Flag | Description |
|------|-------------|
| `--table` | Path to a table (optional—will prompt if omitted) |
| `--history` | Path to command history file |

**Shell commands:**

| Command | Description |
|---------|-------------|
| `query <sql>` | Run a SQL query |
| `query --max-rows 100 <sql>` | Query with options |
| `explain <sql>` | Show query execution plan |
| `append <parquet_path>` | Append a new segment |
| `refresh` | Reload table state from disk |
| `\timing` | Toggle elapsed time display |
| `\pager` | Toggle pager output |
| `alias <name>` | Set a shorter table name for queries |
| `alias --clear` | Reset to default table name |
| `clear` | Clear screen |
| `help` | Show all commands |
| `exit` | Exit the shell |

**Query flags in shell:**
```
query [--max-rows N] [--format csv|jsonl] [--output PATH] [--timing] [--explain] [--] <sql>
```

Use `--` before your SQL if it starts with `--` (to avoid flag parsing issues).

---

## Example: Stock market data

Here's a complete workflow for managing daily stock bars:

```bash
# Create a table for daily bars, partitioned by symbol
tstable create \
  --table ./market_data/daily_bars \
  --time-column date \
  --bucket 1d \
  --entity symbol \
  --timezone America/New_York

# Append historical data
tstable append \
  --table ./market_data/daily_bars \
  --parquet ./downloads/spy_2023.parquet

tstable append \
  --table ./market_data/daily_bars \
  --parquet ./downloads/spy_2024.parquet

# Query: Find the highest closing prices
tstable query \
  --table ./market_data/daily_bars \
  --sql "
    SELECT symbol, date, close
    FROM daily_bars
    WHERE close = (SELECT MAX(close) FROM daily_bars)
  "

# Interactive exploration
tstable shell --table ./market_data/daily_bars
```

---

## Output formats

### CSV (default)

```
symbol,date,open,high,low,close,volume
AAPL,2024-01-02,185.50,186.20,184.80,185.90,50000000
AAPL,2024-01-03,186.00,187.10,185.50,186.80,48000000
```

### JSON Lines (`--format jsonl`)

```json
{"symbol":"AAPL","date":"2024-01-02","open":185.50,"high":186.20,"low":184.80,"close":185.90,"volume":50000000}
{"symbol":"AAPL","date":"2024-01-03","open":186.00,"high":187.10,"low":185.50,"close":186.80,"volume":48000000}
```

---

## Tips

- **Row limit:** By default, only 10 rows are displayed. Use `--max-rows 0` to see everything, or `--output file.csv` to save full results.

- **Table names with special characters:** If your table directory has spaces or hyphens, quote it in SQL: `SELECT * FROM "my-table"`.

- **Time filtering:** DataFusion supports standard SQL timestamp syntax:
  ```sql
  WHERE timestamp > '2024-01-01T00:00:00Z'
  WHERE timestamp BETWEEN '2024-01-01' AND '2024-06-30'
  ```

- **Refreshing in shell:** If another process appends data while you're in the shell, run `refresh` to see the new segments.

---

## Related

- [timeseries-table-core](../timeseries-table-core/README.md) — Core Rust library for building on this format
- [timeseries-table-datafusion](../timeseries-table-datafusion/README.md) — DataFusion integration with time-based pruning
