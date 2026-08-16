# tstable

A command-line tool for creating, managing, and querying time-series tables without writing any Rust code.

## What is this?

This CLI lets you work with **time-series tables** backed by Parquet. Each table has one ascending chronological index: a physical Timestamp or an Int64/UInt64 logical clock.

Instead of manually managing scattered Parquet files, you get:
- A **table abstraction** that tracks all your data segments
- **SQL queries** powered by [Apache DataFusion](https://datafusion.apache.org/)
- **Automatic schema tracking** and overlap detection
- An **interactive shell** for exploratory analysis

## Installation

### From crates.io

```bash
cargo install timeseries-table-format --features cli
```

### From a local clone

```bash
git clone https://github.com/mag1cfrog/timeseries-table-format.git
cd timeseries-table-format
cargo install --path crates/timeseries-table-format --features cli
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
  --index-column timestamp \
  --index-type timestamp \
  --bucket 1h \
  --entity symbol

# 2. Add data with a matching Arrow timestamp column
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
  --index-column timestamp \
  --index-type timestamp \
  --bucket 1h \
  --timezone America/New_York \
  --entity symbol
```

| Flag | Required | Description |
|------|----------|-------------|
| `--table` | Yes | Path where the table will be created |
| `--index-column` | Yes | Name of the ascending index column |
| `--index-type` | Yes | Exact domain: `timestamp`, `int64`, or `uint64` |
| `--bucket` | Timestamp | Coverage bucket such as `1s`, `15m`, `1h`, or `1d` |
| `--timezone` | | Timestamp IANA timezone such as `America/New_York` or `UTC` |
| `--bucket-width` | Int64/UInt64 | Positive integer width in index-value units |
| `--entity` | | Entity identity column, repeatable |

| Index type | Required type-specific option | Optional | Rejected |
|------------|-------------------------------|----------|----------|
| `timestamp` | `--bucket` | `--timezone` | `--bucket-width` |
| `int64` | `--bucket-width` | None | `--bucket`, `--timezone` |
| `uint64` | `--bucket-width` | None | `--bucket`, `--timezone` |

Integer bucket widths must be decimal integers from 1 through `18446744073709551615` (`u64::MAX`).

```bash
tstable create --table ./signed_ticks --index-column tick --index-type int64 --bucket-width 10
tstable create --table ./unsigned_counters --index-column counter --index-type uint64 --bucket-width 100
```

**What are entity columns?**  
Use repeatable `--entity` flags for identifiers such as stock symbols or sensor IDs. Coverage and overlap are tracked separately for each entity.

---

### `append` — Add data to a table

Appends a Parquet file as a new segment. The persisted index specification determines the column and type; append accepts no override.

```bash
tstable append \
  --table ./data/my_table \
  --parquet ./incoming/new_data.parquet
```

| Flag | Required | Description |
|------|----------|-------------|
| `--table` | Yes | Path to an existing table |
| `--parquet` | Yes | Path to the Parquet file to append |
| `--timing` | | Print elapsed time |

**Notes:**
- An external Parquet file is copied into the table's `data/` directory
- The index column must be Arrow Timestamp, Int64, or UInt64 exactly as configured
- Overlapping index buckets with existing segments will cause an error
- Schema must be compatible with existing data (if any)

---

### `query` — Run SQL queries

Execute SQL queries against your table using DataFusion.

```bash
tstable query \
  --table ./data/my_table \
  --sql "SELECT * FROM my_table WHERE timestamp >= TIMESTAMP '2024-01-01 00:00:00' LIMIT 10"
```

| Flag | Required | Description |
|------|----------|-------------|
| `--table` | Yes | Path to the table |
| `--sql` | Yes | SQL query to execute |
| `--max-rows` | | Max rows to display (default: **10**, use `0` for unlimited) |
| `--format` | | Output format: `csv` (default) or `jsonl` |
| `--output` | | Write results to a file instead of stdout |
| `--explain` | | Show the query execution plan |
| `--timing` | | Print elapsed time |
| `--pager` | | Pipe output through `less -S` for horizontal scrolling |
| `--backend` | | Query backend; currently `data-fusion` only |

**Table name in SQL:**  
The table is registered under its directory name. For `./data/my_table`, use `my_table` in your SQL.

**Index predicates:**

```sql
-- Timestamp
WHERE timestamp >= TIMESTAMP '2024-01-01 00:00:00'

-- Int64
WHERE tick >= -20 AND tick < 0

-- UInt64 above i64::MAX
WHERE counter >= CAST('9223372036854775808' AS BIGINT UNSIGNED)
```

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
  --sql "SELECT * FROM stocks WHERE timestamp > TIMESTAMP '2024-06-01 00:00:00'" \
  --explain
```

---

### `shell` — Interactive mode

Opens an interactive shell that keeps the table loaded in memory.

```bash
tstable shell --table ./data/my_table
```

If you omit `--table`, the shell prompts for a path. For a missing table, it then prompts for the index column and type, followed by only the relevant bucket options and entity columns.

| Flag | Description |
|------|-------------|
| `--table` | Path to a table; prompts if omitted |
| `--history` | Path to command history file |
| `--backend` | Query backend; currently `data-fusion` only |

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
# Create a table for daily bars, tracked by symbol
tstable create \
  --table ./market_data/daily_bars \
  --index-column date \
  --index-type timestamp \
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

- **Refreshing in shell:** If another process appends data while you're in the shell, run `refresh` to see the new segments.

---

## Related

- [timeseries-table-format](README.md) - Rust library for building on this format
- [DataFusion integration](DATAFUSION.md) - SQL integration with ordered-index pruning
- [Ordered-index migration](../../README.md#migrating-timestamp-only-callers) - Source-breaking migration table
