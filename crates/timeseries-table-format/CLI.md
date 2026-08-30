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
  --index-granularity 1h \
  --entity symbol

# 2. Add data with a matching Arrow timestamp column
tstable append \
  --table ./my_stocks \
  --parquet ./data/aapl_bars.parquet

# 3. Optionally rewrite mixed-entity segments
tstable optimize --table ./my_stocks

# 4. Query with SQL
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
  --index-granularity 1h \
  --timezone America/New_York \
  --entity symbol
```

| Flag | Required | Description |
|------|----------|-------------|
| `--table` | Yes | Path where the table will be created |
| `--index-column` | Yes | Name of the ascending index column |
| `--index-type` | Yes | Exact domain: `timestamp`, `int64`, or `uint64` |
| `--index-granularity` | Yes | Timestamp interval such as `1h`, or a positive integer for Int64/UInt64 |
| `--timezone` | | Timestamp IANA timezone such as `America/New_York` or `UTC` |
| `--entity` | | Entity identity column, repeatable |

| Index type | `--index-granularity` value | Optional |
|------------|-------------------------------|----------|
| `timestamp` | Time interval using `s`, `m`, `h`, or `d` units | `--timezone` |
| `int64` | Positive integer in index-value units | None |
| `uint64` | Positive integer in index-value units | None |

Integer granularities must be decimal integers from 1 through `18446744073709551615` (`u64::MAX`).

```bash
tstable create --table ./signed_ticks --index-column tick --index-type int64 --index-granularity 10
tstable create --table ./unsigned_counters --index-column counter --index-type uint64 --index-granularity 100
```

**What are entity columns?**  
Use repeatable `--entity` flags for identifiers such as stock symbols or numeric sensor IDs.
Coverage and overlap are tracked separately for each complete identity, in flag order. Supported
registered Arrow types are Utf8, LargeUtf8, Int32, Int64, and UInt64. Actual values must be
non-null. After the canonical schema exists, append may losslessly widen narrower integers into
those registered types, but it never converts between signed and unsigned values. Entity column
names must be unique and cannot also name the ordered index column.

---

### `append` — Add data to a table

Streams a local Parquet file through Arrow into a new table-managed segment. The persisted index
specification determines the column and type; append accepts no override.

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
- The source file is read lazily and remains unchanged, including when it is already under the
  table root
- The table writes the rows to a generated path under `data/_managed/append/`; this directory is
  reserved for table output. The table does not adopt the source path or filename
- Success prints `Appended table version: <VERSION>`; `--timing` adds elapsed milliseconds on the
  same line
- The index column must be Arrow Timestamp, Int64, or UInt64 exactly as configured
- Parquet rows need not be sorted by the index column
- Each complete entity identity may have at most one row in a logical index interval, both within
  one append and across separate appends
- Different complete identities may use the same interval; a table without entity columns has one
  table-wide implicit identity
- Schema must be compatible with existing data (if any)

---

### `optimize` - Rewrite mixed-entity segments

Explicitly rewrites each mixed-entity source segment into one replacement segment per complete
entity identity. This operation is optional; mixed-entity segments remain valid table data.

```bash
tstable optimize --table ./data/my_table
```

| Flag | Required | Description |
|------|----------|-------------|
| `--table` | Yes | Path to an existing table |

The command prints the complete optimization report using stable field names:

```text
starting_version: 1
committed_version: 2
candidate_source_segments: 1
source_segments_replaced: 1
replacement_segments_written: 2
distinct_identities_materialized: 2
rows_read: 4
rows_written: 4
no_op: false
```

A successful no-op prints the same fields with `no_op: true`, equal starting and committed
versions, and zero counts. It exits successfully and does not create a table version. Failures
exit nonzero with table and operation context.

Optimization preserves logical rows, schema, and per-entity coverage. It does not combine small
files or accept a target file size. Replaced source files may remain on disk until a future vacuum
operation removes unreferenced files.

Optimization may change physical row order.

---

### `vacuum` - Remove expired unreferenced files

An interrupted append can leave an incomplete Parquet file under `data/_managed/append/` without
committing it to the transaction log. An interrupted entity rewrite can do the same under
`data/_staged/entity-rewrite/`. Vacuum finds expired files in these reserved directories, along
with unreferenced coverage artifacts. It is a dry-run unless you pass `--apply`.

Start by choosing a cutoff older than the longest writer operation you expect:

```bash
tstable vacuum \
  --table ./data/my_table \
  --older-than 2026-08-01T00:00:00Z
```

Review the removable files, then run the same command with `--apply`:

```bash
tstable vacuum \
  --table ./data/my_table \
  --older-than 2026-08-01T00:00:00Z \
  --apply
```

| Flag | Required | Description |
|------|----------|-------------|
| `--table` | Yes | Path to an existing table |
| `--older-than` | Yes | Exclusive RFC3339 cutoff; must not be in the future |
| `--apply` | | Delete removable files; omit for a dry-run |

Files modified at or after the cutoff are retained. Vacuum also retains files referenced by any
valid retained commit, files whose names are not recognized as table-managed artifacts, and files
that change while apply mode is running. A cutoff that is too recent can select an active writer's
uncommitted file, so leave enough time for the longest expected write to finish.

Parquet files elsewhere under `data/` are not vacuum candidates. This includes source files passed
to `append`, even when the source is inside the table root.

The report includes file counts, byte counts, and one line per file. Artifact paths are quoted and
escaped. The reason values are:

| Reason | Meaning |
|--------|---------|
| `referenced_by_commit` | A retained commit references the file |
| `within_retention` | The file is not older than the cutoff |
| `changed_since_planning` | The file changed before apply could delete it |
| `unrecognized_artifact` | The file is inside a scanned directory but its path is not reserved |
| `unreferenced` | An expired managed file has no retained reference |
| `invalid_or_unreadable_parquet` | An expired unreferenced Parquet file has no readable valid footer |

If apply mode stops on a deletion error, the CLI prints a partial report before exiting nonzero.
Files marked `deleted` were completed before the failure. Files still marked `removable` were not
deleted and can be retried.

Vacuum is orphan-file cleanup. It does not expire snapshots, choose a transaction-log retention
boundary, rewrite table history, or delete transaction-log files. It scans regular files under
`data/` and `_coverage/`, but only the reserved Parquet paths above and recognized coverage paths
can be removed.

---

### `query` — Run SQL queries

Execute SQL queries against your table using DataFusion.
Results are unordered unless the SQL query uses `ORDER BY`.

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

If you omit `--table`, the shell prompts for a path. For a missing table, it then prompts for the
index column, index type, index granularity, optional timestamp timezone, and entity columns.

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
  --index-granularity 1d \
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

## Diagnostics and troubleshooting

`tstable` writes project and dependency diagnostics to stderr. The default level is `warn`, so
successful commands do not normally add diagnostic output.

Set the standard `RUST_LOG` environment variable to inspect a command in more detail:

```bash
RUST_LOG=timeseries_table_format=debug tstable append \
  --table ./data/my_table \
  --parquet ./incoming/new_data.parquet

RUST_LOG=timeseries_table_format=debug,datafusion=warn tstable query \
  --table ./data/my_table \
  --sql "SELECT COUNT(*) FROM my_table"
```

The Rust target uses the underscore form `timeseries_table_format`. DataFusion targets use the
`datafusion` prefix. An invalid `RUST_LOG` filter produces one warning and continues with the
warning-level default.

Diagnostics never enter stdout, `--output` files, pager input, or shell history. Those channels
remain reserved for command and query data. Captured or piped stderr contains no ANSI escape
codes.

The CLI does not provide file logging, JSON diagnostics, metrics export, or an OpenTelemetry
exporter. Rust applications embedding the library configure their own tracing subscriber; the CLI
initializer is not a library API.

---

## Tips

- **Row limit:** By default, only 10 rows are displayed. Use `--max-rows 0` to see everything, or `--output file.csv` to save full results.

- **Table names with special characters:** If your table directory has spaces or hyphens, quote it in SQL: `SELECT * FROM "my-table"`.

- **Refreshing in shell:** If another process appends data while you're in the shell, run `refresh` to see the new segments.

---

## Related

- [timeseries-table-format](README.md) - Rust library for building on this format
- [DataFusion integration](DATAFUSION.md) - SQL integration with ordered-index pruning
