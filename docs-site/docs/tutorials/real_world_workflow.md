# Tutorial: real-world incremental ingestion workflow

**Goal:** Understand how to integrate `timeseries-table-format` into a recurring ingestion job
(e.g. a daily cron task) that safely appends new files over time.

**Prereqs:** Completed [Create, append, query](create_append_query.md).

**What you'll learn:**
- How to open an existing table and append new data to it on each run
- How to skip files that are already loaded (using `CoverageOverlapError`)
- How to verify what's already in the table before and after ingestion

---

## Scenario

You receive one Parquet file per day for a set of symbols. Each file covers one calendar day of
hourly bars. You want a table that accumulates all history on disk, and you want your ingestion
script to be idempotent — running it twice on the same file should not corrupt the table.

---

## Pattern: open-or-create

On the first run, create the table. On subsequent runs, open it.

```python
import timeseries_table_format as ttf
from pathlib import Path

TABLE_ROOT = Path("./prices_table")

def open_or_create_table(table_root: Path) -> ttf.TimeSeriesTable:
    try:
        return ttf.TimeSeriesTable.open(str(table_root))
    except ttf.TimeseriesTableError:
        return ttf.TimeSeriesTable.create(
            table_root=str(table_root),
            time_column="ts",
            bucket="1h",
            entity_columns=["symbol"],
        )
```

---

## Pattern: skip-if-already-loaded

`append_parquet(...)` raises `CoverageOverlapError` if the incoming file overlaps existing
coverage. Use that to make your loop idempotent:

```python
import timeseries_table_format as ttf
from pathlib import Path

def ingest_files(table: ttf.TimeSeriesTable, files: list[Path]) -> None:
    for f in files:
        try:
            version = table.append_parquet(str(f))
            print(f"  appended {f.name} → table version {version}")
        except ttf.CoverageOverlapError as e:
            print(f"  skipped {f.name}: {e.overlap_count} overlapping bucket(s) already loaded")
```

Running this twice on the same list of files will append on the first pass and silently skip on
the second — the table is never double-loaded.

---

## Pattern: verify what's in the table

After ingestion, query the table to confirm what's loaded:

```python
import timeseries_table_format as ttf

def print_table_summary(table_root: str) -> None:
    sess = ttf.Session()
    sess.register_tstable("t", table_root)
    summary = sess.sql(
        """
        SELECT
            symbol,
            min(ts)   AS first_ts,
            max(ts)   AS last_ts,
            count(*)  AS row_count
        FROM t
        GROUP BY symbol
        ORDER BY symbol
        """
    )
    print(summary)
```

---

## Putting it together

```python
from pathlib import Path
import timeseries_table_format as ttf

TABLE_ROOT = Path("./prices_table")

def open_or_create_table(table_root: Path) -> ttf.TimeSeriesTable:
    try:
        return ttf.TimeSeriesTable.open(str(table_root))
    except ttf.TimeseriesTableError:
        return ttf.TimeSeriesTable.create(
            table_root=str(table_root),
            time_column="ts",
            bucket="1h",
            entity_columns=["symbol"],
        )

def run_daily_ingest(new_files: list[Path]) -> None:
    table = open_or_create_table(TABLE_ROOT)

    for f in new_files:
        try:
            version = table.append_parquet(str(f))
            print(f"appended {f.name} (version {version})")
        except ttf.CoverageOverlapError as e:
            print(f"skipped {f.name}: already loaded ({e.overlap_count} overlap(s))")

    # Verify
    sess = ttf.Session()
    sess.register_tstable("prices", str(TABLE_ROOT))
    print(sess.sql("SELECT min(ts), max(ts), count(*) FROM prices"))
```

---

## Key takeaways

- **Open, don't recreate.** Use `TimeSeriesTable.open(...)` on subsequent runs so you keep history.
- **Let `CoverageOverlapError` be your idempotency guard.** Catching it is normal; it means the
  data is already there.
- **Always verify after ingest.** A quick `min/max/count` query is cheap and confirms the append
  actually committed.

Next:
- Reference: [TimeSeriesTable](../reference/timeseries_table.md)
- Reference: [Exceptions](../reference/exceptions.md)
- Concept: [Buckets + overlap](../concepts/bucketing_and_overlap.md)
