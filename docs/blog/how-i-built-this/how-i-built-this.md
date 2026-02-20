# I built a Delta-style table format in Rust for time-series appends -- here's how it works (and why it's faster)

Once you internalize "append-only log + snapshots," a lot of modern data systems start looking like the same idea in different clothes.

That's the rabbit hole that led me to build a small Delta-style table format in Rust, tuned for time-series appends. In our benchmark it beats Postgres / Delta + Spark / ClickHouse on append throughput (~3-6x).

This post is the 10-minute tour of how it works.

If you're mostly here for the performance results, scroll to **Benchmarks** -- I won't make you wait to the end.

The project is called `timeseries-table-format` -- a Rust library (with Python bindings) that implements a minimal Delta-style table format optimized for time-series append workloads.

## The moment it clicked

While I was learning Kafka (docs + blogs + YouTube tutorials), one theme kept coming up: the more useful way to think about Kafka isn't "a message queue", but "an immutable append-only log". Around the same time, I was reading about how big data stacks evolved from Hadoop + Hive to the lakehouse era; when I dug into table formats like Delta Lake and Iceberg, I noticed the same pattern again: an append-only history of metadata that describes table state over time. Once that clicked, the question became unavoidable: if the core idea is just "log + snapshots + a bit of concurrency control", how hard would it be to build a small version myself - and tune it specifically for time-series data? That question turned into a learn-by-doing project...and eventually into the table format I'm writing about in this post.

## Lakehouse table format 101 (Delta-style, then I map it to my repo)

My repo maps almost 1-to-1 onto Delta's mental model -- so I'll explain the minimum concepts once, then show exactly where they live in my code and on disk.

Here's what you need to know:

1) **Immutable data files**
Data lives in immutable files (often Parquet). Appending means writing new files; the table format decides which files are "in" the table.

2) **An append-only transaction log**
Every change is recorded as an append-only sequence of commits ("here's what changed": add/remove files, update table metadata).

3) **Versioning + concurrency control (OCC)**
Writers commit version N+1 only if they started from the latest version N; if someone else won first, you detect a conflict and retry.

4) **A current snapshot for readers (and checkpoints later)**
Readers need a consistent view: "the table as of the latest committed version". Many systems add checkpoints later so readers don't replay a huge log.

### Delta concepts -> this repo (quick mapping)

| Concept | Delta mental model | This repo | Where |
|---|---|---|---|
| Transaction log dir | `_delta_log/` | `_timeseries_log/` | On disk, next to your data |
| Commit entries | JSON actions | `Commit` + `LogAction` | Rust structs, serialized to JSON in the log |
| Latest version | commit protocol | `CURRENT` file | A single file, just contains the latest version number |
| Current snapshot | replay log (+ checkpoints) | `TableState` | In-memory, rebuilt on open |
| Writer safety | OCC | OCC(`commit_with_expected_version(...)`) | Rust API - the only commit path |

Here's the whole lifecycle in one picture:

```text
incoming Parquet files
        |
        v
append_parquet()
        |
        +--> data/                     (immutable data files)
        +--> _timeseries_log/*.json    (append-only commits)
        +--> _timeseries_log/CURRENT   (latest version pointer)
        |
        v
open() -> replay log -> TableState snapshot -> query
```


## Walkthrough: watch one append turn into a queryable table

We just talked about "immutable files + an append-only log + versioning + a current snapshot". Now let's watch those concepts play out in a real append.

### Step 1) Create a table, append one Parquet file

```python
from pathlib import Path
import tempfile

import pyarrow as pa
import pyarrow.parquet as pq

import timeseries_table_format as ttf

with tempfile.TemporaryDirectory() as d:
    root = Path(d) / "prices_tbl"
    tbl = ttf.TimeSeriesTable.create(
        table_root=str(root),
        time_column="ts",
        bucket="1h",
        entity_columns=["symbol"],
        timezone=None,
    )

    incoming = Path(d) / "incoming.parquet"
    pq.write_table(
        pa.table(
            {
                "ts": pa.array(["2024-06-01T00:00:00Z"], type=pa.timestamp("us", tz="UTC")),
                "symbol": pa.array(["NVDA"]),
                "close": pa.array([10.0]),
            }
        ),
        str(incoming),
    )

    print("new version:", tbl.append_parquet(str(incoming)))
```

> What landed on disk (conceptually):
>
> - prices_tbl/_timeseries_log/CURRENT
> - prices_tbl/_timeseries_log/0000000001.json (table metadata commit)
> - prices_tbl/data/incoming.parquet (if the input file was outside the table root and had to be copied in)
> - prices_tbl/_timeseries_log/0000000002.json (append commit)
> - prices_tbl/_timeseries_log/CURRENT now points to version 2

### Step 2) The artifact: a real `AddSegment` action

A new data file becomes part of the table only after it's logged.

An actual `AddSegment` action from this repo (from examples/nvda_table/_timeseries_log/0000000002.json):

```json
{
  "AddSegment": {
    "segment_id": "seg-f0573298681657796623719468bf1133",
    "path": "data/nvda_1h.parquet",
    "format": "parquet",
    "ts_min": "2024-06-01T00:00:00Z",
    "ts_max": "2024-06-10T23:00:00Z",
    "row_count": 240,
    "file_size": 14272,
    "coverage_path": "_coverage/segments/segcov-ca3cea172cc538ce04756e34beaea4a4.roar"
  }
}
```
Notice the `coverage_path` -- we'll come back to that.

If you squint, you can already see the reader-side wins:
- ts_min/ts_max enable coarse pruning (skip files that can't match a time filter).
- the log entry is human-inspectable and replayable.

So far we've looked at one table, one append. But the more interesting question is: can you register multiple tables and query across them? That's what `Session` is for.

## Try it yourself: 60 seconds to a join (Python)

Here's why this matters: `Session` isn't just "a query wrapper for one table". It's a single SQL session where you can register multiple tables and run real joins across them.

```bash
pip install timeseries-table-format
```

```python
from __future__ import annotations

import tempfile
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

import timeseries_table_format as ttf


with tempfile.TemporaryDirectory() as d:
    base = Path(d)

    # None = no timezone normalization (use timestamps as stored in Parquet)
    timezone = None

    prices_root = base / "prices_tbl"
    prices = ttf.TimeSeriesTable.create(
        table_root=str(prices_root),
        time_column="ts",
        bucket="1h",
        entity_columns=["symbol"],
        timezone=timezone,
    )
    prices_seg = base / "prices.parquet"
    pq.write_table(
        pa.table(
            {
                "ts": pa.array(
                    ["2024-06-01T00:00:00Z", "2024-06-01T01:00:00Z"],
                    type=pa.timestamp("us", tz="UTC"),
                ),
                "symbol": pa.array(["NVDA", "NVDA"]),
                "close": pa.array([10.0, 11.0]),
            }
        ),
        str(prices_seg),
    )
    prices.append_parquet(str(prices_seg))

    volumes_root = base / "volumes_tbl"
    volumes = ttf.TimeSeriesTable.create(
        table_root=str(volumes_root),
        time_column="ts",
        bucket="1h",
        entity_columns=["symbol"],
        timezone=timezone,
    )
    volumes_seg = base / "volumes.parquet"
    pq.write_table(
        pa.table(
            {
                "ts": pa.array(
                    ["2024-06-01T00:00:00Z", "2024-06-01T01:00:00Z"],
                    type=pa.timestamp("us", tz="UTC"),
                ),
                "symbol": pa.array(["NVDA", "NVDA"]),
                "volume": pa.array([100, 120]),
            }
        ),
        str(volumes_seg),
    )
    volumes.append_parquet(str(volumes_seg))

    sess = ttf.Session()
    sess.register_tstable("prices", str(prices_root))
    sess.register_tstable("volumes", str(volumes_root))

    out = sess.sql("""
    select p.ts as ts, p.symbol as symbol, p.close as close, v.volume as volume
    from prices p
    join volumes v
      on p.ts = v.ts and p.symbol = v.symbol
    order by ts
    """)

    print(out)  # pyarrow.Table
```

## Why this isn't just Delta-in-Rust: coverage tracking

Remember this field from the `AddSegment` JSON earlier?

```json
"coverage_path": "_coverage/segments/segcov-ca3cea172cc538ce04756e34beaea4a4.roar"
```

Time-series users keep asking questions like:

- "Do I have full coverage for this time range?"
- "Where are the gaps?"
- "Did I already ingest this time window, or am I about to overlap/duplicate data?"

Coverage is my answer to that.

Two concrete wins:

- Gap/coverage questions become metadata reads, not Parquet rescans.
- Overlap-safe ingestion becomes the default, not "best-effort".

### What "coverage" means (in one sentence)

If you created a table with `bucket="1h"`, coverage is just "which 1-hour slots have data".

### What `_coverage/` stores

Under the table root, `_coverage/` stores small sidecar files:

- `_coverage/segments/<id>.roar` - coverage for a segment
- `_coverage/table/<ver>-<id>.roar` - a snapshot coverage for the whole table at a log version

The table snapshot is basically the union of segment coverages so far.

### How append uses coverage (end-to-end)

When you append a Parquet file, the flow becomes:
1. Map the segment's timestamps into bucket IDs (based on your `bucket`, like `1h`).
2. Load the current table coverage snapshot (or empty for the first append).
3. Check overlap: `segment_coverage & table_coverage`.
4. If overlap is non-empty, reject the append (this surfaces as `CoverageOverlapError` in Python).
5. Otherwise:
    - write the segment coverage sidecar (coverage_path)
    - write the new table snapshot sidecar
    - commit the log update (same Delta-style OCC as before)

That's why the `coverage_path` shows up right next to `ts_min`/`ts_max` in the commit JSON: it's just more metadata that makes common time-series questions cheap.

**"Why not just use Delta or Iceberg?"** Fair question. You should, if your workload needs what they're built for -- schema evolution, MERGE/upsert, cloud object stores, the full Spark ecosystem. They're battle-tested and general-purpose. This project exists because time-series append workloads have a narrower contract: you're writing immutable, time-ordered segments, and your most common questions are about coverage and gaps, not schema changes. A format designed for that specific contract can bake in overlap detection, instant coverage queries, and skip the complexity you don't need -- and that's where the speed comes from.

## Benchmarks

Big performance claims are cheap -- so here are the numbers.

I ran the same workload across ClickHouse, Delta Lake + Spark, PostgreSQL, and TimescaleDB using the NYC TLC FHVHV trip dataset (April-June 2024, ~73M rows). The test I care most about is "daily append": 90 day-sized files appended one after another, like a real ETL pipeline.

Headline results (lower is better):

| System | Daily Append (mean) | Time-Range Scan |
|---|---:|---:|
| **timeseries-table** | **335 ms** | **545 ms** |
| ClickHouse | 1,114 ms | 1.4 s |
| Delta + Spark | 1,454 ms | 964 ms |
| PostgreSQL | 1,829 ms | 43.6 s |
| TimescaleDB | 3,197 ms | 43.9 s |

On daily appends, this format is ~3.3x faster than ClickHouse, ~4.3x faster than Delta + Spark, and ~5.5x faster than PostgreSQL in this setup.

The query story holds up too: on time-range scans it's ~2.5x faster than ClickHouse and ~80x faster than PostgreSQL here. (Aggregations are also competitive with ClickHouse: within ~3% in this benchmark.)

Full methodology + reproduction steps:
https://github.com/mag1cfrog/timeseries-table-format/blob/main/docs/benchmarks/README.md
(Also in the repo under `docs/benchmarks/README.md`.)

## Limitations / non-goals (v0)

This is intentionally a narrow v0 - I made explicit tradeoffs to ship something sharp (and measurable) rather than a general-purpose table format:

- Local filesystem tables (no S3/GCS/Azure object store yet)
- No compaction / merge (overlap is rejected; no upsert semantics)
- No schema evolution story yet
- No distributed coordinator (single-writer OCC at the log level; conflicts surface as errors you retry)
- Reader side is "replay the log" (no checkpointing yet)

## Try it / feedback

The quickest "does it feel nice?" path is the Python quickstart earlier in this post ("Try it yourself: 60 seconds to a join (Python)").

Everything -- code, benchmarks, docs -- lives here:
https://github.com/mag1cfrog/timeseries-table-format

If this post was useful, a star helps -- and if you have workload ideas or strong opinions on v1 priorities (compaction, object storage, schema evolution), open an issue.