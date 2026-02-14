# Concept: when to use this

## TL;DR

`timeseries-table-format` gives you:

- Local filesystem **table roots** (a stable directory layout for a table).
- Append Parquet segments with **overlap detection**.
- **SQL** via DataFusion, with results returned as a `pyarrow.Table`.

## Use it when…

- You have time-series Parquet files arriving over time and want a consistent on-disk table layout.
- You want to query across segments (and across multiple tables) with SQL.
- You want overlap detection to catch accidental duplicate ingestion.

## Don’t use it when… (v0)

- You need S3/object storage backends.
- You need compaction, schema evolution, or upserts/merges.
- You want a centralized database/server.

## How it fits

Think of this as a thin **metadata + storage layer** on top of Parquet segment files: it defines a
table root layout, tracks what segments are part of the table, and enforces overlap rules during
appends. **DataFusion** provides the SQL engine, and query results come back as `pyarrow.Table`.

## Alternatives (quick intuition)

- If you only need ad-hoc queries over a few files: DuckDB / Polars directly over Parquet.
- If you need a database with updates and concurrent writers: a real database (server-based).
- If you need object storage + compaction/transactions: tools built for S3 + lakehouse workflows.

Next:
- Tutorial: [Create, append, query](../tutorials/create_append_query.md)
- Concept: [Buckets + overlap](bucketing_and_overlap.md)
