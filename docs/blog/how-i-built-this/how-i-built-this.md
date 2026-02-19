# I built a lakehouse table format from scratch, and here's how I did it

I tried to build a small Delta-style table format in Rust, tuned for time-series appends. It's faster than Postgres / Delta + Spark / ClickHouse on append throughput (5x/4x/3x in our benchmark). Here's why and how it works, in 10 minutes.

## The moment it clicked

While I was learning Kafka (docs + blogs + YouTube tutorials), one theme kept coming up: the more useful way to think about Kafka isn't "a message queue", but "an immutable append-only log".

Around the same time, I was reading about how big data stacks evolved from Hadoop + Hive to the lakehouse era. When I dug into table formats like Delta Lake and Iceberg, I noticed the same pattern again: an append-only history of metadata that describes table state over time.

At that point I thought: this mental model of an immutable, append-only log must be really powerful. If the core idea is just “log + snapshots + a bit of concurrency control,” how hard would it be to build a small version myself — and tune it specifically for time-series data?

That question turned into a learn-by-doing project…and eventually into the table format I’m writing about in this post.

## Lakehouse table format 101 (Delta-style, then I map it to my repo)

My repo maps almost 1-to-1 onto Delta's mental model - so I'll explain the minimum concepts once, then show exactly where they live in my code and on disk.

Here's what you need to know:

1) **Immutable data files**
Data lives in immutable files (often Parquet). Appending means writing new files; the table format decides which files are "in" the table.

2) **An append-only transaction log**
Every change is recorded as an append-only sequence of commits ("here's what changed": add/remove files, update table metadata).

3) **Versioning + concurrncy control(OCC)**
Writers commit version N+1 only if they started from the latest version N; if someone else won first, you detect a conflict and retry.

4) **A current snapshot for readers (and checkpoints later)**
Readers need a consistent view: "the table as of the latest committed version". Many systems add checkpoints later so readers don't replay a huge log.

## Delta concepts -> this repo (quick mapping)

  | Concept | Delta mental model | This repo | Where |
  |---|---|---|---|
  | Transaction log dir | `_delta_log/` | `_timeseries_log/` | on disk, next to your data|
  | Commit entries | JSON actions | `Commit` + `LogAction`| Rust structs, serialized to JSON in the log |
  | Latest version | commit protocol | `CURRENT` file | a single file, just contains the latest version number |
  | Current snapshot | replay log (+ checkpoints) | `TableState` | in-memory, rebuilt on open |
  | Writer safety | OCC | OCC(`commit_with_expected_version(...)`) | Rust API — the only commit path |