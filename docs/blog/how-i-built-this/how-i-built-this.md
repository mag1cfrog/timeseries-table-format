# I built a lakehouse table format from scratch, and here's how I did it

I tried to build a small Delta-style table format in Rust, tuned for time-series appends. It's faster than Postgres / Delta + Spark / ClickHouse on append throughput (5x/4x/3x in our benchmark). Here's why and how it works, in 10 minutes.

## The moment it clicked

While I was learning Kafka (docs + blogs + YouTube tutorials), one theme kept coming up: the more useful way to think about Kafka isn't "a message queue", but "an immutable append-only log".

Around the same time, I was reading about how big data stacks evolved from Hadoop + Hive to the lakehouse era. When I dug into table formats like Delta Lake and Iceberg, I noticed the same pattern again: an append-only history of metadata that describes table state over time.

At that point I thought: this mental model of an immutable, append-only log must be really powerful. If the core idea is just “log + snapshots + a bit of concurrency control,” how hard would it be to build a small version myself — and tune it specifically for time-series data?

That question turned into a learn-by-doing project…and eventually into the table format I’m writing about in this post.

## Table formats in 5 minutes (Delta-style mental model)

First, a quick clarification: Parquet is a file format. Delta Lake and Iceberg are table formats.
A table format is basically a contract that answers:
- Where is the data stored?
- What files belong to the table right now?
- How do writers safely add new data without corrupting readers?
- How do readers get a consistent snapshot?

The bottom line about table formats is: they work because they treat table state as append-only metadata history + a current snapshot/pointer + a commit protocol.

### Delta vs Iceberg
Delta and Iceberg both use the same core shape: data files + append-only metadata history + snapshots.

They differ in the concrete implementation:
- Delta relies on underlying storage/compute primitives to achieve atomic commits, while Iceberg relies on catalog.
- Delta is like maintaining a series of changelogs with occasional checkpoints, while Iceberg is like maintaining a new snapshot for each commit.

## Lakehouse table format 101
Here's what you need to know about lakehouse table formats before we move on. A lakehouse table format usually consists of these major components:

### 1. Immutable data files
In lakehouses, the data usually lives as immutable files (commonly parquet). Appending data means writing new files; you don't "UPDATE rows in place" like a classic OLTP database.

The fiel format matters for performance, but it's not the point here. The table format's real job is to manage which files are currently valid.

### 2. A transaction log (append-only metadata)
A lakehouse table format like Delta's core idea is a transaction log: a sequence of commits where each commits syas "here's what changed".

Typical actions are:
- add data files
- remove data files (tombstones)
- update table-level metadata (schema/config)

Because commits are append-only, you can always reconstruct the table state at version N by replaying commits.

### 3. Versioning + concurrency control (OCC)
To support concurrent writers, each commit is versioned. Writers:
1. read the latest version
2. prepare "version + 1"
3. commit only if the world hasn' changed underneath them

If another writer wins first, you don't corrupt the table -- you just detect a conflict and retry based on the new latest state.

### 4. A current snapshot for readers (and checkpoints)
Readers want a consistent view: "give me the table as of the latest committed version".

Conceptually, a snapshot is just "the table state after applying commits up to version N:. Implementations differ in how they find N and how they materialize state efficiently, but hte idea is the same.

> NOTE:
> Replay a long log can get slow, so systems like Delta often write checkpoints: a compact representation of the current state so readers don't reply from day 1.
> You can think of checkpoints as "snapshots for performance", not a different source of truth.

