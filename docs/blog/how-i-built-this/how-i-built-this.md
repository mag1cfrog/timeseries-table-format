# I built a lakehouse table format from scratch, and here's how I did it

I built a Rust-based lakehouse table format inspired by Delta/Iceberg, and on append-heavy time-series workloads it blows the
common systems out of the water in our benchmark (5x Postgres, 4x Delta + Spark, and 3x ClickHouse). Here's why and how it works, dead simple in 10 minutes.

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
