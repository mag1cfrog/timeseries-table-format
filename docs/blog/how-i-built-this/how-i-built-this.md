# I built a lakehouse table format from scratch, and here's how I did it

## How it started

A couple months ago I was learning Kafka on my own, and I had that “ohhh” moment: Kafka isn’t really a message queue the way people casually describe it — it’s an immutable, append-only log.

Once you see it that way, it clicks: an append-only log plus a local state store is a powerful foundation for stream processing. It’s what lets systems keep state and build “database-like” views over a live stream — the same idea ksqlDB uses to layer SQL on top of Kafka’s log.

Around the same time, I was reading about how big data stacks evolved from Hadoop + Hive to the lakehouse era. Spark largely replaced MapReduce with a more flexible execution model, and Hive gradually shifted toward being “just” a metastore while compute moved elsewhere.

Then I started digging into how modern table formats like Delta Lake and Iceberg work, and I noticed the same core pattern showing up again: an append-only log (or, more generally, append-only metadata) describing the table’s state over time. The details differ — Delta leans on the underlying storage for atomicity, while Iceberg typically relies on a catalog/metastore for commits — but the mental model felt familiar.

At that point I thought: if the core idea is “log + snapshots + a bit of concurrency control,” how hard would it be to build a small version myself — and tune it specifically for time-series data?

That question turned into a learn-by-doing project…and eventually into the table format I’m writing about in this post.

