# Benchmark Results

This document details the benchmark methodology, results, and reproduction steps for comparing `timeseries-table-format` against popular time-series and analytical databases.

## Summary

| System | Bulk Ingest | Daily Append (avg) | Time-Range Query | Aggregation (avg) |
|--------|-------------|--------------------|--------------------|-------------------|
| **timeseries-table** | **1.7s** | **335ms** | **545ms** | **321ms** |
| ClickHouse | 13.1s | 1.1s | 1.4s | 311ms |
| Delta + Spark | 24.2s | 1.5s | 49.7s | 507ms |
| PostgreSQL | 45.5s | 1.8s | 43.6s | 2.4s |
| TimescaleDB | 86.6s | 3.2s | 43.9s | 519ms |

**Key Takeaways:**
- **Bulk ingest**: 7.7× faster than ClickHouse, 27× faster than PostgreSQL
- **Daily append**: 3.3× faster than ClickHouse, 5.5× faster than PostgreSQL  
- **Time-range scan**: 2.5× faster than ClickHouse, 80× faster than PostgreSQL
- **Aggregations**: Competitive with ClickHouse (within 3%)

---

## Dataset

**NYC TLC For-Hire Vehicle High Volume (FHVHV) Trip Records**

| Metric | Value |
|--------|-------|
| Source | [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) |
| Time Range | April – June 2024 |
| Total Rows | ~73 million |
| Bulk File Size | ~476 MB (Parquet) |
| Daily Files | 90 files (one per day) |
| Schema | 24 columns including timestamps, locations, fares, miles |

This dataset is commonly used for analytical benchmarks due to its real-world characteristics:
- High cardinality timestamp column (`pickup_datetime`)
- Mix of numeric, string, and categorical columns
- Realistic time-series distribution patterns

---

## Test Environment

| Resource | Value |
|----------|-------|
| CPU Limit | 8 cores |
| Memory Limit | 16 GB |
| Storage | Local SSD |
| Docker | Compose-based isolation |

All systems run in Docker containers with identical resource constraints via `cpus` and `mem_limit` settings.

---

## Systems Tested

| System | Version | Notes |
|--------|---------|-------|
| **timeseries-table** | local build | Rust CLI, Parquet storage, DataFusion query engine |
| **ClickHouse** | latest | `clickhouse/clickhouse-server:latest` |
| **Delta Lake + Spark** | Spark 3.5.1 + Delta 3.1.0 | Local mode, `apache/spark:3.5.1` |
| **PostgreSQL** | 16 | `postgres:16` |
| **TimescaleDB** | latest | `timescale/timescaledb:latest-pg16` |

### Excluded: InfluxDB 3 Core

InfluxDB 3 Core was initially included but excluded from final results due to:

1. **File scan limit**: The free Core version enforces a file scan limit that prevents running analytical queries (Q1–Q5) on datasets of this size
2. **Ingest performance**: Bulk ingest was ~250s (vs 1.7s for timeseries-table), making it non-competitive for this workload
3. **Protocol mismatch**: Requires line protocol format, adding conversion overhead

InfluxDB 3 is optimized for high-frequency metrics ingestion, not analytical batch workloads.

---

## Benchmark Operations

### 1. Bulk Ingest

Load the first month of data (~20M rows) into an empty table.

| System | Method |
|--------|--------|
| timeseries-table | `timeseries-table-cli append --parquet` |
| ClickHouse | `INSERT INTO ... FORMAT CSVWithNames` |
| Delta + Spark | `df.write.format("delta").save()` |
| PostgreSQL | `\copy ... FROM ... WITH (FORMAT csv)` |
| TimescaleDB | `\copy ... FROM ... WITH (FORMAT csv)` |

### 2. Daily Append

Append 90 daily files sequentially (simulating daily ETL jobs). Each file contains ~600K–800K rows.

The table below shows the **mean** append time across all 90 days:

| System | Mean | Min | Max | Std Dev |
|--------|------|-----|-----|---------|
| timeseries-table | 335ms | 284ms | 485ms | 41ms |
| ClickHouse | 1,114ms | 918ms | 1,344ms | 91ms |
| Delta + Spark | 1,454ms | 1,296ms | 2,237ms | 194ms |
| PostgreSQL | 1,829ms | 1,434ms | 2,524ms | 219ms |
| TimescaleDB | 3,197ms | 2,651ms | 4,265ms | 339ms |

No system showed performance regression over the 90-day append sequence.

### 3. Analytical Queries

Five SQL queries executed against the fully-loaded table (~73M rows):

#### Q1: Time-Range Scan
```sql
SELECT *
FROM trips
WHERE pickup_datetime >= '2024-05-01' AND pickup_datetime < '2024-05-08';
```
Returns all columns for a 7-day window (~4.4M rows).

#### Q2: Simple Aggregation
```sql
SELECT COUNT(*) AS trip_count, SUM(trip_miles) AS miles
FROM trips
WHERE pickup_datetime >= '2024-05-01' AND pickup_datetime < '2024-05-08';
```

#### Q3: Filtered Aggregation
```sql
SELECT COUNT(*) AS trip_count, SUM(base_passenger_fare) AS fare
FROM trips
WHERE pickup_datetime >= '2024-05-01' AND pickup_datetime < '2024-05-08'
  AND trip_miles > 2.0;
```

#### Q4: Group By
```sql
SELECT "PULocationID", COUNT(*) AS trips
FROM trips
WHERE pickup_datetime >= '2024-05-01' AND pickup_datetime < '2024-05-08'
GROUP BY "PULocationID";
```

#### Q5: Time Bucketing
```sql
SELECT date_trunc('hour', pickup_datetime) AS hour, COUNT(*) AS trips
FROM trips
WHERE pickup_datetime >= '2024-05-01' AND pickup_datetime < '2024-05-08'
GROUP BY hour
ORDER BY hour;
```

---

## Detailed Results

### Query Performance (milliseconds)

| Query | timeseries-table | ClickHouse | Delta+Spark | PostgreSQL | TimescaleDB |
|-------|------------------|------------|-------------|------------|-------------|
| Q1 (time-range) | **545** | 1,371 | 49,722 | 43,556 | 43,923 |
| Q2 (aggregation) | **317** | 319 | 552 | 3,565 | 529 |
| Q3 (filter+agg) | **353** | 312 | 391 | 2,035 | 475 |
| Q4 (group by) | **323** | 305 | 507 | 1,929 | 518 |
| Q5 (date_trunc) | **290** | 308 | 579 | 2,040 | 555 |

### Analysis

**Time-Range Query (Q1)**: This is where `timeseries-table` excels. The combination of:
- Parquet column pruning
- Time-based segment metadata
- DataFusion's efficient scanning

Results in 80× faster performance vs PostgreSQL and 2.5× vs ClickHouse.

**Aggregation Queries (Q2–Q5)**: Performance is competitive with ClickHouse (within ±15%). Both systems benefit from columnar storage. PostgreSQL's row-oriented storage shows in the 6–10× slower results.

**Delta + Spark**: The JVM startup overhead and Spark's execution model add latency, especially visible in Q1 where the full result set must be materialized.

---

## Reproduction

### Prerequisites

- Docker & Docker Compose
- ~50 GB disk space (for dataset + working files)
- 16+ GB RAM recommended

### Run

```bash
# Clone the repository
git clone https://github.com/mag1cfrog/timeseries-table-format
cd timeseries-table-format

# Run all benchmarks
./bench/run_all.sh
```

### Configuration

Edit `bench/config.env` to adjust:

```bash
# Dataset
START_MONTH="2024-04"   # First month to download
MONTHS=3                # Number of months

# Resource limits
CPU_LIMIT="8"
MEM_LIMIT="16g"

# Query parameters
QUERY_START="2024-05-01T00:00:00Z"
QUERY_END="2024-05-08T00:00:00Z"
MIN_MILES=2.0
```

### Output

Results are written to `bench/results/<timestamp>/`:

```
bench/results/20260109_135101/
├── clickhouse.csv
├── delta_spark.csv
├── postgres.csv
├── timescale.csv
├── timeseries_table.csv
├── influxdb3.csv
└── combined.csv
```

Each CSV contains:
```csv
system,test,file,rows,bytes,elapsed_ms,cpu_limit,mem_limit,notes
```

---

## Limitations & Caveats

1. **Single-node only**: All tests run on a single machine. Distributed performance characteristics may differ.

2. **Cold cache**: Each system is restarted between runs. Results reflect cold-cache performance.

3. **Default configurations**: Systems use mostly default settings. Production tuning could improve results.

4. **Parquet vs CSV**: `timeseries-table` and Delta use Parquet directly; PostgreSQL/TimescaleDB/ClickHouse ingest from CSV, adding parsing overhead.

5. **Query subset**: These queries represent common time-series patterns but don't cover all workloads (e.g., joins, complex window functions).

---

## Chart Generation

To regenerate the benchmark chart for the README:

```bash
python scripts/bench/generate_benchmark_chart_v2.py
```

Output:
- `docs/assets/benchmark-chart.png` (dark mode)
- `docs/assets/benchmark-chart-light.png` (light mode)

---

## Raw Data

Full benchmark results from our test run:

<details>
<summary>Click to expand raw CSV data</summary>

```csv
system,test,elapsed_ms
timeseries_table,bulk_ingest,1697
timeseries_table,daily_append (mean),335
timeseries_table,q1_time_range,545
timeseries_table,q2_agg,317
timeseries_table,q3_filter_agg,353
timeseries_table,q4_groupby,323
timeseries_table,q5_date_trunc,290
clickhouse,bulk_ingest,13093
clickhouse,daily_append (mean),1114
clickhouse,q1_time_range,1371
clickhouse,q2_agg,319
clickhouse,q3_filter_agg,312
clickhouse,q4_groupby,305
clickhouse,q5_date_trunc,308
delta_spark,bulk_ingest,24196
delta_spark,daily_append (mean),1454
delta_spark,q1_time_range,49722
delta_spark,q2_agg,552
delta_spark,q3_filter_agg,391
delta_spark,q4_groupby,507
delta_spark,q5_date_trunc,579
postgres,bulk_ingest,45528
postgres,daily_append (mean),1829
postgres,q1_time_range,43556
postgres,q2_agg,3565
postgres,q3_filter_agg,2035
postgres,q4_groupby,1929
postgres,q5_date_trunc,2040
timescale,bulk_ingest,86555
timescale,daily_append (mean),3197
timescale,q1_time_range,43923
timescale,q2_agg,529
timescale,q3_filter_agg,475
timescale,q4_groupby,518
timescale,q5_date_trunc,555
```

</details>
