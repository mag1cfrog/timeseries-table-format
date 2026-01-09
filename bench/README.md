# Benchmark harness

Single-command, Dockerized benchmark suite for comparing this table format
against PostgreSQL, TimescaleDB, ClickHouse, InfluxDB 3, and Delta Lake (Spark).

## Quick start

```bash
./bench/run_all.sh
```

This will:
- Download NYC FHVHV Parquet data
- Split monthly Parquet into daily files
- Optionally generate CSVs for Postgres/Timescale
- Start Docker services as needed
- Run append + query benchmarks
- Emit per-system CSVs + a combined CSV under `bench/results/<timestamp>/`

## Config

Edit `bench/config.env`:
- `START_MONTH`, `MONTHS`, `TIME_COLUMN`
- `CPU_LIMIT`, `MEM_LIMIT`
- `QUERY_START`, `QUERY_END`, `MIN_MILES`
- Images + InfluxDB settings
- `GENERATE_INFLUX_LP=1` will generate line protocol files for InfluxDB 3

## Dataset
By default we use NYC FHVHV data from:
`https://d37ci6vzurychx.cloudfront.net/trip-data`

Files are stored under:
- `bench/datasets/raw/`
- `bench/datasets/daily/`
- `bench/datasets/csv/` (when enabled)
- `bench/datasets/manifest.csv` (rows/bytes per file)

## Notes / caveats
- **InfluxDB 3**: line protocol files are generated under
  `bench/datasets/influx/` when `GENERATE_INFLUX_LP=1`. If data is missing,
  the runner will skip with a note in the CSV. Auth is disabled in Compose.
  InfluxDB 3 Core enforces a file-scan limit, so wide-range SQL queries are
  recorded as `not_supported_core_file_limit` in results (ingest-only).
- **ClickHouse**: uses `clickhouse-client` with CSVWithNames and best-effort
  datetime parsing.
- **Delta Lake** uses Spark local mode with `--packages io.delta:delta-spark_2.12:3.1.0`.
- **Resource limits** are applied via Docker Compose (`cpus`, `mem_limit`).

## Results
Each CSV follows:

```
system,test,file,rows,bytes,elapsed_ms,cpu_limit,mem_limit,notes
```

`combined.csv` concatenates all per-system results.
