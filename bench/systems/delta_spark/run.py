#!/usr/bin/env python3
import csv
import os
import re
import time
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def now_ms():
    # Monotonic clock avoids negative durations if wall time shifts.
    return int(time.monotonic() * 1000)


def load_manifest(path):
    data = {}
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            data[row["path"]] = row
    return data


def iso_date(iso_ts: str) -> str:
    return iso_ts.split("T", 1)[0]


def inject_partition_predicate(sql: str, start_date: str, end_date: str, partition_col: str) -> str:
    base = sql.strip().rstrip(";")
    predicate = f"{partition_col} >= '{start_date}' AND {partition_col} < '{end_date}'"
    lower = base.lower()
    where_match = re.search(r"\bwhere\b", lower)
    clause_match = re.search(r"\b(group\s+by|order\s+by|limit)\b", lower)
    if where_match is None:
        if clause_match is None:
            return f"{base} WHERE {predicate}"
        cut = clause_match.start()
        return f"{base[:cut]} WHERE {predicate} {base[cut:]}"
    start = where_match.end()
    if clause_match is None or clause_match.start() < start:
        return f"{base} AND {predicate}"
    cut = clause_match.start()
    return f"{base[:cut]} AND {predicate} {base[cut:]}"


def select_list(cols):
    return ", ".join(f"`{c}`" for c in cols)


def rewrite_select_star(sql: str, select_cols) -> str:
    needle = "select *"
    lower = sql.lower()
    idx = lower.find(needle)
    if idx == -1:
        return sql
    return sql[:idx] + f"SELECT {select_list(select_cols)}" + sql[idx + len(needle):]


def render_sql(path, start, end, min_miles, partition_col, select_cols):
    with open(path, "r", encoding="utf-8") as f:
        sql = f.read()
    sql = sql.replace("{START}", start)
    sql = sql.replace("{END}", end)
    sql = sql.replace("{MIN_MILES}", str(min_miles))
    sql = rewrite_select_star(sql, select_cols)
    return inject_partition_predicate(sql, iso_date(start), iso_date(end), partition_col)


def execute_query(df, full_scan: bool):
    if full_scan:
        cols = ",".join(f"`{c}`" for c in df.columns)
        df.selectExpr(f"sum(hash({cols})) as _h").collect()
    else:
        df.collect()


def main():
    results_csv = Path(os.environ["RESULTS_CSV"])
    manifest_csv = os.environ["MANIFEST_CSV"]
    dataset_dir = Path(os.environ["DATASET_DIR"])
    bulk_rel = os.environ["BULK_REL"]
    daily_dir = Path(os.environ["DAILY_DIR"])
    queries_dir = Path(os.environ["QUERIES_DIR"])
    query_start = os.environ["QUERY_START"]
    query_end = os.environ["QUERY_END"]
    min_miles = os.environ["MIN_MILES"]
    cpu_limit = os.environ.get("CPU_LIMIT", "")
    mem_limit = os.environ.get("MEM_LIMIT", "")
    time_column = os.environ.get("TIME_COLUMN", "pickup_datetime")
    partition_col = "pickup_date"

    manifest = load_manifest(manifest_csv)

    if not results_csv.exists():
        results_csv.parent.mkdir(parents=True, exist_ok=True)
        with open(results_csv, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["system","test","file","rows","bytes","elapsed_ms","cpu_limit","mem_limit","notes"])

    def emit(test, rel_path, elapsed_ms, notes=""):
        row = manifest.get(rel_path, {})
        with open(results_csv, "a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                "delta_spark",
                test,
                rel_path,
                row.get("rows", ""),
                row.get("bytes", ""),
                elapsed_ms,
                cpu_limit,
                mem_limit,
                notes,
            ])

    spark = (
        SparkSession.builder
        .appName("delta-bench")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

    table_path_bulk = os.environ["DELTA_TABLE_PATH_BULK"]
    table_path_daily = os.environ["DELTA_TABLE_PATH_DAILY"]
    bulk_path = dataset_dir / bulk_rel

    def with_partition(df):
        return df.withColumn(partition_col, F.to_date(F.col(time_column)))

    print("delta_spark: bulk ingest start", flush=True)
    start = now_ms()
    bulk_df = with_partition(spark.read.parquet(str(bulk_path)))
    bulk_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").partitionBy(partition_col).save(table_path_bulk)
    elapsed = now_ms() - start
    emit("bulk_ingest", bulk_rel, elapsed)
    print(f"delta_spark: bulk ingest done ({elapsed} ms)", flush=True)

    print("delta_spark: daily table init", flush=True)
    bulk_df.limit(0).write.format("delta").mode("overwrite").option("overwriteSchema", "true").partitionBy(partition_col).save(table_path_daily)
    print("delta_spark: daily appends start", flush=True)

    daily_files = sorted(daily_dir.glob("fhvhv_*.parquet"))
    for file_path in daily_files:
        rel = f"daily/{file_path.name}"
        start = now_ms()
        with_partition(spark.read.parquet(str(file_path))).write.format("delta").mode("append").partitionBy(partition_col).save(table_path_daily)
        elapsed = now_ms() - start
        emit("daily_append", rel, elapsed)

    print("delta_spark: register temp view", flush=True)
    base_df = spark.read.format("delta").load(table_path_daily)
    data_cols = [c for c in base_df.columns if c != partition_col]
    base_df.createOrReplaceTempView("trips")

    print("delta_spark: warmup", flush=True)
    spark.sql("SELECT * FROM trips LIMIT 1").collect()

    for q in [
        "q1_time_range",
        "q2_agg",
        "q3_filter_agg",
        "q4_groupby",
        "q5_date_trunc",
    ]:
        print(f"delta_spark: query {q}", flush=True)
        sql = render_sql(queries_dir / f"{q}.sql", query_start, query_end, min_miles, partition_col, data_cols)
        start = now_ms()
        df = spark.sql(sql)
        execute_query(df, q == "q1_time_range")
        elapsed = now_ms() - start
        emit(q, "", elapsed)

    print("delta_spark: done", flush=True)
    spark.stop()


if __name__ == "__main__":
    main()
