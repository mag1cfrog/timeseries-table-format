#!/usr/bin/env python3
import csv
import os
import time
from pathlib import Path

from pyspark.sql import SparkSession


def now_ms():
    return int(time.time() * 1000)


def load_manifest(path):
    data = {}
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            data[row["path"]] = row
    return data


def render_sql(path, start, end, min_miles):
    with open(path, "r", encoding="utf-8") as f:
        sql = f.read()
    sql = sql.replace("{START}", start)
    sql = sql.replace("{END}", end)
    sql = sql.replace("{MIN_MILES}", str(min_miles))
    return sql


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

    table_path = os.environ["DELTA_TABLE_PATH"]
    bulk_path = dataset_dir / bulk_rel

    start = now_ms()
    spark.read.parquet(str(bulk_path)).write.format("delta").mode("overwrite").save(table_path)
    elapsed = now_ms() - start
    emit("bulk_ingest", bulk_rel, elapsed)

    daily_files = sorted(daily_dir.glob("fhvhv_*.parquet"))
    for file_path in daily_files:
        rel = f"daily/{file_path.name}"
        start = now_ms()
        spark.read.parquet(str(file_path)).write.format("delta").mode("append").save(table_path)
        elapsed = now_ms() - start
        emit("daily_append", rel, elapsed)

    spark.read.format("delta").load(table_path).createOrReplaceTempView("trips")

    for q in [
        "q1_time_range",
        "q2_agg",
        "q3_filter_agg",
        "q4_groupby",
        "q5_date_trunc",
        "q6_date_bin",
    ]:
        sql = render_sql(queries_dir / f"{q}.sql", query_start, query_end, min_miles)
        start = now_ms()
        spark.sql(sql).collect()
        elapsed = now_ms() - start
        emit(q, "", elapsed)

    spark.stop()


if __name__ == "__main__":
    main()
