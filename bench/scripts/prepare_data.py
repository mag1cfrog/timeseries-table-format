#!/usr/bin/env python3
import argparse
import csv
import datetime as dt
import os
import pathlib
import subprocess
import shutil
import sys
import urllib.request

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pyarrow.csv as pacsv


def _escape_lp_key(val: str) -> str:
    return val.replace(" ", "\\ ").replace(",", "\\,").replace("=", "\\=")


def _escape_lp_string(val: str) -> str:
    return val.replace("\\", "\\\\").replace("\"", "\\\"")


def _to_ns(ts_val) -> int:
    if isinstance(ts_val, dt.datetime):
        return int(ts_val.timestamp() * 1_000_000_000)
    if isinstance(ts_val, dt.date):
        return int(dt.datetime(ts_val.year, ts_val.month, ts_val.day).timestamp() * 1_000_000_000)
    try:
        return int(ts_val)
    except Exception:
        return 0


def month_range(start_ym: str, count: int):
    year, month = start_ym.split("-")
    year = int(year)
    month = int(month)
    for _ in range(count):
        yield f"{year:04d}-{month:02d}"
        month += 1
        if month > 12:
            month = 1
            year += 1


def download_file(url: str, dest: pathlib.Path):
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists() and is_parquet_file(dest):
        return
    if dest.exists():
        dest.unlink()
    tmp = dest.with_suffix(dest.suffix + ".part")
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "curl/8.0.0",
            "Accept": "*/*",
        },
    )
    try:
        with urllib.request.urlopen(req) as resp, open(tmp, "wb") as f:
            while True:
                chunk = resp.read(1024 * 1024)
                if not chunk:
                    break
                f.write(chunk)
    except urllib.error.HTTPError as exc:
        if exc.code != 403:
            raise
        cmd = ["curl", "-L", "-A", "curl/8.0.0", "-o", str(tmp), url]
        result = subprocess.run(cmd, check=False)
        if result.returncode != 0:
            raise
    tmp.rename(dest)
    if not is_parquet_file(dest):
        dest.unlink(missing_ok=True)
        raise RuntimeError(f"Downloaded file is not valid parquet: {dest}")


def is_parquet_file(path: pathlib.Path) -> bool:
    try:
        if path.stat().st_size < 12:
            return False
        with open(path, "rb") as f:
            head = f.read(4)
            if head != b"PAR1":
                return False
            f.seek(-4, os.SEEK_END)
            tail = f.read(4)
            return tail == b"PAR1"
    except Exception:
        return False


def split_daily(parquet_path: pathlib.Path, out_dir: pathlib.Path, time_col: str):
    dataset = ds.dataset(parquet_path)
    scanner = dataset.scanner(batch_size=64 * 1024)

    writers = {}
    try:
        for batch in scanner.to_batches():
            ts_col = batch.column(time_col)
            date_arr = pc.cast(ts_col, pa.date32())
            unique_dates = pc.unique(date_arr).to_pylist()
            for date_val in unique_dates:
                mask = pc.equal(date_arr, date_val)
                sub = batch.filter(mask)
                table = pa.Table.from_batches([sub])
                if isinstance(date_val, dt.date):
                    date_str = date_val.isoformat()
                else:
                    date_str = (dt.date(1970, 1, 1) + dt.timedelta(days=int(date_val))).isoformat()
                out_path = out_dir / f"fhvhv_{date_str}.parquet"
                if date_str not in writers:
                    out_path.parent.mkdir(parents=True, exist_ok=True)
                    writers[date_str] = pq.ParquetWriter(out_path, table.schema)
                writers[date_str].write_table(table)
    finally:
        for writer in writers.values():
            writer.close()


def parquet_rows(path: pathlib.Path) -> int:
    pf = pq.ParquetFile(path)
    return pf.metadata.num_rows


def write_csv_from_parquet(parquet_path: pathlib.Path, csv_path: pathlib.Path):
    table = pq.read_table(parquet_path)
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    pacsv.write_csv(table, csv_path)


def write_line_protocol(parquet_path: pathlib.Path, lp_path: pathlib.Path, time_col: str, measurement: str):
    dataset = ds.dataset(parquet_path)
    scanner = dataset.scanner(batch_size=16 * 1024)
    lp_path.parent.mkdir(parents=True, exist_ok=True)

    with open(lp_path, "w", encoding="utf-8") as f:
        for batch in scanner.to_batches():
            rows = batch.to_pylist()
            for row in rows:
                ts_val = row.get(time_col)
                if ts_val is None:
                    continue
                ts_ns = _to_ns(ts_val)
                if ts_ns == 0:
                    continue
                fields = []
                for key, val in row.items():
                    if key == time_col or val is None:
                        continue
                    field_key = _escape_lp_key(str(key))
                    if isinstance(val, bool):
                        fields.append(f"{field_key}={'true' if val else 'false'}")
                    elif isinstance(val, int):
                        fields.append(f"{field_key}={val}i")
                    elif isinstance(val, float):
                        if val != val:
                            continue
                        fields.append(f"{field_key}={val}")
                    else:
                        sval = _escape_lp_string(str(val))
                        fields.append(f"{field_key}=\"{sval}\"")
                if not fields:
                    continue
                measurement_key = _escape_lp_key(measurement)
                f.write(f"{measurement_key} {','.join(fields)} {ts_ns}\n")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--file-prefix", required=True)
    parser.add_argument("--start-month", required=True)
    parser.add_argument("--months", type=int, required=True)
    parser.add_argument("--time-column", required=True)
    parser.add_argument("--influx-measurement", default="trips")
    parser.add_argument("--dataset-dir", required=True)
    parser.add_argument("--local-raw-dir", default="")
    parser.add_argument("--generate-csv", action="store_true")
    parser.add_argument("--generate-influx-lp", action="store_true")
    args = parser.parse_args()

    dataset_dir = pathlib.Path(args.dataset_dir)
    raw_dir = dataset_dir / "raw"
    daily_dir = dataset_dir / "daily"
    csv_dir = dataset_dir / "csv"
    influx_dir = dataset_dir / "influx"
    manifest_path = dataset_dir / "manifest.csv"

    raw_files = []
    for month in month_range(args.start_month, args.months):
        name = f"{args.file_prefix}{month}.parquet"
        url = f"{args.base_url}/{name}"
        dest = raw_dir / name
        if args.local_raw_dir:
            local_path = pathlib.Path(args.local_raw_dir) / name
            if local_path.exists():
                print(f"Using local file {local_path} -> {dest}")
                dest.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(local_path, dest)
            else:
                print(f"Downloading {url} -> {dest}")
                download_file(url, dest)
        else:
            print(f"Downloading {url} -> {dest}")
            download_file(url, dest)
        raw_files.append(dest)

    print("Splitting monthly files into daily parquet...")
    for raw in raw_files:
        split_daily(raw, daily_dir, args.time_column)

    if args.generate_csv:
        print("Generating CSVs for raw and daily files...")
        for raw in raw_files:
            csv_path = csv_dir / "raw" / (raw.stem + ".csv")
            write_csv_from_parquet(raw, csv_path)
        for daily in sorted(daily_dir.glob("fhvhv_*.parquet")):
            date_part = daily.stem.replace("fhvhv_", "")
            csv_path = csv_dir / "daily" / f"fhvhv_{date_part}.csv"
            write_csv_from_parquet(daily, csv_path)

    if args.generate_influx_lp:
        print("Generating Influx line protocol files...")
        for raw in raw_files:
            lp_path = influx_dir / "raw" / (raw.stem + ".lp")
            write_line_protocol(raw, lp_path, args.time_column, args.influx_measurement)
        for daily in sorted(daily_dir.glob("fhvhv_*.parquet")):
            date_part = daily.stem.replace("fhvhv_", "")
            lp_path = influx_dir / "daily" / f"fhvhv_{date_part}.lp"
            write_line_protocol(daily, lp_path, args.time_column, args.influx_measurement)

    print("Writing manifest...")
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    with open(manifest_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["path", "rows", "bytes", "kind", "format"])
        for raw in raw_files:
            writer.writerow([
                str(raw.relative_to(dataset_dir)),
                parquet_rows(raw),
                raw.stat().st_size,
                "raw",
                "parquet",
            ])
        for daily in sorted(daily_dir.glob("fhvhv_*.parquet")):
            writer.writerow([
                str(daily.relative_to(dataset_dir)),
                parquet_rows(daily),
                daily.stat().st_size,
                "daily",
                "parquet",
            ])
        if args.generate_csv:
            for raw in raw_files:
                csv_path = csv_dir / "raw" / (raw.stem + ".csv")
                writer.writerow([
                    str(csv_path.relative_to(dataset_dir)),
                    parquet_rows(raw),
                    csv_path.stat().st_size,
                    "raw",
                    "csv",
                ])
            for daily in sorted((csv_dir / "daily").glob("fhvhv_*.csv")):
                date_part = daily.stem.replace("fhvhv_", "")
                parquet_path = daily_dir / f"fhvhv_{date_part}.parquet"
                rows = parquet_rows(parquet_path) if parquet_path.exists() else ""
                writer.writerow([
                    str(daily.relative_to(dataset_dir)),
                    rows,
                    daily.stat().st_size,
                    "daily",
                    "csv",
                ])
        if args.generate_influx_lp:
            for raw in raw_files:
                lp_path = influx_dir / "raw" / (raw.stem + ".lp")
                writer.writerow([
                    str(lp_path.relative_to(dataset_dir)),
                    parquet_rows(raw),
                    lp_path.stat().st_size,
                    "raw",
                    "lp",
                ])
            for daily in sorted((influx_dir / "daily").glob("fhvhv_*.lp")):
                date_part = daily.stem.replace("fhvhv_", "")
                parquet_path = daily_dir / f"fhvhv_{date_part}.parquet"
                rows = parquet_rows(parquet_path) if parquet_path.exists() else ""
                writer.writerow([
                    str(daily.relative_to(dataset_dir)),
                    rows,
                    daily.stat().st_size,
                    "daily",
                    "lp",
                ])

    print("Done.")


if __name__ == "__main__":
    sys.exit(main())
