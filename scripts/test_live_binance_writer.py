"""Run manual live-source checks; requires PyArrow and the built Rust example.

Usage: python test_live_binance_writer.py --output /tmp/live-writer-check
Longer run: add --soak-seconds 300 --reconnect-runs 3 --restart-cycles 3.
Network access is intentional. This script is not part of offline CI tests.
"""

import argparse
import datetime as dt
import json
from pathlib import Path
import queue
import subprocess
import threading
import time
import urllib.request

import pyarrow.parquet as pq


def committed_rows(root):
    log = root / "_timeseries_log"
    version = int((log / "CURRENT").read_text())
    files = {}
    for number in range(1, version + 1):
        commit = json.loads((log / f"{number:010}.json").read_text())
        for action in commit["actions"]:
            if "AddSegment" in action:
                segment = action["AddSegment"]
                files[segment["path"]] = segment
            if "RemoveSegment" in action:
                files.pop(action["RemoveSegment"]["path"])
    rows = []
    for path, segment in files.items():
        segment_rows = pq.ParquetFile(root / path).read().to_pylist()
        assert len(segment_rows) == segment["row_count"], "segment row count mismatch"
        rows.extend(segment_rows)
    rows.sort(key=lambda row: row["trade_id"])
    ids = [row["trade_id"] for row in rows]
    assert ids, "no rows committed"
    assert len(ids) == len(set(ids)), "duplicate committed trade IDs"
    assert ids == list(range(ids[0], ids[-1] + 1)), "committed trade ID gap"
    return version, files, rows


def verify_remote(root):
    version, files, rows = committed_rows(root)
    for start in range(0, len(rows), 1000):
        expected_rows = rows[start : start + 1000]
        url = (
            "https://data-api.binance.vision/api/v3/aggTrades"
            f"?symbol=BTCUSDT&fromId={expected_rows[0]['trade_id']}"
            f"&limit={len(expected_rows)}"
        )
        request = urllib.request.Request(
            url, headers={"User-Agent": "timeseries-table-format-live-check"}
        )
        with urllib.request.urlopen(request, timeout=15) as response:
            trades = json.load(response)
        actual_rows = [
            {
                "trade_id": trade["a"],
                "trade_time": dt.datetime(1970, 1, 1)
                + dt.timedelta(milliseconds=trade["T"]),
                "price": trade["p"],
                "quantity": trade["q"],
                "buyer_is_maker": trade["m"],
            }
            for trade in trades
        ]
        assert expected_rows == actual_rows, "stored data differs from source REST data"
        time.sleep(0.1)
    referenced = {root / path for path in files}
    return {
        "version": version,
        "segments": len(files),
        "rows": len(rows),
        "first_id": rows[0]["trade_id"],
        "last_id": rows[-1]["trade_id"],
        "duplicate_ids": 0,
        "missing_ids": 0,
        "source_rows_match": True,
        "unreferenced_parquet_files": len(
            set((root / "data").rglob("*.parquet")) - referenced
        ),
    }


def run(binary, root, name, *options, timeout=120):
    started = time.monotonic()
    try:
        result = subprocess.run(
            [str(binary), str(root), *map(str, options)],
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired as error:
        (root.parent / f"{name}.stdout.jsonl").write_bytes(error.stdout or b"")
        (root.parent / f"{name}.stderr.log").write_bytes(error.stderr or b"")
        raise
    (root.parent / f"{name}.stdout.jsonl").write_text(result.stdout)
    (root.parent / f"{name}.stderr.log").write_text(result.stderr)
    assert result.returncode == 0, result.stderr
    events = [json.loads(line) for line in result.stdout.splitlines()]
    assert events[-1]["event"] == "finished", "run did not finish"
    commits = [event for event in events if event["event"] == "committed"]
    first_version = events[0]["version"]
    flags = dict(zip(options[::2], options[1::2], strict=True))
    expected_commits = int(flags.get("--commits", 3))
    expected_rows = int(flags.get("--rows", 50))
    assert len(commits) == expected_commits, "run did not make all requested commits"
    assert all(event["rows"] == expected_rows for event in commits), (
        "commit row count mismatch"
    )
    assert events[-1]["version"] == first_version + expected_commits
    assert [event["version"] for event in commits] == list(
        range(first_version + 1, first_version + len(commits) + 1)
    ), "each commit must advance exactly one version"
    return events, time.monotonic() - started


def kill_with_pending_rows(binary, root, name="killed"):
    events = queue.Queue()
    captured = []
    with (root.parent / f"{name}.stderr.log").open("w") as errors:
        process = subprocess.Popen(
            [
                str(binary),
                str(root),
                "--commits",
                "100",
                "--rows",
                "100",
                "--delay-ms",
                "1000",
            ],
            stdout=subprocess.PIPE,
            stderr=errors,
            text=True,
        )

        def read_stdout():
            for line in process.stdout:
                events.put(json.loads(line))
            events.put(None)

        reader = threading.Thread(target=read_stdout, daemon=True)
        reader.start()
        try:
            while True:
                event = events.get(timeout=60)
                assert event is not None, "process stopped before writing pending rows"
                captured.append(event)
                if event["event"] == "pending":
                    process.kill()
                    process.wait(timeout=15)
                    assert process.returncode != 0, (
                        "process was not forcibly terminated"
                    )
                    break
        finally:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=15)
            reader.join(timeout=5)
            process.stdout.close()
    (root.parent / f"{name}.stdout.jsonl").write_text(
        "".join(json.dumps(event) + "\n" for event in captured)
    )
    assert captured[-1]["rows"] > 0
    assert not any(event["event"] == "committed" for event in captured)
    return captured[-1]


def save_report(output, report):
    (output / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"completed": list(report)}), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--binary", type=Path, default=Path("target/debug/examples/live_binance_writer")
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--soak-seconds", type=int, default=60)
    parser.add_argument("--reconnect-runs", type=int, default=1)
    parser.add_argument("--restart-cycles", type=int, default=1)
    args = parser.parse_args()
    if args.soak_seconds < 60 or args.reconnect_runs < 1 or args.restart_cycles < 1:
        parser.error(
            "soak-seconds must be at least 60; run/cycle counts must be positive"
        )
    args.output.mkdir(parents=True, exist_ok=False)
    binary = args.binary.resolve(strict=True)
    report = {}

    for number in range(1, args.reconnect_runs + 1):
        name = "reconnect" if args.reconnect_runs == 1 else f"reconnect_{number}"
        root = args.output / name
        events, elapsed = run(
            binary, root, name, "--disconnect-after", 15, "--queue", 8
        )
        assert sum(event["event"] == "connected" for event in events) >= 2
        assert events[-1]["source"]["reconnects"] >= 1
        assert events[-1]["source"]["replayed_rows"] > 0
        report[name] = verify_remote(root) | {
            "seconds": elapsed,
            "source": events[-1]["source"],
        }
        save_report(args.output, report)

    root = args.output / "slow_consumer"
    events, elapsed = run(
        binary, root, "slow_consumer", "--queue", 2, "--delay-ms", 200
    )
    assert events[0]["queue_capacity"] == 2
    assert events[-1]["source"]["backpressure_events"] > 0
    assert events[-1]["source"]["blocked_ms"] > 0
    report["slow_consumer"] = verify_remote(root) | {
        "seconds": elapsed,
        "source": events[-1]["source"],
    }
    save_report(args.output, report)

    root = args.output / "restart"
    run(binary, root, "before_kill", "--commits", 1)
    for number in range(1, args.restart_cycles + 1):
        suffix = "" if args.restart_cycles == 1 else f"_{number}"
        before_version, _, before_rows = committed_rows(root)
        expected_next = before_rows[-1]["trade_id"] + 1
        pending = kill_with_pending_rows(binary, root, f"killed{suffix}")
        after_version, _, after_rows = committed_rows(root)
        assert after_version == before_version
        assert after_rows == before_rows, "uncommitted rows became visible"
        events, elapsed = run(binary, root, f"after_restart{suffix}", "--commits", 2)
        assert events[0]["resume_from"] == expected_next
        result = verify_remote(root)
        assert result["last_id"] >= pending["last_id"], (
            "lost pending rows were not replayed"
        )
        assert events[-1]["source"]["replayed_rows"] > 0
        report[f"restart{suffix}"] = result | {
            "seconds": elapsed,
            "resume_from": expected_next,
            "killed_pending": pending,
        }
        save_report(args.output, report)

    root = args.output / "soak"
    soak_commits = (args.soak_seconds + 4) // 5
    events, elapsed = run(
        binary,
        root,
        "soak",
        "--commits",
        soak_commits,
        "--rows",
        10,
        "--delay-ms",
        5000,
        timeout=max(180, args.soak_seconds * 2),
    )
    assert events[-1]["version"] == soak_commits + 1
    assert elapsed >= args.soak_seconds, "soak did not meet its minimum duration"
    report["soak"] = verify_remote(root) | {
        "seconds": elapsed,
        "source": events[-1]["source"],
    }
    save_report(args.output, report)
    print(json.dumps(report, indent=2), flush=True)


if __name__ == "__main__":
    main()
