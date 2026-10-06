import json
from pathlib import Path
import runpy
import subprocess
import sys

BENCHMARK = Path(__file__).resolve().parents[1] / "bench" / "sql_conversion.py"


def test_wide_streaming_benchmark(tmp_path):
    output = tmp_path / "wide.json"
    subprocess.run(
        [
            sys.executable,
            str(BENCHMARK),
            "--wide-streaming",
            "--wide-rows-per-segment",
            "1025",
            "--runs",
            "1",
            "--warmups",
            "0",
            "--json",
            str(output),
        ],
        check=True,
        capture_output=True,
        text=True,
        timeout=60,
    )
    report = json.loads(output.read_text())
    assert report["dataset"]["rows"] == 4100
    assert report["dataset"]["row_groups"] == [1, 1, 1, 1]
    results = report["wide_streaming_results"]
    assert len(results) == 8
    default = results[0]["samples"][0]
    for result in results:
        sample = result["samples"][0]
        assert 0 < sample["time_to_first_batch_s"] <= sample["total_s"]
        assert sample["close_s"] >= 0
        assert sample["rows_per_second"] > 0
        if sys.platform in ("linux", "darwin", "win32"):
            before = sample["process_peak_before_query_bytes"]
            peak = sample["peak_rss_bytes"]
            assert isinstance(before, int) and before > 0
            assert isinstance(peak, int) and peak >= before
        expected_batch_size = (
            1024
            if result["name"].startswith("small_batches")
            else default["effective_batch_size"]
        )
        expected_partitions = (
            1
            if result["name"].endswith("single_partition")
            else default["effective_target_partitions"]
        )
        assert sample["effective_batch_size"] == expected_batch_size > 0
        assert sample["effective_target_partitions"] == expected_partitions > 0
        if result["mode"] == "sql_reader_close_early":
            assert sample["batch_count"] == 1
            assert 0 < sample["row_count"] < 4100
        else:
            assert sample["row_count"] == 4100
            assert sample["total_arrow_bytes"] == 4100 * 16500
        maximum_rows = 1024 if result["name"].startswith("small_batches") else 1025
        assert sample["maximum_batch_arrow_bytes"] == maximum_rows * 16500


def test_benchmark_medians_with_even_sample_counts():
    benchmark = runpy.run_path(str(BENCHMARK))
    assert benchmark["_summarize_seconds"]([3.0, 1.0]) == {
        "min_s": 1.0,
        "median_s": 2.0,
        "runs_s": [3.0, 1.0],
    }
    assert benchmark["_fmt_optional_bytes"]([3 * 1024**2, None, 1024**2]) == "2.0 MiB"
