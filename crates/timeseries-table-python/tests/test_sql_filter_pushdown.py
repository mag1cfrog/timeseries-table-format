import json
from pathlib import Path
import subprocess
import sys

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import timeseries_table_format as ttf


@pytest.mark.parametrize("registration", ["tstable", "parquet"])
@pytest.mark.parametrize("streaming", [False, True])
def test_filter_pushdown_preserves_query_results(tmp_path, registration, streaming):
    data = pa.table(
        {
            "entity": [0] * 6,
            "tick": pa.array(range(6), pa.uint64()),
            "record_id": ["0", "b", None, "3", "4", "z"],
            "value": [0, None, 2, None, 4, 5],
            **{
                f"payload_{column}": pa.array(
                    [
                        None if row == 2 else bytes([row, column]) * 1024
                        for row in range(6)
                    ],
                    pa.binary(),
                )
                for column in range(8)
            },
        }
    )
    session = ttf.Session()
    if registration == "tstable":
        root = tmp_path / "table"
        table = ttf.TimeSeriesTable.create(
            table_root=str(root),
            index_column="tick",
            index_type="uint64",
            index_granularity=1,
            entity_columns=["entity"],
        )
        table.append(data, max_rows_per_row_group=3)
        session.register_tstable("prices", str(root))
    else:
        path = tmp_path / "data.parquet"
        pq.write_table(data, path, row_group_size=3)
        session.register_parquet("prices", str(path))

    cases = [
        (data.column_names, "record_id = 'b'", [1]),
        (["tick", "payload_7"], "record_id IS NULL", [2]),
        (["tick"], "value IS NULL OR value >= 3 ORDER BY tick", [1, 3, 4, 5]),
        (
            ["tick", "payload_0"],
            "record_id IN ('0', 'b') AND (value IS NULL OR value = 0) ORDER BY tick",
            [0, 1],
        ),
        (["tick"], "try_cast(record_id AS BIGINT) = 4", [4]),
        (["payload_0"], "record_id = 'missing'", []),
        (["tick"], "record_id IS NOT NULL ORDER BY tick LIMIT 2", [0, 1]),
    ]
    for pushdown in (None, "false", "true"):
        if pushdown is not None:
            session.sql(
                "SET datafusion.execution.parquet.pushdown_filters = " + pushdown
            )
        for columns, predicate, indices in cases:
            query = f"SELECT {', '.join(columns)} FROM prices WHERE {predicate}"
            if streaming:
                reader = session.sql_reader(query)
                try:
                    actual = reader.read_all()
                finally:
                    reader.close()
            else:
                actual = session.sql(query)
            expected = data.select(columns).take(pa.array(indices, pa.int64()))
            # Registered Parquet can return string/binary views of the same values.
            assert actual.cast(expected.schema).equals(expected), (pushdown, query)


def test_session_enables_filter_pushdown_by_default():
    session = ttf.Session()
    session.sql("SET datafusion.catalog.information_schema = true")
    setting = "datafusion.execution.parquet.pushdown_filters"
    assert session.sql(f"SHOW {setting}")["value"].to_pylist() == ["true"]
    session.sql(f"SET {setting} = false")
    assert session.sql(f"SHOW {setting}")["value"].to_pylist() == ["false"]


def test_filter_pushdown_benchmark(tmp_path):
    script = Path(__file__).resolve().parents[1] / "bench" / "sql_filter_pushdown.py"
    output = tmp_path / "result.json"
    subprocess.run(
        [
            sys.executable,
            str(script),
            "--rows-per-segment",
            "32",
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
    assert report["dataset"]["rows"] == 128
    assert report["dataset"]["row_groups"] == [1, 1, 1, 1]
    results = report["results"]
    assert {(result["case"], result["pushdown_filters"]) for result in results} == {
        (case, enabled)
        for case in ("selective", "nonselective", "unfiltered")
        for enabled in (False, True)
    }
    for result in results:
        assert len(result["samples"]) == 1
        sample = result["samples"][0]
        expected_rows = {"selective": 1, "nonselective": 127, "unfiltered": 128}[
            result["case"]
        ]
        assert sample["row_count"] == expected_rows
        assert sample["result_arrow_bytes"] == expected_rows * 16500
        assert sample["total_s"] > 0 and sample["rows_per_second"] > 0
        assert (
            sample["effective_parquet.pushdown_filters"]
            == str(result["pushdown_filters"]).lower()
        )
        assert int(sample["effective_batch_size"]) > 0
        assert int(sample["effective_target_partitions"]) > 0
        if sys.platform in ("linux", "darwin", "win32"):
            assert (
                sample["peak_rss_bytes"]
                >= sample["process_peak_before_query_bytes"]
                > 0
            )
        if result["case"] == "selective":
            plan = "\n".join(row["plan"] for row in sample["explain_analyze"])
            scan = next(line for line in plan.splitlines() if "DataSourceExec:" in line)
            scanned = 1 if result["pushdown_filters"] else 128
            pruned = 127 if result["pushdown_filters"] else 0
            assert f"metrics=[output_rows={scanned}," in scan
            assert f"pushdown_rows_pruned={pruned}," in scan
