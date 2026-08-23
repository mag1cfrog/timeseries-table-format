import json

import pyarrow as pa
import pytest

import timeseries_table_format as ttf


def _set_protocol_features(
    table_root, *, reader: list[str] | None = None, writer: list[str] | None = None
) -> None:
    log_dir = table_root / "_timeseries_log"
    version = int((log_dir / "CURRENT").read_text().strip())
    commit_path = log_dir / f"{version:010}.json"
    commit = json.loads(commit_path.read_text())
    metadata = next(
        action["UpdateTableMeta"]
        for action in commit["actions"]
        if "UpdateTableMeta" in action
    )
    if reader is not None:
        metadata["required_reader_features"] = reader
    if writer is not None:
        metadata["required_writer_features"] = writer
    commit_path.write_text(json.dumps(commit))


def test_python_protocol_distinguishes_reads_and_writes_before_input_inspection(
    tmp_path,
):
    root = tmp_path / "table"
    table = ttf.TimeSeriesTable.create(
        table_root=str(root),
        index_column="ts",
        index_type="timestamp",
        index_granularity="1h",
        entity_columns=["symbol"],
    )
    table.append(
        pa.table(
            {
                "ts": pa.array([0, 3_600_000], type=pa.timestamp("ms")),
                "symbol": ["A", "A"],
            }
        )
    )
    _set_protocol_features(root, writer=["future_writer"])

    opened = ttf.TimeSeriesTable.open(str(root))
    session = ttf.Session()
    session.register_tstable("protocol_table", str(root))
    assert session.sql("SELECT COUNT(*) AS count FROM protocol_table")["count"].to_pylist() == [
        2
    ]

    class UninspectedSource:
        calls = 0

        def __arrow_c_stream__(self):
            self.calls += 1
            raise AssertionError("source must not be inspected")

    source = UninspectedSource()
    version_before = opened.version()
    with pytest.raises(
        ttf.TimeseriesTableError, match="unsupported table writer features"
    ) as append_error:
        opened.append(source)
    assert type(append_error.value) is ttf.TimeseriesTableError
    assert getattr(append_error.value, "table_root", None) == str(root)
    assert source.calls == 0
    assert opened.version() == version_before

    with pytest.raises(
        ttf.TimeseriesTableError, match="unsupported table writer features"
    ) as optimize_error:
        opened.optimize()
    assert type(optimize_error.value) is ttf.TimeseriesTableError
    assert getattr(optimize_error.value, "table_root", None) == str(root)
    assert opened.version() == version_before

    _set_protocol_features(
        root, reader=["future_reader"], writer=["future_writer"]
    )
    with pytest.raises(
        ttf.TimeseriesTableError, match="unsupported table reader features"
    ) as open_error:
        ttf.TimeSeriesTable.open(str(root))
    assert type(open_error.value) is ttf.TimeseriesTableError
    assert getattr(open_error.value, "table_root", None) == str(root)
