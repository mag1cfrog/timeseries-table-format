from pathlib import Path
from tempfile import TemporaryDirectory

import pyarrow as pa
import pyarrow.compute as pc

import timeseries_table_format as ttf


def run(*, table_root: Path) -> pa.Table:
    table = ttf.TimeSeriesTable.create(
        table_root=str(table_root),
        index_column="tick",
        index_type="int64",
        index_granularity=1,
        entity_columns=["device"],
    )
    table.append(
        pa.table(
            {
                "tick": [0, 0, 1, 1],
                "device": ["A", "B", "A", "B"],
                "reading": [1.0, 2.0, 3.0, 4.0],
            }
        )
    )
    # Add destinations before capturing the version used to compute their values.
    table.add_columns(pa.schema([pa.field("quality", pa.float64())]))
    computed_from_version = table.version()
    session = ttf.Session()
    session.register_tstable("readings", str(table_root))
    original = session.sql("SELECT * FROM readings ORDER BY tick, device")
    # Value computation belongs to the caller. Keep exact key types and annotations.
    assignments = original.select(["tick", "device"]).append_column(
        original.schema.field("quality"), pc.divide(original["reading"], 4.0)
    )
    report = table.update_rows(
        assignments.to_reader(max_chunksize=2),
        columns=["quality"],
        expected_version=computed_from_version,
    )
    assert report.starting_version == computed_from_version
    assert report.committed_version == table.version()
    assert report.rows_updated == 4 and not report.no_op
    print(report)

    # SQL sees value changes without re-registering an unchanged schema.
    assert (
        session.sql("SELECT sum(quality) total FROM readings")["total"][0].as_py()
        == 2.5
    )
    reopened = ttf.TimeSeriesTable.open(str(table_root))
    assert reopened.version() == report.committed_version
    session.register_tstable("reopened", str(table_root))
    return session.sql("SELECT * FROM reopened ORDER BY tick, device")


if __name__ == "__main__":
    with TemporaryDirectory() as directory:
        print(run(table_root=Path(directory) / "readings"))
