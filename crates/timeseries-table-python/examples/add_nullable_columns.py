from pathlib import Path
from tempfile import TemporaryDirectory

import pyarrow as pa

import timeseries_table_format as ttf


def run(*, table_root: Path) -> pa.Table:
    table = ttf.TimeSeriesTable.create(
        table_root=str(table_root),
        index_column="tick",
        index_type="int64",
        index_granularity=1,
        entity_columns=["device"],
    )
    # The first append establishes the canonical schema.
    table.append(
        pa.table({"tick": [0, 0], "device": ["A", "B"], "reading": [1.0, 2.0]})
    )
    session = ttf.Session()
    session.register_tstable("readings", str(table_root))

    version = table.add_columns(
        pa.schema(
            [
                pa.field("quality", pa.float64(), nullable=True),
                pa.field("reviewed", pa.bool_(), nullable=True),
            ]
        )
    )
    assert version == table.version() == 3
    # Replace the existing registration to expose the new schema.
    session.register_tstable("readings", str(table_root))
    assert session.sql("SELECT quality FROM readings")["quality"].to_pylist() == [
        None,
        None,
    ]

    table.append(
        pa.table(
            {
                "tick": [1, 1],
                "device": ["A", "B"],
                "reading": [3.0, 4.0],
                "quality": [0.9, 0.8],
                "reviewed": [True, False],
            }
        )
    )
    # Evolved tables fill omitted nullable payloads with null. All keys are required.
    table.append(pa.table({"tick": [2, 2], "device": ["A", "B"]}))
    return session.sql("SELECT * FROM readings ORDER BY tick, device")


if __name__ == "__main__":
    with TemporaryDirectory() as directory:
        print(run(table_root=Path(directory) / "readings"))
