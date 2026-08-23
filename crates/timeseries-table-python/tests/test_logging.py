import subprocess
import sys
import textwrap


def _run_isolated(code: str) -> None:
    result = subprocess.run(
        [sys.executable, "-c", textwrap.dedent(code)],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert result.stderr == ""


def test_import_preserves_root_logging_policy():
    _run_isolated(
        """
        import logging

        root = logging.getLogger()
        original_level = root.level
        original_handlers = tuple(root.handlers)

        import timeseries_table_format  # noqa: F401

        assert root.level == original_level
        assert tuple(root.handlers) == original_handlers
        """
    )


def test_native_operation_uses_python_logging_namespace():
    _run_isolated(
        """
        import logging
        import tempfile
        from pathlib import Path

        records = []

        class Capture(logging.Handler):
            def emit(self, record):
                records.append(record)

        logger = logging.getLogger("timeseries_table_format")
        logger.setLevel(logging.DEBUG)
        logger.addHandler(Capture())
        logger.propagate = False

        import timeseries_table_format as ttf

        with tempfile.TemporaryDirectory() as directory:
            ttf.TimeSeriesTable.create(
                table_root=str(Path(directory) / "table"),
                index_column="ts",
                index_type="timestamp",
                index_granularity="1h",
            )

        created = next(
            record
            for record in records
            if "Created time-series table" in record.getMessage()
        )
        assert created.name == "timeseries_table_format.table"
        assert created.levelno == logging.INFO
        assert "starting_version=0" in created.getMessage()
        assert "committed_version=1" in created.getMessage()
        assert 'outcome="succeeded"' in created.getMessage()
        assert any("table.create" in record.getMessage() for record in records)
        assert all(
            record.name == "timeseries_table_format"
            or record.name.startswith("timeseries_table_format.")
            for record in records
        )
        """
    )


def test_native_logging_cache_can_be_refreshed_after_level_changes():
    _run_isolated(
        """
        import logging
        import tempfile
        from pathlib import Path

        records = []

        class Capture(logging.Handler):
            def emit(self, record):
                records.append(record)

        logger = logging.getLogger("timeseries_table_format")
        logger.setLevel(logging.WARNING)
        logger.addHandler(Capture())
        logger.propagate = False

        import timeseries_table_format as ttf

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            ttf.TimeSeriesTable.create(
                table_root=str(root / "quiet"),
                index_column="ts",
                index_type="timestamp",
                index_granularity="1h",
            )
            assert records == []

            logger.setLevel(logging.INFO)
            ttf.TimeSeriesTable.create(
                table_root=str(root / "still-quiet"),
                index_column="ts",
                index_type="timestamp",
                index_granularity="1h",
            )
            assert records == []

            ttf.refresh_logging_cache()
            ttf.TimeSeriesTable.create(
                table_root=str(root / "visible"),
                index_column="ts",
                index_type="timestamp",
                index_granularity="1h",
            )

        assert any(
            record.levelno == logging.INFO
            and "Created time-series table" in record.getMessage()
            for record in records
        )
        """
    )


def test_reload_does_not_duplicate_native_records():
    _run_isolated(
        """
        import importlib
        import logging
        import tempfile
        from pathlib import Path

        records = []

        class Capture(logging.Handler):
            def emit(self, record):
                records.append(record)

        logger = logging.getLogger("timeseries_table_format")
        logger.setLevel(logging.INFO)
        logger.addHandler(Capture())
        logger.propagate = False

        import timeseries_table_format as ttf

        importlib.reload(ttf._native)
        ttf = importlib.reload(ttf)

        with tempfile.TemporaryDirectory() as directory:
            ttf.TimeSeriesTable.create(
                table_root=str(Path(directory) / "table"),
                index_column="ts",
                index_type="timestamp",
                index_granularity="1h",
            )

        created = [
            record
            for record in records
            if "Created time-series table" in record.getMessage()
        ]
        assert len(created) == 1
        """
    )


def test_operation_exception_is_not_duplicated_as_an_error_record():
    _run_isolated(
        """
        import logging
        import tempfile
        from pathlib import Path

        records = []

        class Capture(logging.Handler):
            def emit(self, record):
                records.append(record)

        logger = logging.getLogger("timeseries_table_format")
        logger.setLevel(logging.DEBUG)
        logger.addHandler(Capture())
        logger.propagate = False

        import timeseries_table_format as ttf

        with tempfile.TemporaryDirectory() as directory:
            missing = Path(directory) / "missing"
            try:
                ttf.TimeSeriesTable.open(str(missing))
            except ttf.TimeseriesTableError as error:
                error_message = str(error)
            else:
                raise AssertionError("opening a missing table unexpectedly succeeded")

        assert not any(record.levelno >= logging.ERROR for record in records)
        assert all(error_message not in record.getMessage() for record in records)
        """
    )


def test_logging_handler_failure_does_not_fail_committed_operation():
    _run_isolated(
        """
        import logging
        import tempfile
        from pathlib import Path

        class BrokenHandler(logging.Handler):
            def emit(self, record):
                raise RuntimeError("log sink failed")

        logger = logging.getLogger("timeseries_table_format")
        logger.setLevel(logging.INFO)
        logger.addHandler(BrokenHandler())
        logger.propagate = False

        import timeseries_table_format as ttf

        with tempfile.TemporaryDirectory() as directory:
            table = ttf.TimeSeriesTable.create(
                table_root=str(Path(directory) / "table"),
                index_column="ts",
                index_type="int64",
                index_granularity=10,
            )
            assert table.version() == 1
        """
    )


def test_native_records_exclude_sensitive_operation_inputs():
    _run_isolated(
        """
        import logging
        import tempfile
        from pathlib import Path

        import pyarrow as pa
        import pyarrow.parquet as pq

        records = []

        class Capture(logging.Handler):
            def emit(self, record):
                records.append(record)

        logger = logging.getLogger("timeseries_table_format")
        logger.setLevel(logging.DEBUG)
        logger.addHandler(Capture())
        logger.propagate = False

        import timeseries_table_format as ttf

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            table_root = root / "table"
            segment = root / "segment.parquet"
            table = ttf.TimeSeriesTable.create(
                table_root=str(table_root),
                index_column="ts",
                index_type="int64",
                index_granularity=10,
                entity_columns=["private_entity_key_348"],
            )
            pq.write_table(
                pa.table(
                    {
                        "ts": pa.array([1], type=pa.int64()),
                        "private_entity_key_348": ["private_entity_value_348"],
                        "private_schema_field_348": [17],
                    }
                ),
                segment,
            )
            parquet = pq.ParquetFile(segment)
            table.append(
                pa.RecordBatchReader.from_batches(
                    parquet.schema_arrow,
                    parquet.iter_batches(),
                )
            )

            session = ttf.Session()
            session.register_tstable("private_table_348", str(table_root))
            result = session.sql(
                "SELECT private_schema_field_348 AS sql_marker_348 "
                "FROM private_table_348 "
                "WHERE private_entity_key_348 != $bound",
                params={"bound": "private_bound_value_348"},
            )
            assert result.num_rows == 1

        messages = "\\n".join(record.getMessage() for record in records)
        assert "sql_marker_348" not in messages
        assert "private_bound_value_348" not in messages
        assert "private_entity_value_348" not in messages
        assert "private_schema_field_348" not in messages
        """
    )


def test_coverage_snapshot_recovery_emits_one_actionable_warning():
    _run_isolated(
        """
        import logging
        import tempfile
        from pathlib import Path

        import pyarrow as pa
        import pyarrow.parquet as pq

        records = []

        class Capture(logging.Handler):
            def emit(self, record):
                records.append(record)

        logger = logging.getLogger("timeseries_table_format")
        logger.setLevel(logging.DEBUG)
        logger.addHandler(Capture())
        logger.propagate = False

        import timeseries_table_format as ttf

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            table_root = root / "table"
            table = ttf.TimeSeriesTable.create(
                table_root=str(table_root),
                index_column="ts",
                index_type="int64",
                index_granularity=10,
            )

            first = root / "first.parquet"
            pq.write_table(pa.table({"ts": pa.array([1], type=pa.int64())}), first)
            first_parquet = pq.ParquetFile(first)
            assert table.append(
                pa.RecordBatchReader.from_batches(
                    first_parquet.schema_arrow,
                    first_parquet.iter_batches(),
                )
            ) == 2

            snapshots = list((table_root / "_coverage" / "table").glob("*.roar"))
            assert len(snapshots) == 1
            snapshot_path = snapshots[0]
            managed_path = snapshot_path.relative_to(table_root).as_posix()
            snapshot_path.unlink()
            records.clear()

            second = root / "second.parquet"
            pq.write_table(pa.table({"ts": pa.array([11], type=pa.int64())}), second)
            second_parquet = pq.ParquetFile(second)
            assert table.append(
                pa.RecordBatchReader.from_batches(
                    second_parquet.schema_arrow,
                    second_parquet.iter_batches(),
                )
            ) == 3

        warnings = [
            record
            for record in records
            if record.levelno == logging.WARNING
            and "attempting read-only recovery" in record.getMessage()
        ]
        assert len(warnings) == 1
        warning = warnings[0]
        message = warning.getMessage()
        assert warning.name == "timeseries_table_format.table.coverage"
        assert 'coverage_mode="global"' in message
        assert "snapshot_version=2" in message
        assert f"coverage_path={managed_path}" in message
        assert 'recovery_source="segment_sidecars"' in message
        assert str(table_root) not in message
        """
    )


def test_enabled_logging_does_not_deadlock_public_native_operations():
    _run_isolated(
        """
        import logging
        import tempfile
        from pathlib import Path

        import pyarrow as pa
        import pyarrow.parquet as pq

        records = []

        class Capture(logging.Handler):
            def emit(self, record):
                records.append(record)

        logger = logging.getLogger("timeseries_table_format")
        logger.setLevel(logging.DEBUG)
        logger.addHandler(Capture())
        logger.propagate = False

        import timeseries_table_format as ttf

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            table_root = root / "table"
            segment = root / "mixed.parquet"
            table = ttf.TimeSeriesTable.create(
                table_root=str(table_root),
                index_column="tick",
                index_type="uint64",
                index_granularity=10,
                entity_columns=["device_id"],
            )
            pq.write_table(
                pa.table(
                    {
                        "tick": pa.array([0, 10], type=pa.uint64()),
                        "device_id": ["A", "B"],
                        "reading": [1, 2],
                    }
                ),
                segment,
            )
            parquet = pq.ParquetFile(segment)
            assert table.append(
                pa.RecordBatchReader.from_batches(
                    parquet.schema_arrow,
                    parquet.iter_batches(),
                )
            ) == 2
            assert table.optimize().no_op is False

            session = ttf.Session()
            session.register_tstable("readings", str(table_root))
            assert session.sql("SELECT count(*) AS count FROM readings")[
                "count"
            ].to_pylist() == [2]

            reader = session.sql_reader("SELECT reading FROM readings")
            try:
                assert sorted(reader.read_all()["reading"].to_pylist()) == [1, 2]
            finally:
                reader.close()

        messages = "\\n".join(record.getMessage() for record in records)
        for event_name in (
            "table.create",
            "table.append",
            "table.optimize",
            "table.open",
            "table.scan.plan",
        ):
            assert event_name in messages
        """
    )
