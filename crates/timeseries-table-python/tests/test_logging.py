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
                bucket="1h",
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
                bucket="1h",
            )
            assert records == []

            logger.setLevel(logging.INFO)
            ttf.TimeSeriesTable.create(
                table_root=str(root / "still-quiet"),
                index_column="ts",
                index_type="timestamp",
                bucket="1h",
            )
            assert records == []

            ttf.refresh_logging_cache()
            ttf.TimeSeriesTable.create(
                table_root=str(root / "visible"),
                index_column="ts",
                index_type="timestamp",
                bucket="1h",
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
                bucket="1h",
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
