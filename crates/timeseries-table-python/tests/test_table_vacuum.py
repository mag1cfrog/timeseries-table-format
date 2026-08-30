import os
from datetime import datetime, timedelta, timezone, tzinfo
from zoneinfo import ZoneInfo

import pytest

import timeseries_table_format as ttf


class NoOffset(tzinfo):
    def utcoffset(self, dt):
        return None


def test_vacuum_defaults_to_dry_run_and_requires_apply_to_delete(tmp_path):
    assert issubclass(ttf.VacuumApplyError, ttf.StorageError)

    root = tmp_path / "table"
    table = ttf.TimeSeriesTable.create(
        table_root=str(root),
        index_column="tick",
        index_type="uint64",
        index_granularity=10,
    )
    orphan = (
        root
        / "data"
        / "_managed"
        / "append"
        / "00000000-0000-0000-0000-000000000001.parquet"
    )
    orphan.parent.mkdir(parents=True)
    orphan.write_bytes(b"incomplete")
    os.utime(orphan, (0, 0))
    current_before = (root / "_timeseries_log" / "CURRENT").read_bytes()
    older_than = datetime(2000, 1, 1, tzinfo=ZoneInfo("America/Phoenix"))

    with pytest.raises(TypeError, match="timezone-aware"):
        table.vacuum(datetime.now())
    with pytest.raises(TypeError, match="timezone-aware"):
        table.vacuum(datetime(2000, 1, 1, tzinfo=NoOffset()))
    with pytest.raises(ValueError, match="future"):
        table.vacuum(datetime.now(timezone.utc) + timedelta(hours=1))

    dry_run = table.vacuum(older_than)

    assert isinstance(dry_run, ttf.VacuumReport)
    assert dry_run.table_version == 1
    assert dry_run.older_than == older_than
    assert dry_run.older_than.tzinfo == timezone.utc
    assert dry_run.mode == "dry_run"
    assert dry_run.considered_files == 1
    assert dry_run.retained_files == 0
    assert dry_run.removable_files == 1
    assert dry_run.deleted_files == 0
    assert dry_run.already_absent_files == 0
    assert dry_run.considered_bytes == 10
    assert dry_run.retained_bytes == 0
    assert dry_run.removable_bytes == 10
    assert dry_run.deleted_bytes == 0
    assert dry_run.already_absent_bytes == 0
    assert len(dry_run.artifacts) == 1
    artifact = dry_run.artifacts[0]
    assert isinstance(artifact, ttf.VacuumArtifact)
    assert (
        artifact.path
        == "data/_managed/append/00000000-0000-0000-0000-000000000001.parquet"
    )
    assert artifact.size_bytes == 10
    assert artifact.disposition == "removable"
    assert artifact.reason == "invalid_or_unreadable_parquet"
    assert artifact.referenced_by_commit_version is None
    assert artifact.modified_at.tzinfo == timezone.utc
    assert orphan.exists()

    applied = table.vacuum(older_than, apply=True)

    assert applied.mode == "apply"
    assert applied.removable_files == 0
    assert applied.deleted_files == 1
    assert applied.already_absent_files == 0
    assert applied.removable_bytes == 0
    assert applied.deleted_bytes == 10
    assert applied.already_absent_bytes == 0
    assert applied.artifacts[0].disposition == "deleted"
    assert not orphan.exists()
    assert (root / "_timeseries_log" / "CURRENT").read_bytes() == current_before
    assert table.version() == 1
