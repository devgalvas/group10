from datetime import UTC, date, datetime

import pytest

from ais_pipeline.ingest.ingestion_log import (
    LOG_SCHEMA,
    LogEntry,
    latest_success,
    log_path,
    write_entry,
)

pytestmark = pytest.mark.integration

DAY = date(2026, 9, 1)


def test_write_entry_creates_table_with_schema(spark, settings):
    write_entry(spark, settings, LogEntry(DAY, "failed", "r1", message="boom"))
    df = spark.read.format("delta").load(str(log_path(settings)))
    assert [f.name for f in df.schema.fields] == LOG_SCHEMA.fieldNames()
    row = df.first()
    assert row["status"] == "failed"
    assert row["row_count"] is None
    assert row["message"] == "boom"


def test_latest_success_picks_newest_success(spark, settings):
    assert latest_success(spark, settings, DAY) is None
    for sha, status, ts in [
        ("aaa", "success", datetime(2026, 10, 1, 10, tzinfo=UTC)),
        ("bbb", "success", datetime(2026, 10, 2, 10, tzinfo=UTC)),
        ("ccc", "failed", datetime(2026, 10, 3, 10, tzinfo=UTC)),
    ]:
        write_entry(spark, settings, LogEntry(DAY, status, "r", file_sha256=sha, logged_at=ts))
    write_entry(spark, settings, LogEntry(date(2026, 9, 2), "success", "r", file_sha256="ddd"))

    assert latest_success(spark, settings, DAY)["file_sha256"] == "bbb"


def test_unknown_status_is_rejected():
    with pytest.raises(ValueError, match="status"):
        LogEntry(DAY, "done", "r1")
