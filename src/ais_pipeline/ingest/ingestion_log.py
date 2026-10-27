"""ops.ingestion_log: one row per Bronze ingest attempt (success, failed or skipped)."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path

from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from ais_pipeline.config import Settings
from ais_pipeline.delta_io import append, table_exists
from ais_pipeline.paths import table_path

STATUSES = ("success", "failed", "skipped")

LOG_SCHEMA = StructType(
    [
        StructField("source_date", DateType(), False),
        StructField("file_name", StringType(), True),
        StructField("file_sha256", StringType(), True),
        StructField("file_bytes", LongType(), True),
        StructField("row_count", LongType(), True),
        StructField("header_hash", StringType(), True),
        StructField("contract_version", StringType(), True),
        StructField("status", StringType(), False),
        StructField("message", StringType(), True),
        StructField("run_id", StringType(), False),
        StructField("logged_at", TimestampType(), False),
    ]
)


@dataclass(frozen=True)
class LogEntry:
    """One ingestion_log row. Fields unknown at failure time (e.g. row_count) stay None."""

    source_date: date
    status: str
    run_id: str
    file_name: str | None = None
    file_sha256: str | None = None
    file_bytes: int | None = None
    row_count: int | None = None
    header_hash: str | None = None
    contract_version: str | None = None
    message: str | None = None
    logged_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def __post_init__(self) -> None:
        if self.status not in STATUSES:
            raise ValueError(f"status must be one of {STATUSES}, got {self.status!r}")


def log_path(settings: Settings) -> Path:
    return table_path(settings, "ops", "ingestion_log")


def write_entry(spark: SparkSession, settings: Settings, entry: LogEntry) -> None:
    """Append one entry to ops.ingestion_log."""
    values = asdict(entry)
    row = Row(**{f.name: values[f.name] for f in LOG_SCHEMA.fields})
    append(spark.createDataFrame([row], LOG_SCHEMA), log_path(settings))


def latest_success(spark: SparkSession, settings: Settings, day: date) -> Row | None:
    """Return the most recent `success` entry for a source date, or None if there is none."""
    path = log_path(settings)
    if not table_exists(spark, path):
        return None
    return (
        spark.read.format("delta")
        .load(str(path))
        .where(F.col("source_date") == F.lit(day.isoformat()).cast("date"))
        .where(F.col("status") == "success")
        .orderBy(F.col("logged_at").desc())
        .first()
    )
