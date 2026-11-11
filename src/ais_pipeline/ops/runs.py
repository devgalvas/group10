"""Pipeline run ids, the ops.pipeline_runs log and the GitHub step summary."""

from __future__ import annotations

import logging
import os
import subprocess
import time
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from ais_pipeline.config import Settings
from ais_pipeline.delta_io import append
from ais_pipeline.paths import table_path

logger = logging.getLogger(__name__)

STAGES = ("download", "bronze", "silver", "gold", "run")
STATUSES = ("success", "failed", "skipped", "stale")

RUNS_SCHEMA = StructType(
    [
        StructField("run_id", StringType(), False),
        StructField("env", StringType(), False),
        StructField("git_sha", StringType(), True),
        StructField("stage", StringType(), False),
        StructField("source_date", DateType(), True),
        StructField("started_at", TimestampType(), False),
        StructField("ended_at", TimestampType(), False),
        StructField("status", StringType(), False),
        StructField("rows_in", LongType(), True),
        StructField("rows_out", LongType(), True),
        StructField("rows_rejected", LongType(), True),
        StructField("message", StringType(), True),
    ]
)


def new_run_id() -> str:
    """Return a sortable, unique run id such as 20261023T141500Z-1a2b3c4d."""
    return f"{datetime.now(UTC):%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:8]}"


def git_sha() -> str:
    """The commit being run: $GITHUB_SHA in Actions, else `git rev-parse`, else 'unknown'."""
    if sha := os.environ.get("GITHUB_SHA"):
        return sha[:12]
    try:
        out = subprocess.run(
            ["git", "rev-parse", "--short=12", "HEAD"],
            capture_output=True,
            text=True,
            check=True,
            cwd=Path(__file__).resolve().parent,
        )
        return out.stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return "unknown"


def runs_path(settings: Settings) -> Path:
    return table_path(settings, "ops", "pipeline_runs")


@dataclass
class StageRun:
    """Mutable record a stage fills in while it runs; written when the stage ends."""

    run_id: str
    stage: str
    source_date: date | None
    status: str = "success"
    rows_in: int | None = None
    rows_out: int | None = None
    rows_rejected: int | None = None
    message: str | None = None
    started_at: datetime = field(default_factory=lambda: datetime.now(UTC).replace(tzinfo=None))
    seconds: float = 0.0


# Every finished stage of this process, for the step summary.
_FINISHED: list[StageRun] = []


@contextmanager
def run_logger(
    spark: SparkSession, settings: Settings, run_id: str, stage: str, source_date: date | None
) -> Iterator[StageRun]:
    """Log one stage to ops.pipeline_runs: `failed` with the error if the block raises.

    The block can set rows_* and message, or status to 'skipped' / 'stale'.
    """
    if stage not in STAGES:
        raise ValueError(f"stage must be one of {STAGES}, got {stage!r}")
    rec = StageRun(run_id, stage, source_date)
    started = time.monotonic()
    try:
        yield rec
    except Exception as exc:
        rec.status = "failed"
        rec.message = f"{type(exc).__name__}: {exc}"[:2000]
        raise
    finally:
        rec.seconds = time.monotonic() - started
        _write(spark, settings, rec)
        _FINISHED.append(rec)


def _write(spark: SparkSession, settings: Settings, rec: StageRun) -> None:
    if rec.status not in STATUSES:
        raise ValueError(f"status must be one of {STATUSES}, got {rec.status!r}")
    row = (
        rec.run_id,
        settings.env,
        git_sha(),
        rec.stage,
        rec.source_date,
        rec.started_at,
        datetime.now(UTC).replace(tzinfo=None),
        rec.status,
        rec.rows_in,
        rec.rows_out,
        rec.rows_rejected,
        rec.message,
    )
    try:
        append(spark.createDataFrame([row], RUNS_SCHEMA), runs_path(settings))
    except Exception:
        # Never let the log hide the stage's own error.
        logger.exception("could not write pipeline_runs for %s %s", rec.stage, rec.source_date)


def summary_markdown(records: list[StageRun]) -> str:
    """A Markdown table of stage runs, for $GITHUB_STEP_SUMMARY."""

    def n(v: int | None) -> str:
        return "" if v is None else f"{v:,}"

    lines = [
        "| date | stage | status | rows in | rows out | rejected | seconds | message |",
        "|---|---|---|---:|---:|---:|---:|---|",
    ]
    for r in records:
        msg = (r.message or "").replace("|", "/")[:120]
        lines.append(
            f"| {r.source_date or ''} | {r.stage} | {r.status} | {n(r.rows_in)} | "
            f"{n(r.rows_out)} | {n(r.rows_rejected)} | {r.seconds:.1f} | {msg} |"
        )
    return "\n".join(lines)


def write_step_summary(title: str) -> None:
    """Append this process's stage table to the GitHub step summary, if running in Actions."""
    target = os.environ.get("GITHUB_STEP_SUMMARY")
    if not target or not _FINISHED:
        return
    with open(target, "a", encoding="utf-8") as fh:
        fh.write(f"### {title}\n\n{summary_markdown(_FINISHED)}\n\n")
