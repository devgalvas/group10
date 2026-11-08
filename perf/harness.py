"""Measure one experiment variant: wall time, Spark stage metrics and the physical plan.

Each measured action runs in its own Spark job group. After it finishes, the driver's REST
API (the same data the Spark UI shows) is queried for that group's stages, and their input
bytes, shuffle bytes, spill and task-time quantiles are summed into one result row, appended
to ops.perf_results.
"""

from __future__ import annotations

import json
import logging
import statistics
import time
import urllib.request
from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    DoubleType,
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

PLANS_DIR = Path("docs/perf/plans")

PERF_SCHEMA = StructType(
    [
        StructField("experiment", StringType(), False),
        StructField("variant", StringType(), False),
        StructField("run_ts", TimestampType(), False),
        StructField("wall_s", DoubleType(), False),
        StructField("rows", LongType(), True),
        StructField("input_bytes", LongType(), True),
        StructField("shuffle_read_bytes", LongType(), True),
        StructField("shuffle_write_bytes", LongType(), True),
        StructField("spill_bytes", LongType(), True),
        StructField("max_task_s", DoubleType(), True),
        StructField("median_task_s", DoubleType(), True),
        StructField("plan_file", StringType(), True),
        StructField("notes", StringType(), True),
    ]
)


@dataclass
class PerfResult:
    experiment: str
    variant: str
    run_ts: datetime
    wall_s: float
    rows: int | None = None
    input_bytes: int | None = None
    shuffle_read_bytes: int | None = None
    shuffle_write_bytes: int | None = None
    spill_bytes: int | None = None
    max_task_s: float | None = None
    median_task_s: float | None = None
    plan_file: str | None = None
    notes: str | None = None


def _get(url: str) -> Any:
    with urllib.request.urlopen(url, timeout=30) as resp:  # noqa: S310 - local driver UI
        return json.load(resp)


def stage_metrics(spark: SparkSession, job_group: str) -> dict[str, float]:
    """Sum the metrics of every stage that ran under job_group (via the driver REST API)."""
    sc = spark.sparkContext
    base = f"{sc.uiWebUrl}/api/v1/applications/{sc.applicationId}"
    jobs = [j for j in _get(f"{base}/jobs") if j.get("jobGroup") == job_group]
    stage_ids = sorted({s for j in jobs for s in j["stageIds"]})
    totals = dict(input_bytes=0, shuffle_read=0, shuffle_write=0, spill=0)
    task_max, task_medians = 0.0, []
    for sid in stage_ids:
        for attempt in _get(f"{base}/stages/{sid}"):
            if attempt.get("status") != "COMPLETE":
                continue  # skipped stages (reused shuffle output) did no work
            totals["input_bytes"] += attempt.get("inputBytes", 0)
            totals["shuffle_read"] += attempt.get("shuffleReadBytes", 0)
            totals["shuffle_write"] += attempt.get("shuffleWriteBytes", 0)
            totals["spill"] += attempt.get("memoryBytesSpilled", 0) + attempt.get(
                "diskBytesSpilled", 0
            )
            summary = _get(
                f"{base}/stages/{sid}/{attempt['attemptId']}/taskSummary?quantiles=0.5,1.0"
            )
            run_ms = summary.get("executorRunTime", [0, 0])
            task_medians.append(run_ms[0] / 1000)
            task_max = max(task_max, run_ms[1] / 1000)
    totals["max_task_s"] = task_max
    # The slowest stage's median task is the most telling single number for skew.
    totals["median_task_s"] = max(task_medians) if task_medians else 0.0
    totals["stages"] = len(stage_ids)
    return totals


def save_plan(df: DataFrame, experiment: str, variant: str) -> str:
    """Write df.explain('formatted') to docs/perf/plans/<experiment>_<variant>.txt."""
    PLANS_DIR.mkdir(parents=True, exist_ok=True)
    path = PLANS_DIR / f"{experiment}_{variant}.txt"
    # The same JVM call DataFrame.explain() makes, but returning the text instead of printing.
    jvm = df.sparkSession.sparkContext._jvm
    path.write_text(jvm.PythonSQLUtils.explainString(df._jdf.queryExecution(), "formatted"))
    return str(path)


def measure(
    spark: SparkSession,
    experiment: str,
    variant: str,
    action: Callable[[], Any],
    plan_df: DataFrame | None = None,
    notes: str | None = None,
) -> PerfResult:
    """Run action once in its own job group and return its timing and stage metrics.

    action is a zero-argument callable that triggers the Spark work (count, write, ...).
    If it returns an int, that is recorded as `rows`.
    """
    group = f"{experiment}:{variant}:{time.time_ns()}"
    sc = spark.sparkContext
    sc.setJobGroup(group, f"{experiment} {variant}")
    started = time.monotonic()
    try:
        out = action()
    finally:
        wall = time.monotonic() - started
        sc.setLocalProperty("spark.jobGroup.id", None)
    m = stage_metrics(spark, group)
    result = PerfResult(
        experiment=experiment,
        variant=variant,
        run_ts=datetime.now(UTC).replace(tzinfo=None),
        wall_s=wall,
        rows=out if isinstance(out, int) else None,
        input_bytes=int(m["input_bytes"]),
        shuffle_read_bytes=int(m["shuffle_read"]),
        shuffle_write_bytes=int(m["shuffle_write"]),
        spill_bytes=int(m["spill"]),
        max_task_s=m["max_task_s"],
        median_task_s=m["median_task_s"],
        plan_file=save_plan(plan_df, experiment, variant) if plan_df is not None else None,
        notes=notes,
    )
    logger.info(
        "%s %s: %.1f s, input %.1f MB, shuffle %.1f MB, max task %.1f s",
        experiment,
        variant,
        wall,
        result.input_bytes / 1e6,
        (result.shuffle_read_bytes or 0) / 1e6,
        result.max_task_s,
    )
    return result


def measure_repeated(
    spark: SparkSession,
    experiment: str,
    variant: str,
    action: Callable[[], Any],
    plan_df: DataFrame | None = None,
    runs: int = 3,
    notes: str | None = None,
) -> list[PerfResult]:
    """Run a variant `runs` times (the plan is saved once); the report uses the median."""
    results = []
    for i in range(runs):
        results.append(
            measure(spark, experiment, variant, action, plan_df if i == 0 else None, notes)
        )
    walls = [r.wall_s for r in results]
    logger.info(
        "%s %s: median %.1f s over %d runs", experiment, variant, statistics.median(walls), runs
    )
    return results


def perf_path(settings: Settings) -> Path:
    return table_path(settings, "ops", "perf_results")


def save_results(spark: SparkSession, settings: Settings, results: list[PerfResult]) -> None:
    """Append results to ops.perf_results."""
    rows = [tuple(asdict(r)[f.name] for f in PERF_SCHEMA.fields) for r in results]
    append(spark.createDataFrame(rows, PERF_SCHEMA), perf_path(settings))
