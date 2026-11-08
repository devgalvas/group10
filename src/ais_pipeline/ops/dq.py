"""ops.dq_metrics: data-quality measurements at each checkpoint, in long format.

Each checkpoint is measured in ONE aggregation pass: every metric is a conditional sum or a
distinct count inside a single `agg`, so Spark scans the data once instead of once per metric.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from ais_pipeline.config import Settings
from ais_pipeline.delta_io import append, table_exists
from ais_pipeline.paths import table_path

CHECKPOINTS = ("bronze_in", "silver_out", "gold_out")

DQ_SCHEMA = StructType(
    [
        StructField("run_id", StringType(), False),
        StructField("source_date", DateType(), False),
        StructField("checkpoint", StringType(), False),
        StructField("metric", StringType(), False),
        StructField("value", DoubleType(), True),
        StructField("logged_at", TimestampType(), False),
    ]
)


@dataclass(frozen=True)
class Metric:
    checkpoint: str
    metric: str
    value: float


def count_if(cond: Column) -> Column:
    """Number of rows where cond is true (null counts as false)."""
    return F.sum(F.when(cond, 1).otherwise(0))


def profile(df: DataFrame, checkpoint: str, exprs: dict[str, Column]) -> list[Metric]:
    """Evaluate every aggregate expression in one pass and return them as Metrics."""
    if checkpoint not in CHECKPOINTS:
        raise ValueError(f"checkpoint must be one of {CHECKPOINTS}, got {checkpoint!r}")
    row = df.agg(*[e.alias(name) for name, e in exprs.items()]).first()
    return [
        Metric(checkpoint, name, None if row[name] is None else float(row[name])) for name in exprs
    ]


def as_dict(metrics: list[Metric], checkpoint: str) -> dict[str, float]:
    return {m.metric: m.value for m in metrics if m.checkpoint == checkpoint}


def dq_path(settings: Settings):
    return table_path(settings, "ops", "dq_metrics")


def write_metrics(
    spark: SparkSession, settings: Settings, run_id: str, day: date, metrics: list[Metric]
) -> None:
    """Append one run's metrics to ops.dq_metrics (an append-only log table)."""
    now = datetime.now(UTC).replace(tzinfo=None)
    rows = [(run_id, day, m.checkpoint, m.metric, m.value, now) for m in metrics]
    append(spark.createDataFrame(rows, DQ_SCHEMA), dq_path(settings))


def latest_metrics(spark: SparkSession, settings: Settings, day: date) -> DataFrame | None:
    """The metrics of the most recent run that measured day, or None if there is none."""
    path = dq_path(settings)
    if not table_exists(spark, path):
        return None
    df = spark.read.format("delta").load(str(path)).where(F.col("source_date") == F.lit(day))
    last = df.orderBy(F.desc("logged_at")).select("run_id").first()
    return None if last is None else df.where(F.col("run_id") == last["run_id"])


def report_table(metrics: DataFrame) -> str:
    """A Markdown table: one row per metric, one column per checkpoint (bounded, small)."""
    rows = metrics.select("checkpoint", "metric", "value").collect()
    cps = [c for c in CHECKPOINTS if any(r["checkpoint"] == c for r in rows)]
    values: dict[str, dict[str, float]] = {}
    for r in rows:
        values.setdefault(r["metric"], {})[r["checkpoint"]] = r["value"]

    def fmt(v: float | None) -> str:
        if v is None:
            return ""
        return f"{v:,.0f}" if float(v).is_integer() else f"{v:,.4f}"

    lines = [f"| metric | {' | '.join(cps)} |", "|---|" + "---:|" * len(cps)]
    for metric in sorted(values, key=_metric_order):
        cells = " | ".join(fmt(values[metric].get(c)) for c in cps)
        lines.append(f"| {metric} | {cells} |")
    return "\n".join(lines)


def _metric_order(name: str) -> tuple[int, str]:
    prefixes = ["rows", "null_", "exact_", "out_of_scope", "reject_", "conflict", "flag_", "acc"]
    rank = next((i for i, p in enumerate(prefixes) if name.startswith(p)), len(prefixes))
    return rank, name
