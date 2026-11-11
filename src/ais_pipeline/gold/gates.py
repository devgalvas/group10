"""Quality gates: a Silver date is published to Gold only if its metrics pass every check."""

from __future__ import annotations

import statistics
from dataclasses import dataclass, field
from datetime import date

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from ais_pipeline.config import QualitySettings, Settings
from ais_pipeline.delta_io import table_exists
from ais_pipeline.ops.dq import dq_path


@dataclass(frozen=True)
class GateResult:
    source_date: date
    passed: bool
    reasons: list[str] = field(default_factory=list)


def check(
    day: date,
    rows: float | None,
    hours_present: float | None,
    reject_rate: float | None,
    other_days_rows: list[float],
    quality: QualitySettings,
) -> GateResult:
    """Apply the three gates to one day's numbers (pure, no Spark).

    - completeness: at least min_hours_present distinct hours in the cleaned output;
    - quality: in-scope reject rate at most max_reject_rate;
    - volume: at least min_rows_vs_median x the median input rows of the other days
      (skipped while there is no other day to compare with).
    """
    reasons = []
    if rows is None or hours_present is None or reject_rate is None:
        return GateResult(day, False, ["no dq_metrics for this date (silver did not finish)"])
    if hours_present < quality.min_hours_present:
        reasons.append(f"hours_present {hours_present:.0f} < {quality.min_hours_present}")
    if reject_rate > quality.max_reject_rate:
        reasons.append(f"reject_rate {reject_rate:.4f} > {quality.max_reject_rate}")
    if other_days_rows:
        floor = quality.min_rows_vs_median * statistics.median(other_days_rows)
        if rows < floor:
            reasons.append(f"rows {rows:.0f} < {floor:.0f} ({quality.min_rows_vs_median} x median)")
    return GateResult(day, not reasons, reasons)


def day_metrics(spark: SparkSession, settings: Settings) -> dict[date, dict[str, float]]:
    """Latest run's gate metrics for every date in ops.dq_metrics (one small result)."""
    path = dq_path(settings)
    if not table_exists(spark, path):
        return {}
    df = spark.read.format("delta").load(str(path))
    wanted = {
        ("bronze_in", "rows"): "rows",
        ("silver_out", "hours_present"): "hours_present",
        ("silver_out", "reject_rate"): "reject_rate",
    }
    cond = F.lit(False)
    for cp, metric in wanted:
        cond = cond | ((F.col("checkpoint") == cp) & (F.col("metric") == metric))
    # The latest value of each (date, checkpoint, metric).
    w = Window.partitionBy("source_date", "checkpoint", "metric").orderBy(F.desc("logged_at"))
    latest = (
        df.where(cond)
        .withColumn("_rn", F.row_number().over(w))
        .where("_rn = 1")
        .select("source_date", "checkpoint", "metric", "value")
        .collect()  # 3 rows per processed date: bounded by the window length
    )
    out: dict[date, dict[str, float]] = {}
    for r in latest:
        out.setdefault(r["source_date"], {})[wanted[(r["checkpoint"], r["metric"])]] = r["value"]
    return out


def evaluate(spark: SparkSession, settings: Settings, days: list[date]) -> dict[date, GateResult]:
    """Gate every listed day against ops.dq_metrics."""
    metrics = day_metrics(spark, settings)
    results = {}
    for day in days:
        m = metrics.get(day, {})
        others = [v["rows"] for d, v in metrics.items() if d != day and v.get("rows")]
        results[day] = check(
            day,
            m.get("rows"),
            m.get("hours_present"),
            m.get("reject_rate"),
            others,
            settings.quality,
        )
    return results
