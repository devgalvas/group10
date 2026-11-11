"""Gold stage: gate the Silver dates, then rebuild port calls and hourly congestion."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from ais_pipeline.config import Settings
from ais_pipeline.delta_io import overwrite, table_exists, write_partitions
from ais_pipeline.gold.congestion import hourly_congestion
from ais_pipeline.gold.gates import GateResult, evaluate
from ais_pipeline.gold.port_calls import detect_port_calls
from ais_pipeline.paths import table_path

logger = logging.getLogger(__name__)


@dataclass
class GoldResult:
    published: list[date] = field(default_factory=list)
    stale: dict[date, GateResult] = field(default_factory=dict)
    port_calls: int = 0
    congestion_rows: int = 0


def silver_dates(spark: SparkSession, settings: Settings) -> list[date]:
    """Dates present in silver.vessel_position (one per processed day: a small list)."""
    path = table_path(settings, "silver", "vessel_position")
    if not table_exists(spark, path):
        return []
    df = spark.read.format("delta").load(str(path))
    return sorted(r["date"] for r in df.select("date").distinct().collect())


def build_gold(spark: SparkSession, settings: Settings) -> GoldResult:
    """Publish Gold from every Silver date that passes its quality gates.

    - gold.port_calls is fully recomputed from the gated Silver dates (after the zone filter
      it is small) and replaced in one Delta commit;
    - gold.port_congestion_hourly is rewritten only for the gated dates (replaceWhere), so a
      date that fails its gate keeps its previous Gold rows and is reported as stale.
    """
    result = GoldResult()
    days = silver_dates(spark, settings)
    gates = evaluate(spark, settings, days)
    result.published = [d for d in days if gates[d].passed]
    result.stale = {d: g for d, g in gates.items() if not g.passed}
    for d, g in result.stale.items():
        logger.warning("gold: %s is stale: %s", d, "; ".join(g.reasons))
    if not result.published:
        logger.warning("gold: no Silver date passed its gates; Gold left unchanged")
        return result

    f = settings.features
    published = [F.lit(d.isoformat()).cast("date") for d in result.published]
    positions = (
        spark.read.format("delta")
        .load(str(table_path(settings, "silver", "vessel_position")))
        .where(F.col("date").isin(published))
        .where(F.col("zone").isin("port", "anchorage"))
    ).cache()

    calls = detect_port_calls(positions, f.port_call_gap_h, f.min_port_call_min).cache()
    overwrite(calls, table_path(settings, "gold", "port_calls"))

    congestion = hourly_congestion(spark, positions, calls, f.anchor_max_sog_kn).where(
        F.col("date").isin(published)
    )
    write_partitions(
        congestion, table_path(settings, "gold", "port_congestion_hourly"), "date", result.published
    )

    result.port_calls = calls.count()
    result.congestion_rows = (
        spark.read.format("delta")
        .load(str(table_path(settings, "gold", "port_congestion_hourly")))
        .where(F.col("date").isin(published))
        .count()
    )
    calls.unpersist()
    positions.unpersist()
    logger.info(
        "gold: %d dates published, %d stale, %d port calls, %d congestion rows",
        len(result.published),
        len(result.stale),
        result.port_calls,
        result.congestion_rows,
    )
    return result
