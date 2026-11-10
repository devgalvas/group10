"""Silver stage orchestration: Bronze partition -> cleaned positions, rejects and DQ metrics.

The pure steps live in standardize/rules/dedup/sentinels; this module wires them together and
does the I/O.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import date

from pyspark import StorageLevel
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from ais_pipeline.config import Settings
from ais_pipeline.delta_io import write_partition
from ais_pipeline.ops.dq import Metric, as_dict, count_if, profile, write_metrics
from ais_pipeline.paths import table_path
from ais_pipeline.silver.dedup import TS_GROUP_COL, drop_exact, resolve_conflicts
from ais_pipeline.silver.flags import FLAG_CODES, has_flag
from ais_pipeline.silver.refs import load_nav_status
from ais_pipeline.silver.rules import REASON_COL, REJECT_CODES, with_reject_reason
from ais_pipeline.silver.sentinels import SENTINEL_RAW_COLS, apply_sentinels
from ais_pipeline.silver.standardize import RAW_COL, ROW_HASH_COL, SOURCE_COLS, standardize

logger = logging.getLogger(__name__)

ESSENTIAL_COLS = ["mmsi", "ts_utc", "lat", "lon"]
OPTIONAL_COLS = ["sog_kn", "cog_deg", "heading_deg", "nav_status"]


# --- pure steps --------------------------------------------------------------------------------


def tag(bronze_df: DataFrame, nav_lookup: DataFrame) -> DataFrame:
    """Standardise Bronze rows and add reject_reason (null = valid)."""
    return with_reject_reason(standardize(bronze_df, nav_lookup))


def rejects_frame(tagged: DataFrame, silver_run_id: str) -> DataFrame:
    """Rejected rows exactly as they came from Bronze, with their reason and lineage."""
    return tagged.where(F.col(REASON_COL).isNotNull()).select(
        *[F.col(f"{RAW_COL}.{c}").alias(c) for c in SOURCE_COLS],
        F.col(f"{RAW_COL}._corrupt_record").alias("_corrupt_record"),
        "_source_file",
        "_source_date",
        "_bronze_run_id",
        REASON_COL,
        F.lit(silver_run_id).alias("_silver_run_id"),
    )


def clean(tagged: DataFrame) -> DataFrame:
    """Valid rows -> exact dedup -> (mmsi, ts_utc) conflicts -> sentinels.

    The raw struct is cut to the fields the sentinels read: carrying all 26 raw strings
    through the dedup sort ran a full prod day out of heap.
    """
    valid = (
        tagged.where(F.col(REASON_COL).isNull())
        .drop(REASON_COL)
        .withColumn(
            RAW_COL, F.struct(*[F.col(f"{RAW_COL}.{c}").alias(c) for c in SENTINEL_RAW_COLS])
        )
    )
    return apply_sentinels(resolve_conflicts(drop_exact(valid)))


def _hours_present() -> Column:
    return F.countDistinct(F.hour("ts_utc"))


def bronze_in_exprs() -> dict[str, Column]:
    """Metrics of the input, measured on the tagged rows (one pass)."""
    reason = F.col(REASON_COL)
    exprs = {"rows": F.count(F.lit(1))}
    exprs |= {f"null_{c}": count_if(F.col(c).isNull()) for c in ESSENTIAL_COLS}
    exprs["exact_duplicates"] = F.count(F.lit(1)) - F.countDistinct(ROW_HASH_COL)
    exprs["out_of_scope"] = count_if(reason == "OUT_OF_SCOPE_MOBILE")
    exprs |= {f"reject_{code}": count_if(reason == code) for code in REJECT_CODES}
    exprs["rejected"] = count_if(reason.isNotNull())
    # Rows left after drop_exact = distinct hashes among valid rows (measured, not derived).
    exprs["valid_distinct"] = F.countDistinct(F.when(reason.isNull(), F.col(ROW_HASH_COL)))
    exprs["valid"] = count_if(reason.isNull())
    exprs["distinct_mmsi"] = F.countDistinct("mmsi")
    exprs["hours_present"] = _hours_present()
    return exprs


def silver_out_exprs() -> dict[str, Column]:
    """Metrics of the cleaned output (one pass)."""
    exprs = {"rows": F.count(F.lit(1))}
    exprs |= {f"null_{c}": count_if(F.col(c).isNull()) for c in ESSENTIAL_COLS + OPTIONAL_COLS}
    exprs["conflict_dropped"] = F.sum(F.col(TS_GROUP_COL) - 1)
    # POSITION_JUMP is set later by the movement features; it is measured in final_exprs.
    core_flags = [c for c in FLAG_CODES if c != "POSITION_JUMP"]
    exprs |= {f"flag_{code}": count_if(has_flag(code)) for code in core_flags}
    exprs["distinct_mmsi"] = F.countDistinct("mmsi")
    exprs["hours_present"] = _hours_present()
    return exprs


def derived_metrics(bronze_in: dict[str, float], silver_out: dict[str, float]) -> list[Metric]:
    """Counts that link the two checkpoints, plus the conservation check."""
    exact_removed = bronze_in["valid"] - bronze_in["valid_distinct"]
    accepted = silver_out["rows"]
    accounted = bronze_in["rejected"] + exact_removed + (silver_out["conflict_dropped"] or 0)
    in_scope = bronze_in["rows"] - bronze_in["out_of_scope"]
    rejected_in_scope = bronze_in["rejected"] - bronze_in["out_of_scope"]
    return [
        Metric("silver_out", "exact_duplicates_removed", exact_removed),
        Metric("silver_out", "accepted", accepted),
        Metric("silver_out", "unaccounted", bronze_in["rows"] - accounted - accepted),
        Metric("silver_out", "reject_rate", rejected_in_scope / in_scope if in_scope else 0.0),
    ]


# --- I/O ---------------------------------------------------------------------------------------


def _rdd_cross_check(bronze_df: DataFrame, df_count: float) -> list[Metric]:
    from ais_pipeline.silver.rdd_check import exact_duplicates_rdd

    started = time.monotonic()
    rdd_count = exact_duplicates_rdd(bronze_df)
    seconds = time.monotonic() - started
    if rdd_count != df_count:
        logger.warning("exact duplicates differ: rdd=%d dataframe=%d", rdd_count, df_count)
    logger.info("rdd cross-check: %d exact duplicates in %.1f s", rdd_count, seconds)
    return [
        Metric("bronze_in", "exact_duplicates_rdd", float(rdd_count)),
        Metric("bronze_in", "exact_duplicates_rdd_s", seconds),
    ]


@dataclass
class CoreResult:
    """Output of the cleaning part of the Silver stage for one source date."""

    source_date: date
    clean: DataFrame  # persisted; the caller writes it and then calls unpersist()
    metrics: list[Metric]

    def metric(self, checkpoint: str, name: str) -> float:
        return as_dict(self.metrics, checkpoint)[name]


def read_bronze_day(spark: SparkSession, settings: Settings, day: date) -> DataFrame:
    path = table_path(settings, "bronze", "ais_raw")
    return (
        spark.read.format("delta")
        .load(str(path))
        .where(F.col("_source_date") == F.lit(day.isoformat()).cast("date"))
    )


def run_silver_core(
    spark: SparkSession,
    settings: Settings,
    day: date,
    run_id: str,
    bronze_df: DataFrame | None = None,
    rdd_check: bool = False,
) -> CoreResult:
    """Clean one source date: bronze_in metrics, rejects written, clean rows, silver_out metrics.

    The tagged rows are read by three jobs (metrics, rejects, clean), so they are persisted
    once instead of re-reading and re-parsing Bronze three times. They are persisted on disk
    only: a full day (18.7 M wide rows) cached with MEMORY_AND_DISK ran out of a 5 GB heap
    while the cache blocks were built. With rdd_check, the exact
    duplicates are recounted with RDDs (AIS-22) and logged as exact_duplicates_rdd.
    """
    bronze_df = bronze_df if bronze_df is not None else read_bronze_day(spark, settings, day)
    nav_lookup = load_nav_status(spark, settings.refs.nav_status)

    tagged = tag(bronze_df, nav_lookup).persist(StorageLevel.DISK_ONLY)
    metrics = profile(tagged, "bronze_in", bronze_in_exprs())
    bronze_in = as_dict(metrics, "bronze_in")
    if bronze_in["rows"] == 0:
        tagged.unpersist()
        raise ValueError(f"no Bronze rows for {day}: run the bronze stage first")
    if rdd_check:
        metrics += _rdd_cross_check(bronze_df, bronze_in["exact_duplicates"])

    write_partition(
        rejects_frame(tagged, run_id),
        table_path(settings, "silver", "rejects"),
        "_source_date",
        day,
    )

    cleaned = clean(tagged).persist(StorageLevel.DISK_ONLY)
    metrics += profile(cleaned, "silver_out", silver_out_exprs())
    tagged.unpersist()
    metrics += derived_metrics(bronze_in, as_dict(metrics, "silver_out"))
    write_metrics(spark, settings, run_id, day, metrics)

    out = as_dict(metrics, "silver_out")
    logger.info(
        "silver core %s: %d in, %d rejected, %d exact dups, %d conflicts, %d accepted",
        day,
        bronze_in["rows"],
        bronze_in["rejected"],
        out["exact_duplicates_removed"],
        out["conflict_dropped"] or 0,
        out["accepted"],
    )
    if out["unaccounted"] != 0:
        logger.warning(
            "row conservation broken for %s: %s rows unaccounted", day, out["unaccounted"]
        )
    return CoreResult(day, cleaned, metrics)


# --- full Silver stage (AIS-28) ----------------------------------------------------------------

POSITION_COLS = [
    "mmsi",
    "ts_utc",
    "date",
    "hour",
    "mobile_class",
    "lat",
    "lon",
    "sog_kn",
    "cog_deg",
    "heading_deg",
    "rot",
    "nav_status",
    "vessel_sk",
    "ship_type_group",
    "zone",
    "port_id",
    "gap_s",
    "step_km",
    "implied_speed_kn",
    "flags",
    "_source_file",
    "_bronze_run_id",
    "_silver_run_id",
    "_processed_at",
]


def enrich(
    cleaned: DataFrame,
    dim_vessel: DataFrame,
    dim_port: DataFrame,
    jump_speed_kn: float,
    silver_run_id: str,
    context: DataFrame | None = None,
) -> DataFrame:
    """Cleaned rows -> movement features -> zones -> vessel version -> calendar and lineage."""
    from ais_pipeline.silver.dim_vessel import join_vessel
    from ais_pipeline.silver.features import add_movement
    from ais_pipeline.silver.zones import assign

    df = add_movement(cleaned, jump_speed_kn, context)
    df = assign(df, dim_port)
    df = join_vessel(df, dim_vessel)
    derived = {
        "date": F.col("_source_date"),
        "hour": F.hour("ts_utc"),
        "_silver_run_id": F.lit(silver_run_id),
        "_processed_at": F.current_timestamp(),
    }
    return df.select(*[derived.get(c, F.col(c)).alias(c) for c in POSITION_COLS])


def final_exprs() -> dict[str, Column]:
    """Metrics of the written vessel_position partition (one pass)."""
    return {
        "rows_written": F.count(F.lit(1)),
        "flag_POSITION_JUMP": count_if(has_flag("POSITION_JUMP")),
        "zone_port": count_if(F.col("zone") == "port"),
        "zone_anchorage": count_if(F.col("zone") == "anchorage"),
        "zone_at_sea": count_if(F.col("zone") == "at_sea"),
        "null_vessel_sk": count_if(F.col("vessel_sk").isNull()),
        "null_gap_s": count_if(F.col("gap_s").isNull()),
    }


@dataclass(frozen=True)
class SilverResult:
    source_date: date
    rows_in: int
    rejected: int
    rows_written: int
    versions_inserted: int
    seconds: float


def _previous_day(spark: SparkSession, settings: Settings, day: date) -> DataFrame | None:
    from datetime import timedelta

    from ais_pipeline.delta_io import table_exists

    path = table_path(settings, "silver", "vessel_position")
    if not table_exists(spark, path):
        return None
    prev = (
        spark.read.format("delta")
        .load(str(path))
        .where(F.col("date") == F.lit((day - timedelta(days=1)).isoformat()).cast("date"))
    )
    return prev.select("mmsi", "ts_utc", "lat", "lon")


def build_silver(
    spark: SparkSession,
    settings: Settings,
    day: date,
    run_id: str,
    bronze_df: DataFrame | None = None,
    rdd_check: bool = False,
) -> SilverResult:
    """Build every Silver table for one source date (dates must be processed in order).

    rejects and dq_metrics (run_silver_core) -> dim_port -> dim_vessel SCD2 merge ->
    vessel_position, written with replaceWhere on date so a rerun replaces the day.
    """
    from ais_pipeline.silver.dim_port import write_dim_port
    from ais_pipeline.silver.dim_vessel import daily_versions, merge_scd2
    from ais_pipeline.silver.features import last_positions
    from ais_pipeline.silver.refs import load_ship_type_groups

    started = time.monotonic()
    core = run_silver_core(spark, settings, day, run_id, bronze_df, rdd_check)
    try:
        dim_port = write_dim_port(spark, settings)
        dim_path = str(table_path(settings, "silver", "dim_vessel"))
        groups = load_ship_type_groups(spark, settings.refs.ship_types)
        staged = daily_versions(core.clean, settings.features.scd2_min_reports)
        scd = merge_scd2(spark, dim_path, staged, groups)
        dim_vessel = spark.read.format("delta").load(dim_path)

        prev = _previous_day(spark, settings, day)
        context = last_positions(prev) if prev is not None else None
        positions = enrich(
            core.clean, dim_vessel, dim_port, settings.features.jump_speed_kn, run_id, context
        )
        out_path = table_path(settings, "silver", "vessel_position")
        write_partition(positions, out_path, "date", day)
    finally:
        core.clean.unpersist()

    written = (
        spark.read.format("delta")
        .load(str(out_path))
        .where(F.col("date") == F.lit(day.isoformat()).cast("date"))
    )
    final = profile(written, "silver_out", final_exprs())
    final += [
        Metric("silver_out", "dim_vessel_inserted", float(scd["inserted"])),
        Metric("silver_out", "dim_vessel_closed", float(scd["closed"])),
        Metric("silver_out", "silver_s", time.monotonic() - started),
    ]
    write_metrics(spark, settings, run_id, day, final)

    stats = as_dict(final, "silver_out")
    result = SilverResult(
        source_date=day,
        rows_in=int(core.metric("bronze_in", "rows")),
        rejected=int(core.metric("bronze_in", "rejected")),
        rows_written=int(stats["rows_written"]),
        versions_inserted=scd["inserted"],
        seconds=stats["silver_s"],
    )
    logger.info(
        "silver %s: %d in, %d rejected, %d written, %d vessel versions, %.1f s",
        day,
        result.rows_in,
        result.rejected,
        result.rows_written,
        result.versions_inserted,
        result.seconds,
    )
    return result
