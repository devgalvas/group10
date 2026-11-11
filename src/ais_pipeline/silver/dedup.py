"""Deduplication: exact copies are dropped, (mmsi, ts_utc) conflicts resolved deterministically."""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from ais_pipeline.silver.flags import add_flag
from ais_pipeline.silver.standardize import ROW_HASH_COL

# Group size of each (mmsi, ts_utc) before conflict resolution. Kept on the surviving row so
# the number of dropped conflict rows can be counted without another pass (sum of size - 1).
TS_GROUP_COL = "_ts_group_rows"

# Typed business columns compared when choosing the most complete row of a conflict.
CONFLICT_VALUE_COLS = [
    "lat",
    "lon",
    "sog_kn",
    "cog_deg",
    "heading_deg",
    "rot",
    "nav_status_text",
    "imo",
    "callsign",
    "name",
    "ship_type",
    "cargo_type",
    "width_m",
    "length_m",
    "draught_m",
    "destination",
    "eta_utc",
]


def drop_exact(df: DataFrame) -> DataFrame:
    """Keep one row per _row_hash: the same message received by several base stations.

    _row_hash covers all 26 source columns verbatim, so only true copies collapse.
    """
    return df.dropDuplicates([ROW_HASH_COL])


def resolve_conflicts(df: DataFrame) -> DataFrame:
    """Keep one row per (mmsi, ts_utc) and flag TS_CONFLICT where there was a choice.

    The kept row is the one with the most non-null values; ties go to the smallest
    _row_hash, so reruns always keep the same row. Adds TS_GROUP_COL (rows in the group).
    """
    non_null = sum(F.col(c).isNotNull().cast("int") for c in CONFLICT_VALUE_COLS)
    key = Window.partitionBy("mmsi", "ts_utc")
    ranked = (
        df.withColumn("_non_null", non_null)
        .withColumn(TS_GROUP_COL, F.count(F.lit(1)).over(key))
        .withColumn(
            "_rank", F.row_number().over(key.orderBy(F.desc("_non_null"), F.asc(ROW_HASH_COL)))
        )
    )
    kept = ranked.where(F.col("_rank") == 1).drop("_rank", "_non_null")
    return add_flag(kept, F.col(TS_GROUP_COL) > 1, "TS_CONFLICT")
