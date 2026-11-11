"""Movement features per vessel: gap, step distance, implied speed and the jump flag.

Haversine is built from native Spark functions (no Python UDF), so it runs inside the JVM
and Catalyst can optimise it.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from ais_pipeline.geo import EARTH_RADIUS_KM
from ais_pipeline.silver.flags import add_flag

KM_PER_S_TO_KN = 3600.0 / 1.852  # km/s -> knots (1 kn = 1.852 km/h)
CONTEXT_COL = "_context"


def haversine_km(lat1: Column, lon1: Column, lat2: Column, lon2: Column) -> Column:
    """Great-circle distance in km between two points given in degrees."""
    p1, p2 = F.radians(lat1), F.radians(lat2)
    dp, dl = p2 - p1, F.radians(lon2) - F.radians(lon1)
    a = F.sin(dp / 2) ** 2 + F.cos(p1) * F.cos(p2) * F.sin(dl / 2) ** 2
    return F.lit(2 * EARTH_RADIUS_KM) * F.asin(F.sqrt(a))


def last_positions(previous_day: DataFrame) -> DataFrame:
    """The last (mmsi, ts_utc, lat, lon) of each vessel on the previous Silver day.

    max over a struct compares its fields in order, so max(struct(ts_utc, lat, lon)) is the
    latest fix; this is one aggregation, with no sort of the whole day.
    """
    last = previous_day.groupBy("mmsi").agg(F.max(F.struct("ts_utc", "lat", "lon")).alias("_l"))
    return last.select("mmsi", "_l.ts_utc", "_l.lat", "_l.lon")


def add_movement(
    df: DataFrame, jump_speed_kn: float, context: DataFrame | None = None
) -> DataFrame:
    """Add gap_s, step_km and implied_speed_kn, and flag POSITION_JUMP.

    Each row is compared with the previous fix of the same mmsi (window ordered by ts_utc).
    `context` holds the last fix per mmsi of the previous day (see last_positions): those rows
    are added before the window and dropped after it, so the first fix after midnight still
    gets a gap and a step instead of nulls.
    """
    df = df.withColumn(CONTEXT_COL, F.lit(False))
    if context is not None:
        ctx = context.select("mmsi", "ts_utc", "lat", "lon").withColumn(CONTEXT_COL, F.lit(True))
        df = df.unionByName(ctx, allowMissingColumns=True)

    w = Window.partitionBy("mmsi").orderBy("ts_utc")
    prev_ts, prev_lat, prev_lon = (F.lag(c).over(w) for c in ("ts_utc", "lat", "lon"))
    gap_s = F.col("ts_utc").cast("long") - prev_ts.cast("long")
    df = (
        df.withColumn("gap_s", gap_s)
        .withColumn("step_km", haversine_km(prev_lat, prev_lon, F.col("lat"), F.col("lon")))
        .withColumn(
            "implied_speed_kn",
            F.when(F.col("gap_s") > 0, F.col("step_km") / F.col("gap_s") * KM_PER_S_TO_KN),
        )
        .where(~F.col(CONTEXT_COL))
        .drop(CONTEXT_COL)
    )
    return add_flag(df, F.col("implied_speed_kn") > jump_speed_kn, "POSITION_JUMP")
