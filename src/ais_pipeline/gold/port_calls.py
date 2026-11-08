"""gold.port_calls: one row per vessel visit to a port, by gaps and islands."""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

CALL_COLS = [
    "call_id",
    "mmsi",
    "vessel_sk",
    "port_id",
    "ship_type_group",
    "arrival_ts",
    "departure_ts",
    "dwell_h",
    "n_positions",
    "arrival_date",
]


def detect_port_calls(silver: DataFrame, gap_h: float, min_call_min: float) -> DataFrame:
    """Group consecutive in-port positions of a vessel into calls.

    Filtered to zone = 'port' first (a few % of Silver), then, per mmsi in time order, a new
    call ("island") starts when the port changes or the vessel was not seen in port for more
    than gap_h hours. A running sum of those starts numbers the calls. Calls shorter than
    min_call_min minutes are dropped (a pass-by, not a visit).
    """
    p = silver.where(F.col("zone") == "port").select(
        "mmsi", "ts_utc", "port_id", "vessel_sk", "ship_type_group"
    )
    w = Window.partitionBy("mmsi").orderBy("ts_utc")
    prev_port, prev_ts = F.lag("port_id").over(w), F.lag("ts_utc").over(w)
    gap_s = F.col("ts_utc").cast("long") - prev_ts.cast("long")
    new_call = prev_port.isNull() | (F.col("port_id") != prev_port) | (gap_s > gap_h * 3600)
    seq = F.sum(new_call.cast("int")).over(w.rowsBetween(Window.unboundedPreceding, 0))

    calls = (
        p.withColumn("call_seq", seq)
        .groupBy("mmsi", "port_id", "call_seq")
        .agg(
            F.min("ts_utc").alias("arrival_ts"),
            F.max("ts_utc").alias("departure_ts"),
            F.count(F.lit(1)).alias("n_positions"),
            # The vessel version and type group at arrival.
            F.min_by("vessel_sk", "ts_utc").alias("vessel_sk"),
            F.min_by("ship_type_group", "ts_utc").alias("ship_type_group"),
        )
        .withColumn(
            "dwell_h",
            (F.col("departure_ts").cast("long") - F.col("arrival_ts").cast("long")) / 3600.0,
        )
        .where(F.col("dwell_h") * 60 >= min_call_min)
    )
    call_id = F.sha2(F.concat_ws("|", "mmsi", "port_id", F.col("arrival_ts").cast("string")), 256)
    return (
        calls.withColumn("call_id", call_id)
        .withColumn("arrival_date", F.to_date("arrival_ts"))
        .select(*CALL_COLS)
    )
