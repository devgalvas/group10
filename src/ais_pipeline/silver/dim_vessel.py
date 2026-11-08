"""silver.dim_vessel: vessel static attributes as a Type 2 slowly changing dimension.

A vessel's name, call sign, type or dimensions can change. Instead of overwriting them, each
change opens a new version (valid_from / valid_to / is_current), and every position joins to
the version that was valid at its timestamp.
"""

from __future__ import annotations

import logging
from datetime import datetime

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from ais_pipeline.delta_io import table_exists

logger = logging.getLogger(__name__)

ATTR_COLS = ["imo", "callsign", "name", "ship_type", "length_m", "width_m"]
OPEN_END = datetime(9999, 12, 31)

DIM_SCHEMA = StructType(
    [
        StructField("vessel_sk", LongType(), False),
        StructField("mmsi", LongType(), False),
        StructField("imo", LongType(), True),
        StructField("callsign", StringType(), True),
        StructField("name", StringType(), True),
        StructField("ship_type", StringType(), True),
        StructField("ship_type_group", StringType(), True),
        StructField("length_m", DoubleType(), True),
        StructField("width_m", DoubleType(), True),
        StructField("attr_hash", StringType(), False),
        StructField("valid_from", TimestampType(), False),
        StructField("valid_to", TimestampType(), False),
        StructField("is_current", BooleanType(), False),
    ]
)


def attr_hash() -> Column:
    """SHA-256 of the static attributes; a null and an empty value hash differently."""
    parts = [F.coalesce(F.col(c).cast("string"), F.lit("\u0000")) for c in ATTR_COLS]
    return F.sha2(F.concat_ws("\u001f", *parts), 256)


def daily_versions(clean_df: DataFrame, min_reports: int) -> DataFrame:
    """Static attribute combinations seen in one day's positions, one row per combination.

    A combination needs at least min_reports rows, so a single garbled message does not open
    a version. Columns: mmsi, ATTR_COLS, attr_hash, first_seen, n_reports.
    """
    any_attr = F.lit(False)
    for c in ATTR_COLS:
        any_attr = any_attr | F.col(c).isNotNull()
    return (
        clean_df.where(any_attr)
        .groupBy("mmsi", *ATTR_COLS)
        .agg(F.min("ts_utc").alias("first_seen"), F.count(F.lit(1)).alias("n_reports"))
        .where(F.col("n_reports") >= min_reports)
        .withColumn("attr_hash", attr_hash())
    )


def plan_changes(staged: DataFrame, dim: DataFrame) -> DataFrame:
    """Rows for the SCD2 MERGE: `insert` new versions, `close` the current ones they replace.

    - Staged versions not later than the vessel's current version are ignored: that makes a
      rerun of an already-processed day a no-op (days must be processed in date order).
    - A staged version equal to the version right before it (same attr_hash) is no change.
    - A vessel's very first version starts at midnight of its first day, so the positions
      before its first static report still find a version.
    """
    cur = dim.where("is_current").select(
        "mmsi",
        "attr_hash",
        "valid_from",
        F.col("vessel_sk").alias("_cur_sk"),
        F.lit(True).alias("_is_cur"),
    )
    cur_from = cur.select("mmsi", F.col("valid_from").alias("_cur_from"))
    st = (
        staged.join(cur_from, "mmsi", "left")
        .where(F.col("_cur_from").isNull() | (F.col("first_seen") > F.col("_cur_from")))
        .select(
            "mmsi",
            *[c for c in ATTR_COLS],
            "attr_hash",
            F.col("first_seen").alias("valid_from"),
            F.lit(False).alias("_is_cur"),
            F.col("_cur_from").isNotNull().alias("_has_history"),
        )
    )

    w = Window.partitionBy("mmsi").orderBy("valid_from")
    timeline = st.unionByName(cur, allowMissingColumns=True)
    changed = (
        timeline.withColumn("_prev_hash", F.lag("attr_hash").over(w))
        .withColumn("_is_first", F.row_number().over(w) == 1)
        .where(~F.col("_is_cur"))
        .where(F.col("_prev_hash").isNull() | (F.col("_prev_hash") != F.col("attr_hash")))
    )
    first_ever = F.col("_is_first") & ~F.col("_has_history")
    new = changed.withColumn(
        "valid_from",
        F.when(first_ever, F.date_trunc("day", "valid_from")).otherwise(F.col("valid_from")),
    )
    wn = Window.partitionBy("mmsi").orderBy("valid_from")
    nxt = F.lead("valid_from").over(wn)
    new = (
        new.withColumn("valid_to", F.coalesce(nxt, F.lit(OPEN_END)))
        .withColumn("is_current", nxt.isNull())
        .withColumn("vessel_sk", F.xxhash64("mmsi", "valid_from"))
    )

    inserts = new.select(
        F.lit("insert").alias("_action"),
        F.col("vessel_sk").alias("_merge_key"),
        "vessel_sk",
        "mmsi",
        *ATTR_COLS,
        "attr_hash",
        "valid_from",
        "valid_to",
        "is_current",
    )
    first_new = new.groupBy("mmsi").agg(F.min("valid_from").alias("_close_at"))
    closes = cur.join(first_new, "mmsi").select(
        F.lit("close").alias("_action"),
        F.col("_cur_sk").alias("_merge_key"),
        F.col("_close_at").alias("valid_to"),
    )
    return inserts.unionByName(closes, allowMissingColumns=True)


MERGE_SQL = """
MERGE INTO delta.`{path}` AS t
USING {source} AS s
ON t.vessel_sk = s._merge_key
WHEN MATCHED AND s._action = 'close' THEN
  UPDATE SET t.valid_to = s.valid_to, t.is_current = false
WHEN NOT MATCHED AND s._action = 'insert' THEN
  INSERT ({cols}) VALUES ({values})
"""


def merge_scd2(
    spark: SparkSession,
    path: str,
    staged: DataFrame,
    ship_type_groups: DataFrame,
) -> dict[str, int]:
    """Apply one day's staged versions to dim_vessel with a single, atomic Delta MERGE.

    Closing the old version and inserting the new one happen in the same commit, so a reader
    never sees a vessel with zero or two current versions.
    """
    from ais_pipeline.silver.refs import with_ship_type_group

    if not table_exists(spark, path):
        spark.createDataFrame([], DIM_SCHEMA).write.format("delta").save(path)
    dim = spark.read.format("delta").load(path)

    changes = with_ship_type_group(plan_changes(staged, dim), ship_type_groups).cache()
    counts = {
        r["_action"]: r["count"] for r in changes.groupBy("_action").count().collect()
    }  # at most 2 rows
    view = "_dim_vessel_changes"
    changes.createOrReplaceTempView(view)
    cols = [f.name for f in DIM_SCHEMA.fields]
    spark.sql(
        MERGE_SQL.format(
            path=path,
            source=view,
            cols=", ".join(cols),
            values=", ".join(f"s.{c}" for c in cols),
        )
    )
    changes.unpersist()
    result = {"inserted": counts.get("insert", 0), "closed": counts.get("close", 0)}
    logger.info("dim_vessel merge: %(inserted)d versions inserted, %(closed)d closed", result)
    return result


def join_vessel(positions: DataFrame, dim: DataFrame, broadcast: bool = True) -> DataFrame:
    """As-of join: each position gets the vessel version valid at its ts_utc.

    Equi-join on mmsi plus the range valid_from <= ts_utc < valid_to. Adds vessel_sk and
    ship_type_group (`unknown` when the vessel never sent static data).
    """
    d = dim.select(
        "vessel_sk",
        F.col("mmsi").alias("_d_mmsi"),
        F.col("valid_from").alias("_d_from"),
        F.col("valid_to").alias("_d_to"),
        F.col("ship_type_group").alias("_d_group"),
    )
    if broadcast:
        d = F.broadcast(d)
    cond = (
        (positions["mmsi"] == d["_d_mmsi"])
        & (positions["ts_utc"] >= d["_d_from"])
        & (positions["ts_utc"] < d["_d_to"])
    )
    out = positions.join(d, cond, "left")
    return out.withColumn("ship_type_group", F.coalesce("_d_group", F.lit("unknown"))).drop(
        "_d_mmsi", "_d_from", "_d_to", "_d_group"
    )
