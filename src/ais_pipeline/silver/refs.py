"""Load the small reference CSVs in ref/ as Spark DataFrames, with explicit schemas."""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from ais_pipeline.geo import resolve_ref

NAV_STATUS_SCHEMA = StructType(
    [
        StructField("source_text", StringType(), False),
        StructField("nav_status", StringType(), False),
    ]
)


def read_ref_csv(spark: SparkSession, path: str | Path, schema: StructType) -> DataFrame:
    """Read a reference CSV (header row, comma) with an explicit schema; fail on bad rows."""
    return spark.read.csv(str(resolve_ref(path)), schema=schema, header=True, mode="FAILFAST")


def load_nav_status(spark: SparkSession, path: str | Path) -> DataFrame:
    """ref/nav_status.csv: DMA navigational-status text -> controlled label."""
    return read_ref_csv(spark, path, NAV_STATUS_SCHEMA)


SHIP_TYPE_SCHEMA = StructType(
    [
        StructField("ship_type", StringType(), False),
        StructField("ship_type_group", StringType(), False),
    ]
)
SHIP_TYPE_GROUPS = (
    "cargo",
    "tanker",
    "passenger",
    "fishing",
    "service",
    "pleasure",
    "other",
    "unknown",
)

PORT_SCHEMA = StructType(
    [
        StructField("port_id", StringType(), False),
        StructField("name", StringType(), False),
        StructField("lat", DoubleType(), False),
        StructField("lon", DoubleType(), False),
        StructField("port_radius_km", DoubleType(), False),
        StructField("anchorage_radius_km", DoubleType(), False),
    ]
)


def load_ship_type_groups(spark: SparkSession, path: str | Path) -> DataFrame:
    """ref/ship_type_groups.csv: DMA ship-type text -> one of SHIP_TYPE_GROUPS."""
    return read_ref_csv(spark, path, SHIP_TYPE_SCHEMA)


def with_ship_type_group(df: DataFrame, groups: DataFrame) -> DataFrame:
    """Add ship_type_group from the ship_type text (broadcast lookup, no shuffle).

    A missing ship type is `unknown`; a type not in the reference is `other`.
    """
    lookup = F.broadcast(groups.withColumnRenamed("ship_type", "_st_key"))
    joined = df.join(lookup, df["ship_type"] == lookup["_st_key"], "left").drop("_st_key")
    group = (
        F.when(F.col("ship_type").isNull(), F.lit("unknown"))
        .when(F.col("ship_type_group").isNull(), F.lit("other"))
        .otherwise(F.col("ship_type_group"))
    )
    return joined.withColumn("ship_type_group", group)
