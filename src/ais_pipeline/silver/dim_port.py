"""silver.dim_port: port and anchorage areas from ref/ports.csv (reference data)."""

from __future__ import annotations

import logging
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ais_pipeline.config import Settings
from ais_pipeline.paths import table_path
from ais_pipeline.silver.refs import PORT_SCHEMA, read_ref_csv

logger = logging.getLogger(__name__)


class RefDataError(ValueError):
    """Raised when a reference file has values that cannot be right."""


def load_dim_port(spark: SparkSession, path: str | Path) -> DataFrame:
    """Read ref/ports.csv with an explicit schema and validate it."""
    df = read_ref_csv(spark, path, PORT_SCHEMA)
    validate_ports(df)
    return df


def validate_ports(df: DataFrame) -> None:
    """Coordinates in range, radii positive, anchorage wider than the port, unique ids."""
    bad = df.where(
        ~F.col("lat").between(-90, 90)
        | ~F.col("lon").between(-180, 180)
        | (F.col("port_radius_km") <= 0)
        | (F.col("anchorage_radius_km") <= F.col("port_radius_km"))
    )
    problems = [r["port_id"] for r in bad.select("port_id").collect()]  # 3 rows: bounded
    if problems:
        raise RefDataError(f"invalid port rows: {problems}")
    dupes = df.groupBy("port_id").count().where("count > 1").select("port_id").collect()
    if dupes:
        raise RefDataError(f"duplicate port_id: {[r['port_id'] for r in dupes]}")


def write_dim_port(spark: SparkSession, settings: Settings) -> DataFrame:
    """Overwrite silver.dim_port from the reference CSV and return it (small, cached)."""
    df = load_dim_port(spark, settings.refs.ports)
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(
        str(table_path(settings, "silver", "dim_port"))
    )
    logger.info("dim_port written: %d ports", df.count())
    return df.cache()
