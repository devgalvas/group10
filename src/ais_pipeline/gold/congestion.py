"""gold.port_congestion_hourly, written in Spark SQL (congestion.sql) over temp views."""

from __future__ import annotations

from importlib import resources

from pyspark.sql import DataFrame, SparkSession


def congestion_sql() -> str:
    return resources.files("ais_pipeline.gold").joinpath("congestion.sql").read_text()


def hourly_congestion(
    spark: SparkSession, positions: DataFrame, calls: DataFrame, anchor_max_sog_kn: float
) -> DataFrame:
    """Vessels in port, at anchor (SOG below the threshold), arrivals and departures per hour.

    The DataFrames are registered as temp views so the aggregation reads as plain SQL.
    """
    positions.createOrReplaceTempView("_gold_positions")
    calls.createOrReplaceTempView("_gold_calls")
    return spark.sql(
        congestion_sql().format(
            positions="_gold_positions",
            calls="_gold_calls",
            anchor_max_sog_kn=float(anchor_max_sog_kn),
        )
    )
