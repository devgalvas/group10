"""Delta Lake write helpers. Pipeline outputs are only written idempotently."""

from __future__ import annotations

import logging
from datetime import date
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)


def table_exists(spark: SparkSession, path: Path | str) -> bool:
    """True if path holds a Delta table."""
    from delta.tables import DeltaTable

    return DeltaTable.isDeltaTable(spark, str(path))


def write_partition(df: DataFrame, path: Path | str, partition_col: str, value: date | str) -> None:
    """Overwrite exactly one partition (partition_col = value) of a Delta table.

    `replaceWhere` makes the write idempotent: rerunning a day replaces that day's rows and
    leaves every other partition untouched. Delta rejects the write if any row of df falls
    outside the partition, which guards against writing the wrong day.
    """
    predicate = f"{partition_col} = '{value}'"
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", predicate)
        .partitionBy(partition_col)
        .save(str(path))
    )
    logger.info("wrote %s partition %s", path, predicate)


def append(df: DataFrame, path: Path | str) -> None:
    """Append rows to a Delta table, creating it on first use. Only for ops log tables."""
    df.write.format("delta").mode("append").save(str(path))
    logger.info("appended to %s", path)
