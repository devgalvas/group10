"""The `flags` array column: codes for rows that are kept but carry a quality note."""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from ais_pipeline.silver.standardize import FLAGS_COL

FLAG_CODES = ["SOG_SENTINEL", "COG_SENTINEL", "HEADING_SENTINEL", "TS_CONFLICT", "POSITION_JUMP"]


def add_flag(df: DataFrame, condition: Column, code: str) -> DataFrame:
    """Append code to `flags` where condition is true (null counts as false); no duplicates."""
    if code not in FLAG_CODES:
        raise ValueError(f"unknown flag code {code!r}")
    flagged = F.array_union(F.col(FLAGS_COL), F.array(F.lit(code)))
    return df.withColumn(
        FLAGS_COL, F.when(F.coalesce(condition, F.lit(False)), flagged).otherwise(F.col(FLAGS_COL))
    )


def has_flag(code: str) -> Column:
    """True where `flags` contains code."""
    return F.array_contains(F.col(FLAGS_COL), code)
