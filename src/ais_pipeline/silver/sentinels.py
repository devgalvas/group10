"""AIS 'not available' codes in SOG, COG and heading: set to null and flag the row.

The position is still valid, so the row is kept (ROADMAP A.4 flag codes).
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from ais_pipeline.silver.flags import add_flag
from ais_pipeline.silver.standardize import PLACEHOLDERS, RAW_COL

SOG_NA_KN = 102.3  # 1023 in 0.1 kn units: "not available"
COG_NA_DEG = 360.0  # 3600 in 0.1 deg units: "not available"
HEADING_NA_DEG = 511.0
# The only raw fields read after validation: clean() keeps just these in RAW_COL.
SENTINEL_RAW_COLS = ("sog", "cog", "heading")


def _unparsed(raw_name: str, typed: str) -> Column:
    """The raw text held something (not a placeholder) but did not cast to a number."""
    raw = F.trim(F.col(f"{RAW_COL}.{raw_name}"))
    return raw.isNotNull() & ~raw.isin(PLACEHOLDERS) & F.col(typed).isNull()


def sog_sentinel() -> Column:
    return (F.col("sog_kn") >= SOG_NA_KN) | _unparsed("sog", "sog_kn")


def cog_sentinel() -> Column:
    return (F.col("cog_deg") >= COG_NA_DEG) | _unparsed("cog", "cog_deg")


def heading_sentinel() -> Column:
    h = F.col("heading_deg")
    return (h == HEADING_NA_DEG) | (h > 359.0) | _unparsed("heading", "heading_deg")


SENTINELS = [
    ("sog_kn", sog_sentinel, "SOG_SENTINEL"),
    ("cog_deg", cog_sentinel, "COG_SENTINEL"),
    ("heading_deg", heading_sentinel, "HEADING_SENTINEL"),
]


def apply_sentinels(df: DataFrame) -> DataFrame:
    """Flag each sentinel, then null the value. The flag is computed before the value changes."""
    for col, rule, code in SENTINELS:
        cond = F.coalesce(rule(), F.lit(False))
        df = add_flag(df, cond, code).withColumn(col, F.when(~cond, F.col(col)))
    return df
