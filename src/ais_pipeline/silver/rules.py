"""Validation rules: one Column expression per reject code, combined in priority order.

The same rules exist as Spark SQL in rules.sql; a test checks both give identical results.
"""

from __future__ import annotations

from collections.abc import Callable
from importlib import resources

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from ais_pipeline.ingest.bronze import CORRUPT_COL
from ais_pipeline.silver.standardize import RAW_COL

REASON_COL = "reject_reason"

# Valid MMSI for a ship: exactly 9 digits, first digit 2-7 (ITU maritime identification digits).
MMSI_SHIP_RE = r"^[2-7][0-9]{8}$"
LAT_SENTINEL = 91.0
LON_SENTINEL = 181.0


def malformed_row() -> Column:
    """The CSV line had the wrong number of fields (kept verbatim in _corrupt_record)."""
    return F.col(f"{RAW_COL}.{CORRUPT_COL}").isNotNull()


def out_of_scope_mobile() -> Column:
    """Not a Class A or Class B transponder: base stations, AtoN, SAR aircraft, ..."""
    return F.col("mobile_class").isNull()


def bad_timestamp() -> Column:
    """The timestamp did not parse as dd/MM/yyyy HH:mm:ss."""
    return F.col("ts_utc").isNull()


def ts_outside_file_day() -> Column:
    """The timestamp is on another day than the file it came from."""
    return F.to_date("ts_utc") != F.col("_source_date")


def position_sentinel() -> Column:
    """DMA 'position not available': latitude 91 or longitude 181."""
    return (F.col("lat") == LAT_SENTINEL) | (F.col("lon") == LON_SENTINEL)


def bad_position() -> Column:
    """Missing, unparseable or out-of-range coordinates."""
    lat, lon = F.col("lat"), F.col("lon")
    return lat.isNull() | lon.isNull() | ~lat.between(-90.0, 90.0) | ~lon.between(-180.0, 180.0)


def bad_mmsi() -> Column:
    """The verbatim MMSI is not 9 digits starting with 2-7."""
    return ~F.coalesce(F.trim(F.col(f"{RAW_COL}.mmsi")).rlike(MMSI_SHIP_RE), F.lit(False))


# Priority order (ROADMAP A.4): when several rules fail, the first code wins. The rules are
# functions, not Column objects, because a Column can only be built with a running Spark.
REJECT_RULES: list[tuple[str, Callable[[], Column]]] = [
    ("MALFORMED_ROW", malformed_row),
    ("OUT_OF_SCOPE_MOBILE", out_of_scope_mobile),
    ("BAD_TIMESTAMP", bad_timestamp),
    ("TS_OUTSIDE_FILE_DAY", ts_outside_file_day),
    ("POSITION_SENTINEL", position_sentinel),
    ("BAD_POSITION", bad_position),
    ("BAD_MMSI", bad_mmsi),
]
REJECT_CODES: list[str] = [code for code, _ in REJECT_RULES]


def reject_reason_col() -> Column:
    """The first failing rule's code, or null when the row passes every rule."""
    expr = None
    for code, rule in REJECT_RULES:
        # coalesce: a rule on a null input (e.g. no timestamp) must not hide later rules.
        cond = F.coalesce(rule(), F.lit(False))
        expr = F.when(cond, code) if expr is None else expr.when(cond, code)
    return expr


def with_reject_reason(df: DataFrame) -> DataFrame:
    """Add reject_reason (null for valid rows)."""
    return df.withColumn(REASON_COL, reject_reason_col())


def split_valid_rejects(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Return (valid rows without reject_reason, rejected rows with reject_reason)."""
    tagged = with_reject_reason(df)
    valid = tagged.where(F.col(REASON_COL).isNull()).drop(REASON_COL)
    rejects = tagged.where(F.col(REASON_COL).isNotNull())
    return valid, rejects


def rules_sql() -> str:
    """The text of rules.sql: the same rules as a CASE WHEN over the view {view}."""
    return resources.files("ais_pipeline.silver").joinpath("rules.sql").read_text()


def with_reject_reason_sql(spark: SparkSession, df: DataFrame) -> DataFrame:
    """Spark SQL version of with_reject_reason, for documentation and cross-checking."""
    view = "_silver_standardized"
    df.createOrReplaceTempView(view)
    return spark.sql(rules_sql().format(view=view))
