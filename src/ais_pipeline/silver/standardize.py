"""Silver standardisation: Bronze strings to typed, renamed columns with explicit casts.

Every function here is a pure DataFrame -> DataFrame transformation (no I/O), so it can be
unit-tested on hand-built DataFrames.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType

from ais_pipeline.ingest.bronze import CORRUPT_COL, load_contract

_CONTRACT = load_contract()
SOURCE_COLS: list[str] = [c["bronze_name"] for c in _CONTRACT["columns"]]
PLACEHOLDERS: list[str] = _CONTRACT["null_placeholders"]
TS_FORMAT: str = _CONTRACT["timestamp_format"]

RAW_COL = "_raw"
ROW_HASH_COL = "_row_hash"
FLAGS_COL = "flags"
MOBILE_CLASSES = {"Class A": "A", "Class B": "B"}

# Guards applied before a cast, so a malformed value becomes null instead of an error
# (even with ANSI mode on). 18 digits keep every match inside the range of a long.
_INT_RE = r"^-?\d{1,18}$"
_DEC_RE = r"^-?\d+(\.\d+)?$"


def nullify_placeholders(df: DataFrame, cols: list[str]) -> DataFrame:
    """Trim the given string columns and turn the contract placeholders into null.

    `Unknown`, `Unknown value`, `Undefined`, `N/A` and the empty string all mean "no value"
    in the DMA files.
    """
    for c in cols:
        trimmed = F.trim(F.col(c))
        df = df.withColumn(c, F.when(~trimmed.isin(PLACEHOLDERS), trimmed))
    return df


def to_long(col: Column) -> Column:
    """Cast an integer string to long; anything that is not an integer becomes null."""
    return F.when(col.rlike(_INT_RE), col.cast("long"))


def to_double(col: Column) -> Column:
    """Cast a decimal string (dot separator) to double; anything else becomes null."""
    return F.when(col.rlike(_DEC_RE), col.cast("double"))


def row_hash(cols: list[str]) -> Column:
    """SHA-256 over the given columns; null and empty string hash differently.

    concat_ws skips nulls, so nulls are replaced by a NUL marker first; the unit separator
    keeps ("ab", "c") and ("a", "bc") apart.
    """
    parts = [F.coalesce(F.col(c), F.lit("\u0000")) for c in cols]
    return F.sha2(F.concat_ws("\u001f", *parts), 256)


def _with_all_source_cols(df: DataFrame) -> DataFrame:
    """Add missing optional contract columns (and _corrupt_record) as null strings."""
    for c in [*SOURCE_COLS, CORRUPT_COL]:
        if c not in df.columns:
            df = df.withColumn(c, F.lit(None).cast(StringType()))
    return df


def to_typed(bronze_df: DataFrame) -> DataFrame:
    """Project Bronze rows onto typed Silver columns.

    Output columns:
    - typed values: mmsi, ts_utc, mobile_class, lat, lon, sog_kn, cog_deg, heading_deg, rot,
      nav_status_text, imo, callsign, name, ship_type, cargo_type, width_m, length_m,
      draught_m, destination, eta_utc;
    - lineage: _source_date, _source_file, _bronze_run_id;
    - `_raw`: a struct with the 26 Bronze columns and _corrupt_record, verbatim, so rejects
      can be stored exactly as received;
    - `_row_hash`: SHA-256 of the 26 verbatim Bronze columns (exact-duplicate key);
    - `flags`: an empty array<string>, filled by later steps.
    """
    df = _with_all_source_cols(bronze_df)
    df = df.withColumn(RAW_COL, F.struct(*[F.col(c) for c in [*SOURCE_COLS, CORRUPT_COL]]))
    df = df.withColumn(ROW_HASH_COL, row_hash(SOURCE_COLS))
    df = nullify_placeholders(df, SOURCE_COLS)

    mobile = F.col("type_of_mobile")
    mobile_class = F.lit(None).cast(StringType())
    for text, code in MOBILE_CLASSES.items():
        mobile_class = F.when(mobile == text, code).otherwise(mobile_class)

    return df.select(
        to_long(F.col("mmsi")).alias("mmsi"),
        F.to_timestamp(F.col("timestamp"), TS_FORMAT).alias("ts_utc"),
        mobile_class.alias("mobile_class"),
        to_double(F.col("latitude")).alias("lat"),
        to_double(F.col("longitude")).alias("lon"),
        to_double(F.col("sog")).alias("sog_kn"),
        to_double(F.col("cog")).alias("cog_deg"),
        to_double(F.col("heading")).alias("heading_deg"),
        to_double(F.col("rot")).alias("rot"),
        F.col("navigational_status").alias("nav_status_text"),
        to_long(F.col("imo")).alias("imo"),
        F.col("callsign"),
        F.col("name"),
        F.col("ship_type"),
        F.col("cargo_type"),
        to_double(F.col("width")).alias("width_m"),
        to_double(F.col("length")).alias("length_m"),
        to_double(F.col("draught")).alias("draught_m"),
        F.col("destination"),
        F.to_timestamp(F.col("eta"), TS_FORMAT).alias("eta_utc"),
        F.col("_source_date"),
        F.col("_source_file"),
        F.col("_run_id").alias("_bronze_run_id"),
        F.col(RAW_COL),
        F.col(ROW_HASH_COL),
        F.array().cast(ArrayType(StringType())).alias(FLAGS_COL),
    )


def apply_nav_labels(df: DataFrame, nav_lookup: DataFrame) -> DataFrame:
    """Map the DMA navigational-status text to a controlled snake_case label.

    nav_lookup has columns (source_text, nav_status) and is tiny, so it is broadcast: every
    executor gets a copy and the join needs no shuffle. Text not in the lookup becomes
    `other`; a missing status stays null.
    """
    lookup = F.broadcast(nav_lookup.withColumnRenamed("nav_status", "_nav_label"))
    joined = df.join(lookup, df["nav_status_text"] == lookup["source_text"], "left")
    label = F.when(F.col("nav_status_text").isNotNull(), F.coalesce("_nav_label", F.lit("other")))
    return joined.withColumn("nav_status", label).drop("source_text", "_nav_label")


def standardize(bronze_df: DataFrame, nav_lookup: DataFrame) -> DataFrame:
    """to_typed followed by the controlled labels."""
    return apply_nav_labels(to_typed(bronze_df), nav_lookup)
