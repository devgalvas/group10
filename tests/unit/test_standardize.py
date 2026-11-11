"""Tests for silver/standardize.py on hand-built Bronze rows."""

from datetime import datetime

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType, TimestampType

from ais_pipeline.silver.standardize import (
    nullify_placeholders,
    row_hash,
    standardize,
    to_typed,
)


def _one(df):
    rows = df.collect()
    assert len(rows) == 1
    return rows[0]


def test_nullify_placeholders_trims_and_nulls(spark):
    df = spark.createDataFrame(
        [(" Unknown value ",), ("Undefined",), ("N/A",), ("",), ("  MAERSK  ",), (None,)],
        "v STRING",
    )
    out = [r.v for r in nullify_placeholders(df, ["v"]).collect()]
    assert out == [None, None, None, None, "MAERSK", None]


def test_day_first_timestamp_is_parsed_as_utc(make_bronze):
    row = _one(to_typed(make_bronze([{"timestamp": "02/09/2026 13:45:07"}])))
    # The session time zone is UTC, so the naive datetime Spark returns is UTC.
    assert row.ts_utc == datetime(2026, 9, 2, 13, 45, 7)


def test_iso_timestamp_becomes_null(make_bronze):
    row = _one(to_typed(make_bronze([{"timestamp": "2026-09-01T00:10:08"}])))
    assert row.ts_utc is None


def test_typed_columns_have_the_silver_types(make_bronze):
    df = to_typed(make_bronze([{}]))
    types = {f.name: f.dataType for f in df.schema.fields}
    assert types["mmsi"] == LongType()
    assert types["imo"] == LongType()
    assert types["ts_utc"] == TimestampType()
    for c in ("lat", "lon", "sog_kn", "cog_deg", "heading_deg", "rot", "length_m", "width_m"):
        assert types[c] == DoubleType(), c


def test_values_are_cast(make_bronze):
    row = _one(to_typed(make_bronze([{}])))
    assert row.mmsi == 219000001
    assert row.lat == 55.466 and row.lon == 8.43
    assert row.sog_kn == 0.1 and row.heading_deg == 208.0
    assert row.imo == 9332559 and row.length_m == 186.0
    assert row.eta_utc == datetime(2026, 9, 1, 6, 0, 0)
    assert row._bronze_run_id == "bronze-test"


def test_bad_casts_become_null(make_bronze):
    row = _one(
        to_typed(
            make_bronze(
                [{"mmsi": "21900000X", "latitude": "fifty", "sog": "1,5", "imo": "Unknown"}]
            )
        )
    )
    assert row.mmsi is None
    assert row.lat is None
    assert row.sog_kn is None  # a comma decimal is not a valid number
    assert row.imo is None


def test_placeholders_become_null_in_typed_columns(make_bronze):
    row = _one(to_typed(make_bronze([{"name": "Unknown", "ship_type": "Undefined"}])))
    assert row.name is None
    assert row.ship_type is None


def test_mobile_class(make_bronze):
    df = to_typed(
        make_bronze(
            [
                {"type_of_mobile": "Class A"},
                {"type_of_mobile": "Class B"},
                {"type_of_mobile": "AtoN"},
            ]
        )
    )
    assert sorted((r.mobile_class or "-") for r in df.collect()) == ["-", "A", "B"]


def test_raw_struct_keeps_verbatim_values(make_bronze):
    row = _one(to_typed(make_bronze([{"name": " Unknown ", "sog": "102.3"}])))
    assert row._raw["name"] == " Unknown "
    assert row._raw["sog"] == "102.3"
    assert row._raw["_corrupt_record"] is None


def test_row_hash_tells_null_from_empty(spark):
    df = spark.createDataFrame([("a", None), ("a", ""), ("a", None)], "x STRING, y STRING")
    hashes = [r.h for r in df.select(row_hash(["x", "y"]).alias("h")).collect()]
    assert hashes[0] == hashes[2]
    assert hashes[0] != hashes[1]


def test_nav_status_labels(make_bronze, nav_lookup):
    df = standardize(
        make_bronze(
            [
                {"navigational_status": "Under way using engine"},
                {"navigational_status": "Something new"},
                {"navigational_status": "Unknown value"},
            ]
        ),
        nav_lookup,
    )
    labels = sorted((r.nav_status or "-") for r in df.collect())
    assert labels == ["-", "other", "under_way_engine"]


def test_every_fixture_nav_status_is_mapped(bronze_tiny, nav_lookup):
    df = standardize(bronze_tiny, nav_lookup)
    assert df.where(F.col("nav_status") == "other").count() == 0
