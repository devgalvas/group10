"""Tests for silver/rules.py: one test per reject code, priority, and DataFrame == SQL."""

import pytest
from pyspark.sql import functions as F

from ais_pipeline.silver.rules import (
    REASON_COL,
    REJECT_CODES,
    split_valid_rejects,
    with_reject_reason,
    with_reject_reason_sql,
)
from ais_pipeline.silver.standardize import standardize, to_typed


def _reason(make_bronze, **overrides):
    rows = with_reject_reason(to_typed(make_bronze([overrides]))).collect()
    return rows[0][REASON_COL]


def test_clean_row_is_valid(make_bronze):
    assert _reason(make_bronze) is None


@pytest.mark.parametrize(
    ("overrides", "code"),
    [
        ({"_corrupt_record": "01/09/2026 00:00:59,Class A,219787000,too,few"}, "MALFORMED_ROW"),
        ({"type_of_mobile": "Base Station"}, "OUT_OF_SCOPE_MOBILE"),
        ({"type_of_mobile": "AtoN"}, "OUT_OF_SCOPE_MOBILE"),
        ({"timestamp": "2026-09-01T00:10:08"}, "BAD_TIMESTAMP"),
        ({"timestamp": None}, "BAD_TIMESTAMP"),
        ({"timestamp": "31/08/2026 23:59:59"}, "TS_OUTSIDE_FILE_DAY"),
        ({"latitude": "91.000000"}, "POSITION_SENTINEL"),
        ({"longitude": "181.000000"}, "POSITION_SENTINEL"),
        ({"latitude": None}, "BAD_POSITION"),
        ({"latitude": "abc"}, "BAD_POSITION"),
        ({"latitude": "-90.5"}, "BAD_POSITION"),
        ({"longitude": "180.5"}, "BAD_POSITION"),
        ({"mmsi": "12345"}, "BAD_MMSI"),
        ({"mmsi": "119000001"}, "BAD_MMSI"),
        ({"mmsi": "819000001"}, "BAD_MMSI"),
        ({"mmsi": "2190000011"}, "BAD_MMSI"),
        ({"mmsi": None}, "BAD_MMSI"),
    ],
)
def test_each_rule(make_bronze, overrides, code):
    assert _reason(make_bronze, **overrides) == code


def test_boundaries_are_valid(make_bronze):
    assert _reason(make_bronze, latitude="90.0", longitude="-180.0") is None
    assert _reason(make_bronze, mmsi="799999999") is None
    assert _reason(make_bronze, timestamp="01/09/2026 23:59:59") is None


def test_first_code_wins(make_bronze):
    # Out of scope AND a bad timestamp AND a bad MMSI: the earliest rule in A.4 wins.
    assert (
        _reason(make_bronze, type_of_mobile="AtoN", timestamp="garbage", mmsi="1")
        == "OUT_OF_SCOPE_MOBILE"
    )
    # The sentinel is checked before the range rule, which would also catch lat 91.
    assert _reason(make_bronze, latitude="91", mmsi="1") == "POSITION_SENTINEL"


def test_split_is_a_partition(make_bronze):
    df = to_typed(make_bronze([{}, {"mmsi": "1"}, {"type_of_mobile": "AtoN"}]))
    valid, rejects = split_valid_rejects(df)
    assert valid.count() == 1
    assert rejects.count() == 2
    assert REASON_COL not in valid.columns


def test_fixture_defects_get_the_expected_codes(bronze_tiny, nav_lookup, tiny_defects):
    tagged = with_reject_reason(standardize(bronze_tiny, nav_lookup))
    got = {
        (r["_raw"]["mmsi"], r["_raw"]["timestamp"]): r[REASON_COL]
        for r in tagged.where(F.col(REASON_COL).isNotNull()).collect()
    }
    expected = {
        (d["mmsi"], d["timestamp"]): d["expected"]
        for d in tiny_defects["defects"]
        if d["expected"] in REJECT_CODES
    }
    assert got == expected


def test_sql_and_dataframe_rules_agree(spark, bronze_tiny, nav_lookup, make_bronze):
    extra = make_bronze(
        [
            {"_corrupt_record": "x"},
            {"latitude": "abc"},
            {"mmsi": None},
            {"timestamp": None},
            {"longitude": "181"},
        ]
    )
    df = standardize(bronze_tiny.unionByName(extra), nav_lookup)
    by_df = with_reject_reason(df).groupBy(REASON_COL).count()
    by_sql = with_reject_reason_sql(spark, df).groupBy(REASON_COL).count()
    assert sorted(by_df.collect(), key=str) == sorted(by_sql.collect(), key=str)
    # And row by row, not just the totals.
    key = ["_row_hash", REASON_COL]
    assert (
        with_reject_reason(df)
        .select(key)
        .exceptAll(with_reject_reason_sql(spark, df).select(key))
        .isEmpty()
    )
