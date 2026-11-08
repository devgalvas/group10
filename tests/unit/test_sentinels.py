"""Tests for silver/sentinels.py and silver/flags.py."""

import pytest
from pyspark.sql import functions as F

from ais_pipeline.silver.flags import add_flag
from ais_pipeline.silver.sentinels import apply_sentinels
from ais_pipeline.silver.standardize import to_typed


def _one(make_bronze, **overrides):
    return apply_sentinels(to_typed(make_bronze([overrides]))).first()


def test_clean_values_are_untouched(make_bronze):
    row = _one(make_bronze)
    assert row.flags == []
    assert (row.sog_kn, row.cog_deg, row.heading_deg) == (0.1, 200.8, 208.0)


@pytest.mark.parametrize(
    ("overrides", "col", "code"),
    [
        ({"sog": "102.3"}, "sog_kn", "SOG_SENTINEL"),
        ({"sog": "150"}, "sog_kn", "SOG_SENTINEL"),
        ({"sog": "fast"}, "sog_kn", "SOG_SENTINEL"),
        ({"cog": "360.0"}, "cog_deg", "COG_SENTINEL"),
        ({"heading": "511"}, "heading_deg", "HEADING_SENTINEL"),
        ({"heading": "360"}, "heading_deg", "HEADING_SENTINEL"),
    ],
)
def test_sentinel_is_nulled_and_flagged(make_bronze, overrides, col, code):
    row = _one(make_bronze, **overrides)
    assert row[col] is None
    assert row.flags == [code]


def test_edge_values_are_valid(make_bronze):
    row = _one(make_bronze, sog="102.2", cog="359.9", heading="359")
    assert row.flags == []
    assert (row.sog_kn, row.cog_deg, row.heading_deg) == (102.2, 359.9, 359.0)


def test_placeholder_is_null_without_a_flag(make_bronze):
    row = _one(make_bronze, sog=None, heading="Unknown")
    assert row.sog_kn is None and row.heading_deg is None
    assert row.flags == []


def test_several_flags_on_one_row(make_bronze):
    row = _one(make_bronze, sog="102.3", heading="511")
    assert sorted(row.flags) == ["HEADING_SENTINEL", "SOG_SENTINEL"]


def test_add_flag_does_not_duplicate(make_bronze):
    df = to_typed(make_bronze([{}]))
    df = add_flag(add_flag(df, F.lit(True), "TS_CONFLICT"), F.lit(True), "TS_CONFLICT")
    assert df.first().flags == ["TS_CONFLICT"]


def test_add_flag_rejects_unknown_codes(make_bronze):
    with pytest.raises(ValueError, match="unknown flag"):
        add_flag(to_typed(make_bronze([{}])), F.lit(True), "NOPE")
