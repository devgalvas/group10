"""Tests for silver/rdd_check.py."""

from ais_pipeline.silver.rdd_check import exact_duplicates_rdd


def test_counts_extra_copies(make_bronze):
    df = make_bronze([{}, {}, {}, {"sog": "5.0"}, {"sog": "5.0"}, {"sog": "6.0"}])
    assert exact_duplicates_rdd(df) == 3  # 2 extra of the first row + 1 of the second


def test_no_duplicates(make_bronze):
    assert exact_duplicates_rdd(make_bronze([{"sog": "1"}, {"sog": "2"}])) == 0


def test_agrees_with_dataframe_count_on_fixture(bronze_tiny):
    from ais_pipeline.silver.standardize import SOURCE_COLS

    df_count = bronze_tiny.count() - bronze_tiny.select(*SOURCE_COLS).distinct().count()
    assert exact_duplicates_rdd(bronze_tiny) == df_count == 2
