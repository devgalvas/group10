"""SCD2 tests for silver/dim_vessel.py: three days, a name change, rerun, as-of join."""

from datetime import date, datetime

import pytest
from pyspark.sql import functions as F

from ais_pipeline.paths import table_path
from ais_pipeline.silver.build import clean, tag
from ais_pipeline.silver.dim_vessel import OPEN_END, daily_versions, join_vessel, merge_scd2
from ais_pipeline.silver.refs import load_ship_type_groups

pytestmark = pytest.mark.integration

MMSI = "219000001"


@pytest.fixture(scope="module")
def groups(spark):
    return load_ship_type_groups(spark, "ref/ship_type_groups.csv").cache()


def _day(make_bronze, nav_lookup, d: date, name: str, n: int = 3):
    stamp = d.strftime("%d/%m/%Y")
    rows = [{"timestamp": f"{stamp} 0{h}:00:00", "mmsi": MMSI, "name": name} for h in range(n)]
    return clean(tag(make_bronze(rows, day=d), nav_lookup))


def _merge(spark, settings, groups, positions):
    path = str(table_path(settings, "silver", "dim_vessel"))
    stats = merge_scd2(spark, path, daily_versions(positions, 2), groups)
    return stats, spark.read.format("delta").load(path).orderBy("valid_from")


def test_three_days_with_a_name_change(spark, settings, make_bronze, nav_lookup, groups):
    d1, d2, d3 = date(2026, 9, 1), date(2026, 9, 2), date(2026, 9, 3)

    stats, dim = _merge(spark, settings, groups, _day(make_bronze, nav_lookup, d1, "OLD NAME"))
    assert stats == {"inserted": 1, "closed": 0}
    first = dim.first()
    assert first.valid_from == datetime(2026, 9, 1)  # the first version starts at midnight
    assert first.valid_to == OPEN_END and first.is_current
    assert first.ship_type_group == "cargo"

    stats, dim = _merge(spark, settings, groups, _day(make_bronze, nav_lookup, d2, "OLD NAME"))
    assert stats == {"inserted": 0, "closed": 0}
    assert dim.count() == 1

    day3 = _day(make_bronze, nav_lookup, d3, "NEW NAME")
    stats, dim = _merge(spark, settings, groups, day3)
    assert stats == {"inserted": 1, "closed": 1}
    old, new = dim.collect()
    assert (old.name, old.is_current, old.valid_to) == ("OLD NAME", False, datetime(2026, 9, 3))
    assert (new.name, new.is_current, new.valid_to) == ("NEW NAME", True, OPEN_END)
    assert dim.where("is_current").count() == 1

    # Rerunning day 3 changes nothing.
    stats, dim = _merge(spark, settings, groups, day3)
    assert stats == {"inserted": 0, "closed": 0}
    assert dim.count() == 2


def test_as_of_join_picks_the_valid_version(spark, settings, make_bronze, nav_lookup, groups):
    d1, d2 = date(2026, 9, 1), date(2026, 9, 2)
    p1 = _day(make_bronze, nav_lookup, d1, "OLD NAME")
    p2 = _day(make_bronze, nav_lookup, d2, "NEW NAME")
    _merge(spark, settings, groups, p1)
    _, dim = _merge(spark, settings, groups, p2)

    joined = join_vessel(p1.unionByName(p2), dim).join(
        dim.select("vessel_sk", F.col("name").alias("version_name")), "vessel_sk"
    )
    assert joined.where(F.col("name") != F.col("version_name")).isEmpty()
    assert joined.count() == 6


def test_single_report_does_not_open_a_version(spark, settings, make_bronze, nav_lookup, groups):
    d = date(2026, 9, 1)
    stamp = d.strftime("%d/%m/%Y")
    rows = [{"timestamp": f"{stamp} 0{h}:00:00", "mmsi": MMSI} for h in range(3)]
    rows.append({"timestamp": f"{stamp} 05:00:00", "mmsi": MMSI, "name": "GARBLED"})
    _, dim = _merge(spark, settings, groups, clean(tag(make_bronze(rows, day=d), nav_lookup)))
    assert [r.name for r in dim.collect()] == ["TEST VESSEL"]


def test_fixture_name_change_opens_a_second_version(
    spark, settings, bronze_tiny, nav_lookup, groups, tiny_defects
):
    _, dim = _merge(spark, settings, groups, clean(tag(bronze_tiny, nav_lookup)))
    mmsi = int(tiny_defects["vessels"]["moving_b"])
    versions = dim.where(F.col("mmsi") == mmsi).orderBy("valid_from").collect()
    assert [v.name for v in versions] == ["SONANGOL HUILA", "SONANGOL HUILA II"]
    assert versions[0].valid_to == versions[1].valid_from
    assert (
        dim.groupBy("mmsi")
        .agg(F.sum(F.col("is_current").cast("int")).alias("c"))
        .where("c != 1")
        .isEmpty()
    )
