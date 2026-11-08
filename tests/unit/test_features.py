"""Tests for silver/features.py."""

from datetime import date

import pytest
from pyspark.sql import functions as F

from ais_pipeline.silver.features import add_movement, haversine_km, last_positions
from ais_pipeline.silver.flags import has_flag
from ais_pipeline.silver.standardize import to_typed

ESBJERG = (55.466, 8.430)
AARHUS = (56.150, 10.230)


def test_haversine_esbjerg_aarhus(spark):
    df = spark.createDataFrame([(*ESBJERG, *AARHUS)], "a DOUBLE, b DOUBLE, c DOUBLE, d DOUBLE")
    km = df.select(haversine_km(F.col("a"), F.col("b"), F.col("c"), F.col("d"))).first()[0]
    assert km == pytest.approx(135.5, abs=2)


def test_haversine_zero(spark):
    df = spark.createDataFrame([(*ESBJERG,)], "a DOUBLE, b DOUBLE")
    assert df.select(haversine_km(F.col("a"), F.col("b"), F.col("a"), F.col("b"))).first()[0] == 0


def _track(make_bronze, fixes, mmsi="219000001", day=date(2026, 9, 1)):
    return to_typed(
        make_bronze(
            [
                {"mmsi": mmsi, "timestamp": ts, "latitude": str(lat), "longitude": str(lon)}
                for ts, lat, lon in fixes
            ],
            day=day,
        )
    )


def test_gap_step_and_speed(make_bronze):
    # 0.01 deg of latitude is ~1.112 km; in 60 s that is ~36 kn.
    df = _track(
        make_bronze,
        [("01/09/2026 00:00:00", 55.00, 8.0), ("01/09/2026 00:01:00", 55.01, 8.0)],
    )
    rows = add_movement(df, jump_speed_kn=50).orderBy("ts_utc").collect()
    assert rows[0].gap_s is None and rows[0].step_km is None
    assert rows[1].gap_s == 60
    assert rows[1].step_km == pytest.approx(1.112, abs=0.01)
    assert rows[1].implied_speed_kn == pytest.approx(36.0, abs=0.5)
    assert rows[1].flags == []


def test_jump_is_flagged(make_bronze):
    df = _track(
        make_bronze,
        [("01/09/2026 00:00:00", 55.0, 8.0), ("01/09/2026 00:00:30", 55.5, 8.0)],
    )
    rows = add_movement(df, jump_speed_kn=50).orderBy("ts_utc").collect()
    assert rows[1].flags == ["POSITION_JUMP"]


def test_vessels_do_not_mix(make_bronze):
    a = _track(make_bronze, [("01/09/2026 00:00:00", 55.0, 8.0)], mmsi="219000001")
    b = _track(make_bronze, [("01/09/2026 00:00:10", 56.0, 9.0)], mmsi="219000002")
    rows = add_movement(a.unionByName(b), jump_speed_kn=50).collect()
    assert all(r.gap_s is None for r in rows)


def test_context_row_fills_the_midnight_gap(make_bronze):
    yesterday = _track(make_bronze, [("31/08/2026 23:59:00", 55.0, 8.0)], day=date(2026, 8, 31))
    today = _track(make_bronze, [("01/09/2026 00:01:00", 55.0, 8.0)])
    rows = add_movement(today, 50, context=last_positions(yesterday)).collect()
    assert len(rows) == 1  # the context row is dropped again
    assert rows[0].gap_s == 120
    assert rows[0].step_km == 0


def test_last_positions_takes_the_latest_fix(make_bronze):
    df = _track(
        make_bronze,
        [("01/09/2026 10:00:00", 55.0, 8.0), ("01/09/2026 23:00:00", 56.0, 9.0)],
    )
    row = last_positions(df).first()
    assert (row.lat, row.lon) == (56.0, 9.0)


def test_fixture_jump_is_flagged(bronze_tiny, nav_lookup, tiny_defects):
    from ais_pipeline.silver.build import clean, tag

    out = add_movement(clean(tag(bronze_tiny, nav_lookup)), jump_speed_kn=50)
    jump = next(d for d in tiny_defects["defects"] if d["expected"] == "POSITION_JUMP")
    flagged = out.where(has_flag("POSITION_JUMP")).select(
        F.date_format("ts_utc", "dd/MM/yyyy HH:mm:ss").alias("ts"), "mmsi"
    )
    got = {(str(r.mmsi), r.ts) for r in flagged.collect()}
    assert (jump["mmsi"], jump["timestamp"]) in got
    # The fix after the jump comes back, so it is flagged too: 2 flags, both for that vessel.
    assert len(got) == 2
    assert {m for m, _ in got} == {jump["mmsi"]}
