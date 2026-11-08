"""Gold unit tests on hand-built Silver slices: port calls, congestion, gates."""

from datetime import date, datetime, timedelta

import pytest

from ais_pipeline.config import QualitySettings
from ais_pipeline.gold.congestion import hourly_congestion
from ais_pipeline.gold.gates import check
from ais_pipeline.gold.port_calls import detect_port_calls

T0 = datetime(2026, 9, 1, 8, 0, 0)
SCHEMA = (
    "mmsi LONG, ts_utc TIMESTAMP, date DATE, zone STRING, port_id STRING, vessel_sk LONG, "
    "ship_type_group STRING, sog_kn DOUBLE"
)


def _track(spark, points):
    """points: (mmsi, minutes after T0, zone, port_id[, sog_kn])."""
    rows = []
    for p in points:
        mmsi, minutes, zone, port = p[:4]
        sog = p[4] if len(p) > 4 else 0.0
        ts = T0 + timedelta(minutes=minutes)
        rows.append((mmsi, ts, ts.date(), zone, port, 7, "cargo", sog))
    return spark.createDataFrame(rows, SCHEMA)


def _calls(spark, points, gap_h=2, min_min=30):
    return detect_port_calls(_track(spark, points), gap_h, min_min).orderBy("arrival_ts").collect()


def test_one_continuous_call(spark):
    calls = _calls(spark, [(1, m, "port", "DKEBJ") for m in range(0, 121, 10)])
    assert len(calls) == 1
    c = calls[0]
    assert (c.arrival_ts, c.departure_ts) == (T0, T0 + timedelta(minutes=120))
    assert c.dwell_h == 2.0 and c.n_positions == 13
    assert c.ship_type_group == "cargo" and c.arrival_date == date(2026, 9, 1)


def test_gap_longer_than_threshold_splits_the_call(spark):
    first = [(1, m, "port", "DKEBJ") for m in range(0, 61, 10)]
    second = [(1, m, "port", "DKEBJ") for m in range(240, 301, 10)]  # back after 3 h
    assert len(_calls(spark, first + second)) == 2


def test_short_stay_is_dropped(spark):
    assert _calls(spark, [(1, m, "port", "DKEBJ") for m in range(0, 21, 5)]) == []


def test_port_a_then_port_b(spark):
    pts = [(1, m, "port", "DKEBJ") for m in range(0, 61, 10)]
    pts += [(1, m, "port", "DKAAR") for m in range(70, 131, 10)]
    calls = _calls(spark, pts)
    assert [c.port_id for c in calls] == ["DKEBJ", "DKAAR"]


def test_positions_outside_port_are_ignored(spark):
    pts = [(1, m, "port", "DKEBJ") for m in range(0, 61, 10)]
    pts += [(1, 30, "anchorage", "DKEBJ"), (1, 35, "at_sea", None)]
    calls = _calls(spark, pts)
    assert len(calls) == 1 and calls[0].n_positions == 7


def test_call_id_is_deterministic(spark):
    pts = [(1, m, "port", "DKEBJ") for m in range(0, 61, 10)]
    assert _calls(spark, pts)[0].call_id == _calls(spark, list(reversed(pts)))[0].call_id


def test_hourly_congestion(spark):
    positions = _track(
        spark,
        [
            (1, 5, "port", "DKEBJ"),
            (1, 10, "port", "DKEBJ"),  # same vessel twice in the hour: counted once
            (2, 15, "port", "DKEBJ"),
            (3, 20, "anchorage", "DKEBJ", 0.3),  # waiting at anchor
            (4, 25, "anchorage", "DKEBJ", 8.0),  # moving through the anchorage: not waiting
            (5, 70, "port", "DKEBJ"),  # next hour
        ],
    )
    calls = detect_port_calls(
        _track(spark, [(9, m, "port", "DKEBJ") for m in range(0, 91, 10)]), 2, 30
    )
    rows = {
        r.hour_ts: r
        for r in hourly_congestion(spark, positions, calls, anchor_max_sog_kn=1.0).collect()
    }
    h8, h9 = rows[T0], rows[T0 + timedelta(hours=1)]
    assert (h8.vessels_in_port, h8.vessels_at_anchor, h8.arrivals, h8.departures) == (2, 1, 1, 0)
    assert (h9.vessels_in_port, h9.vessels_at_anchor, h9.arrivals, h9.departures) == (1, 0, 0, 1)
    assert h8.date == date(2026, 9, 1)


QUALITY = QualitySettings(max_reject_rate=0.05, min_rows_vs_median=0.5, min_hours_present=24)
DAY = date(2026, 9, 1)


def test_gate_passes():
    assert check(DAY, 1000, 24, 0.01, [900, 1100], QUALITY).passed


@pytest.mark.parametrize(
    ("rows", "hours", "rate", "others", "reason"),
    [
        (1000, 20, 0.01, [], "hours_present"),
        (1000, 24, 0.10, [], "reject_rate"),
        (400, 24, 0.01, [1000, 1000, 900], "rows"),
        (None, None, None, [], "no dq_metrics"),
    ],
)
def test_gate_fails(rows, hours, rate, others, reason):
    result = check(DAY, rows, hours, rate, others, QUALITY)
    assert not result.passed
    assert any(reason in r for r in result.reasons)


def test_volume_gate_needs_history():
    assert check(DAY, 10, 24, 0.0, [], QUALITY).passed
