"""Tests for silver/dedup.py."""

from pyspark.sql import functions as F

from ais_pipeline.silver.dedup import TS_GROUP_COL, drop_exact, resolve_conflicts
from ais_pipeline.silver.flags import has_flag
from ais_pipeline.silver.standardize import to_typed


def test_drop_exact_keeps_one_copy(make_bronze):
    df = to_typed(make_bronze([{}, {}, {}, {"sog": "5.0"}]))
    assert drop_exact(df).count() == 2


def test_drop_exact_tells_null_from_text(make_bronze):
    # "Unknown" and a missing value both become null in typed columns, but they are not the
    # same message: only verbatim copies are exact duplicates.
    df = to_typed(make_bronze([{"name": None}, {"name": "Unknown"}]))
    assert drop_exact(df).count() == 2


def test_conflict_keeps_most_complete_row_and_flags_it(make_bronze):
    df = to_typed(make_bronze([{"sog": None, "cog": None}, {"sog": "7.5"}]))
    out = resolve_conflicts(df).collect()
    assert len(out) == 1
    assert out[0].sog_kn == 7.5
    assert out[0].flags == ["TS_CONFLICT"]
    assert out[0][TS_GROUP_COL] == 2


def test_conflict_tie_break_is_deterministic(make_bronze):
    rows = [{"sog": "7.5"}, {"sog": "7.6"}, {"sog": "7.7"}]
    first = resolve_conflicts(to_typed(make_bronze(rows))).first()
    again = resolve_conflicts(to_typed(make_bronze(list(reversed(rows))))).first()
    assert first._row_hash == again._row_hash
    assert first._row_hash == min(r._row_hash for r in to_typed(make_bronze(rows)).collect())


def test_same_mmsi_other_timestamp_is_not_a_conflict(make_bronze):
    df = to_typed(
        make_bronze([{"timestamp": "01/09/2026 00:00:00"}, {"timestamp": "01/09/2026 00:00:01"}])
    )
    out = resolve_conflicts(df)
    assert out.count() == 2
    assert out.where(has_flag("TS_CONFLICT")).count() == 0


def test_other_mmsi_same_timestamp_is_not_a_conflict(make_bronze):
    df = to_typed(make_bronze([{"mmsi": "219000001"}, {"mmsi": "219000002"}]))
    assert resolve_conflicts(df).where(F.size("flags") > 0).count() == 0
