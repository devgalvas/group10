"""End to end on the fixture: `run` (bronze, silver, gold), reconciliation, stale, next day."""

import dataclasses
import zipfile
from datetime import date
from pathlib import Path

import pytest
from pyspark.sql import functions as F

from ais_pipeline.cli import gold_stage, main, next_day
from ais_pipeline.config import load_settings
from ais_pipeline.ops.runs import runs_path
from ais_pipeline.paths import table_path

pytestmark = pytest.mark.integration

DAY = date(2026, 9, 1)


def _load(spark, settings, layer, name):
    return spark.read.format("delta").load(str(table_path(settings, layer, name)))


@pytest.fixture(scope="module")
def ran(spark, tmp_path_factory):
    """Land the fixture as the 2026-09-01 zip and run bronze -> silver -> gold via the CLI.

    Module-scoped: the full run takes about a minute, so the tests below share one lake.
    """
    root = tmp_path_factory.mktemp("lake")
    with pytest.MonkeyPatch.context() as mp:
        mp.setenv("AIS_DATA_ROOT", str(root))
        settings = load_settings("ci")
        folder = settings.landing_dir / f"date={DAY.isoformat()}"
        folder.mkdir(parents=True)
        with zipfile.ZipFile(folder / "aisdk-2026-09-01.zip", "w", zipfile.ZIP_DEFLATED) as zf:
            zf.write("tests/fixtures/ais_tiny.csv", "aisdk-2026-09-01.csv")
        argv = ["--env", "ci", "run", "--start", "2026-09-01", "--end", "2026-09-01"]
        assert main([*argv, "--stages", "bronze,silver,gold"]) == 0
        yield settings


def test_pipeline_runs_log(spark, ran):
    runs = spark.read.format("delta").load(str(runs_path(ran)))
    got = {(r.stage, r.status) for r in runs.collect()}
    assert got == {
        ("bronze", "success"),
        ("silver", "success"),
        ("gold", "success"),
        ("run", "success"),
    }
    silver = runs.where("stage = 'silver'").first()
    assert (silver.rows_in, silver.rows_out, silver.rows_rejected) == (259, 249, 7)


def test_vessel_in_port_has_a_call(spark, ran):
    calls = _load(spark, ran, "gold", "port_calls")
    assert calls.where(F.col("mmsi") == 212209000).count() >= 1
    assert calls.where(F.col("dwell_h") <= 0).isEmpty()


def test_calls_lie_within_the_silver_track(spark, ran):
    silver = _load(spark, ran, "silver", "vessel_position")
    span = silver.groupBy("mmsi").agg(F.min("ts_utc").alias("lo"), F.max("ts_utc").alias("hi"))
    calls = _load(spark, ran, "gold", "port_calls").join(span, "mmsi")
    outside = calls.where(
        (F.col("arrival_ts") < F.col("lo")) | (F.col("departure_ts") > F.col("hi"))
    )
    assert outside.isEmpty()


def test_reconciliation_in_port_counts(spark, ran):
    """Hourly vessels in port recomputed straight from Silver must equal Gold."""
    silver = _load(spark, ran, "silver", "vessel_position")
    expected = (
        silver.where("zone = 'port'")
        .groupBy("port_id", F.date_trunc("HOUR", "ts_utc").alias("hour_ts"), "ship_type_group")
        .agg(F.countDistinct("mmsi").alias("vessels_in_port"))
    )
    gold = (
        _load(spark, ran, "gold", "port_congestion_hourly")
        .where("vessels_in_port > 0")
        .select("port_id", "hour_ts", "ship_type_group", "vessels_in_port")
    )
    assert expected.count() > 0
    assert expected.exceptAll(gold).isEmpty() and gold.exceptAll(expected).isEmpty()


def test_failed_gate_marks_the_date_stale(spark, ran):
    before = _load(spark, ran, "gold", "port_congestion_hourly").count()
    # The fixture's in-scope reject rate is 4/256 = 1.6 %; a 0.1 % ceiling fails the gate.
    strict = dataclasses.replace(
        ran, quality=dataclasses.replace(ran.quality, max_reject_rate=0.001)
    )
    gold_stage(spark, strict, "strict-run")

    runs = spark.read.format("delta").load(str(runs_path(ran))).where("run_id = 'strict-run'")
    stale = runs.where("status = 'stale'").first()
    assert stale.source_date == DAY and "reject_rate" in stale.message
    assert runs.where("stage = 'gold' AND source_date IS NULL").first().status == "skipped"
    # The previous Gold rows for the date are kept.
    assert _load(spark, ran, "gold", "port_congestion_hourly").count() == before


def test_next_day_skips_done_dates(spark, ran):
    two_days = dataclasses.replace(
        ran, window=dataclasses.replace(ran.window, start="2026-09-01", end="2026-09-02")
    )
    assert next_day(spark, two_days) == date(2026, 9, 2)
    assert next_day(spark, ran) is None


def test_bad_stage_list_fails():
    argv = ["--env", "ci", "run", "--start", "2026-09-01", "--end", "2026-09-01"]
    assert main([*argv, "--stages", "bronze,platinum"]) == 1


def test_dashboard_renders_the_gold_tables(ran, monkeypatch):
    pytest.importorskip("streamlit")
    from streamlit.testing.v1 import AppTest

    monkeypatch.setattr("sys.argv", ["app.py", "--env", "ci"])
    app = Path(__file__).resolve().parents[2] / "dashboard" / "app.py"
    at = AppTest.from_file(str(app), default_timeout=60).run()
    assert not at.exception
    assert [t.label for t in at.tabs] == ["Congestion", "Port calls", "Pipeline health"]
    assert dict((m.label, m.value) for m in at.metric)["Port calls"] == "1"
