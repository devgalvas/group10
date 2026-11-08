"""Silver core on the ais_tiny fixture: planted defects, row conservation, DQ metrics."""

from datetime import date

import pytest
from pyspark.sql import functions as F

from ais_pipeline.cli import main
from ais_pipeline.ops.dq import dq_path, latest_metrics
from ais_pipeline.paths import table_path
from ais_pipeline.silver.build import clean, run_silver_core, tag
from ais_pipeline.silver.dedup import drop_exact
from ais_pipeline.silver.flags import has_flag
from ais_pipeline.silver.rules import REASON_COL

pytestmark = pytest.mark.integration

DAY = date(2026, 9, 1)


@pytest.fixture
def core(spark, settings, bronze_tiny):
    result = run_silver_core(spark, settings, DAY, "silver-test", bronze_df=bronze_tiny)
    yield result
    result.clean.unpersist()


def test_fixture_counts(core):
    assert core.metric("bronze_in", "rows") == 259
    assert core.metric("bronze_in", "out_of_scope") == 3
    assert core.metric("bronze_in", "rejected") == 7
    assert core.metric("bronze_in", "exact_duplicates") == 2
    assert core.metric("silver_out", "exact_duplicates_removed") == 2
    assert core.metric("silver_out", "conflict_dropped") == 1
    assert core.metric("silver_out", "accepted") == 249
    assert core.metric("silver_out", "flag_TS_CONFLICT") == 1
    assert core.metric("silver_out", "flag_SOG_SENTINEL") == 1
    assert core.metric("silver_out", "flag_HEADING_SENTINEL") == 1


def test_rows_are_conserved(spark, core, bronze_tiny, nav_lookup):
    """rows_in = rejects + exact duplicates + dropped conflicts + accepted, each counted apart."""
    tagged = tag(bronze_tiny, nav_lookup)
    rows_in = bronze_tiny.count()
    rejects = tagged.where(F.col(REASON_COL).isNotNull()).count()
    valid = tagged.where(F.col(REASON_COL).isNull()).drop(REASON_COL)
    exact = valid.count() - drop_exact(valid).count()
    accepted = clean(tagged).count()
    conflicts = drop_exact(valid).select("mmsi", "ts_utc").distinct().count()
    conflict_dropped = drop_exact(valid).count() - conflicts

    assert rows_in == rejects + exact + conflict_dropped + accepted
    assert core.metric("silver_out", "unaccounted") == 0
    assert core.clean.count() == accepted


def test_no_duplicate_key_in_clean(core):
    assert core.clean.groupBy("mmsi", "ts_utc").count().where("count > 1").isEmpty()


def test_sentinel_values_are_null(core):
    flagged = core.clean.where(has_flag("SOG_SENTINEL")).first()
    assert flagged.sog_kn is None


def test_rejects_table_holds_bronze_rows(spark, settings, core):
    rejects = spark.read.format("delta").load(str(table_path(settings, "silver", "rejects")))
    assert rejects.count() == 7
    row = rejects.where(F.col(REASON_COL) == "BAD_MMSI").first()
    assert row["mmsi"] == "12345"  # verbatim string, not a typed value
    assert row["_silver_run_id"] == "silver-test"


def test_rerun_replaces_rejects_and_appends_metrics(spark, settings, bronze_tiny, core):
    again = run_silver_core(spark, settings, DAY, "silver-test-2", bronze_df=bronze_tiny)
    again.clean.unpersist()
    rejects = spark.read.format("delta").load(str(table_path(settings, "silver", "rejects")))
    assert rejects.count() == 7
    assert {r[0] for r in rejects.select("_silver_run_id").distinct().collect()} == {
        "silver-test-2"
    }
    runs = spark.read.format("delta").load(str(dq_path(settings))).select("run_id").distinct()
    assert runs.count() == 2
    assert latest_metrics(spark, settings, DAY).first()["run_id"] == "silver-test-2"


def test_dq_report_cli(core, capsys):
    assert main(["--env", "ci", "dq-report", "--date", DAY.isoformat()]) == 0
    out = capsys.readouterr().out
    assert "| metric | bronze_in | silver_out |" in out
    assert "| rows | 259 | 249 |" in out
