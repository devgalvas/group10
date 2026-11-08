"""Smoke test of the performance harness on a small DataFrame."""

from pyspark.sql import functions as F

from perf.harness import PLANS_DIR, measure, perf_path, save_results
from perf.report import summary, to_markdown


def test_measure_and_report(spark, settings, monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)  # plans are written under ./docs/perf/plans
    df = spark.range(10_000).groupBy((F.col("id") % 7).alias("k")).count()

    result = measure(spark, "T0", "groupby", lambda: df.count(), plan_df=df)

    assert result.rows == 7
    assert result.wall_s > 0
    assert result.shuffle_write_bytes > 0  # the groupBy shuffles
    assert (tmp_path / PLANS_DIR / "T0_groupby.txt").read_text().startswith("== Physical Plan ==")

    save_results(spark, settings, [result, result])
    rows = summary(spark.read.format("delta").load(str(perf_path(settings)))).collect()
    assert rows[0]["runs"] == 2
    assert "| T0 | groupby | 2 |" in to_markdown(rows)
