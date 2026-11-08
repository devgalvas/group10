"""Smoke tests: the shared Spark session runs a job and reads back its own Delta write."""

from pyspark.sql import functions as F


def test_spark_counts_a_dataframe(spark):
    df = spark.createDataFrame([(1, "a"), (2, "b"), (3, "c")], "id INT, label STRING")

    assert df.filter(F.col("id") > 1).count() == 2


def test_session_time_zone_is_utc(spark):
    assert spark.conf.get("spark.sql.session.timeZone") == "UTC"


def test_delta_round_trip(spark, tmp_path):
    path = str(tmp_path / "smoke_delta")
    spark.range(5).write.format("delta").save(path)

    back = spark.read.format("delta").load(path)

    assert sorted(r.id for r in back.collect()) == [0, 1, 2, 3, 4]
