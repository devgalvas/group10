"""Smoke tests: the shared Spark session runs a job and reads back its own Delta write."""

import sys

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


def test_python_workers_import_the_package(spark):
    """Workers run the driver's interpreter, so RDD lambdas and UDFs can import ais_pipeline."""

    def worker_python(_):
        import sys

        import ais_pipeline  # noqa: F401  (fails in a python without the package)

        return sys.executable

    assert spark.sparkContext.parallelize([0], 1).map(worker_python).collect() == [sys.executable]
