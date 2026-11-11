"""End-to-end Silver on the fixture: Bronze ingest -> build_silver -> vessel_position."""

import zipfile
from datetime import date

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    TimestampType,
)

from ais_pipeline.cli import main
from ais_pipeline.ingest.bronze import ingest_day
from ais_pipeline.paths import table_path
from ais_pipeline.silver.build import POSITION_COLS, build_silver

pytestmark = pytest.mark.integration

DAY = date(2026, 9, 1)
FIXTURE = "tests/fixtures/ais_tiny.csv"

EXPECTED_TYPES = {
    "mmsi": LongType(),
    "ts_utc": TimestampType(),
    "date": DateType(),
    "hour": IntegerType(),
    "lat": DoubleType(),
    "sog_kn": DoubleType(),
    "vessel_sk": LongType(),
    "zone": StringType(),
    "gap_s": LongType(),
    "step_km": DoubleType(),
    "flags": ArrayType(StringType()),
}


@pytest.fixture
def bronze_day(spark, settings):
    folder = settings.landing_dir / f"date={DAY.isoformat()}"
    folder.mkdir(parents=True)
    with zipfile.ZipFile(folder / "aisdk-2026-09-01.zip", "w", zipfile.ZIP_DEFLATED) as zf:
        zf.write(FIXTURE, "aisdk-2026-09-01.csv")
    ingest_day(spark, settings, DAY, run_id="bronze-run")
    return settings


def _positions(spark, settings):
    return spark.read.format("delta").load(str(table_path(settings, "silver", "vessel_position")))


def test_build_silver_on_fixture(spark, bronze_day):
    settings = bronze_day
    result = build_silver(spark, settings, DAY, "silver-run")
    assert (result.rows_in, result.rejected, result.rows_written) == (259, 7, 249)

    df = _positions(spark, settings)
    assert df.columns == POSITION_COLS
    types = {f.name: f.dataType for f in df.schema.fields}
    for col, expected in EXPECTED_TYPES.items():
        assert types[col] == expected, col

    assert df.groupBy("mmsi", "ts_utc").count().where("count > 1").isEmpty()
    # The vessel parked in port is in the port zone; every row has a zone.
    in_port = df.where(F.col("mmsi") == 212209000)
    assert in_port.where(F.col("zone") != "port").isEmpty()
    assert df.where(F.col("zone").isNull()).isEmpty()
    # Vessels that sent static data get a version; their ship type group is set.
    named = df.where(F.col("mmsi") == 311066400)
    assert named.where(F.col("vessel_sk").isNull()).isEmpty()
    assert {r.ship_type_group for r in named.select("ship_type_group").distinct().collect()} == {
        "tanker"
    }


def test_rerun_gives_the_same_rows(spark, bronze_day):
    settings = bronze_day
    build_silver(spark, settings, DAY, "silver-run-1")
    first = _positions(spark, settings).drop("_silver_run_id", "_processed_at")
    n = first.count()
    snapshot = first.collect()

    build_silver(spark, settings, DAY, "silver-run-2")
    again = _positions(spark, settings)
    assert again.count() == n
    assert sorted(again.drop("_silver_run_id", "_processed_at").collect()) == sorted(snapshot)
    dim = spark.read.format("delta").load(str(table_path(settings, "silver", "dim_vessel")))
    assert (
        dim.groupBy("mmsi")
        .agg(F.sum(F.col("is_current").cast("int")).alias("c"))
        .where("c != 1")
        .isEmpty()
    )


def test_silver_cli(bronze_day):
    assert main(["--env", "ci", "silver", "--start", "2026-09-01", "--end", "2026-09-01"]) == 0
