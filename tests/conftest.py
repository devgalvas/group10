"""Shared pytest fixtures: one Spark session for the whole run and a throwaway lake root."""

import csv
import json
import os
import time
from datetime import date
from pathlib import Path

import pytest
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType, StructField, StructType

from ais_pipeline.config import load_settings
from ais_pipeline.spark import build_spark

# collect() turns timestamps into naive datetimes in the Python process's local time zone.
# Pin it to UTC (as on CI) so tests compare UTC values on every laptop.
os.environ["TZ"] = "UTC"
time.tzset()


@pytest.fixture
def tmp_lake(monkeypatch, tmp_path):
    """Point AIS_DATA_ROOT at a fresh temp dir so every path in the ci config lands there."""
    monkeypatch.setenv("AIS_DATA_ROOT", str(tmp_path))
    return tmp_path


@pytest.fixture
def settings(tmp_lake):
    """ci settings whose landing and lake dirs live under tmp_lake."""
    return load_settings("ci")


@pytest.fixture(scope="session")
def spark():
    """One local[2] SparkSession with Delta for all tests (starting one is slow)."""
    session = build_spark(load_settings("ci"))
    # Tiny test tables: the default 50 Delta snapshot partitions only add scheduling overhead.
    session.conf.set("spark.databricks.delta.snapshotPartitions", "2")
    yield session
    session.stop()


# --- Silver helpers --------------------------------------------------------------------------

FIXTURE_DIR = Path(__file__).parent / "fixtures"
TINY_DAY = date(2026, 9, 1)

# A clean Class A row; tests override only the fields they care about. Empty CSV fields are
# null in Bronze (Spark reads "" as null), so they are None here.
GOOD_ROW = {
    "timestamp": "01/09/2026 00:00:00",
    "type_of_mobile": "Class A",
    "mmsi": "219000001",
    "latitude": "55.466000",
    "longitude": "8.430000",
    "navigational_status": "Moored",
    "rot": "0.0",
    "sog": "0.1",
    "cog": "200.8",
    "heading": "208",
    "imo": "9332559",
    "callsign": "OXAB2",
    "name": "TEST VESSEL",
    "ship_type": "Cargo",
    "cargo_type": None,
    "width": "27",
    "length": "186",
    "type_of_position_fixing_device": "GPS",
    "draught": "6.1",
    "destination": "ESBJERG",
    "eta": "01/09/2026 06:00:00",
    "data_source_type": "AIS",
    "a": None,
    "b": None,
    "c": None,
    "d": None,
}


@pytest.fixture(scope="session")
def make_bronze(spark):
    """Factory: list of partial row dicts -> a Bronze-shaped DataFrame (all strings + lineage)."""
    from ais_pipeline.ingest.bronze import CORRUPT_COL, add_lineage, load_contract

    cols = [c["bronze_name"] for c in load_contract()["columns"]]
    schema = StructType([StructField(c, StringType()) for c in [*cols, CORRUPT_COL]])

    def build(rows: list[dict], day: date = TINY_DAY, run_id: str = "bronze-test") -> DataFrame:
        data = [tuple({**GOOD_ROW, **r}.get(c) for c in [*cols, CORRUPT_COL]) for r in rows]
        return add_lineage(spark.createDataFrame(data, schema), "test.zip", "sha", day, run_id)

    return build


@pytest.fixture(scope="session")
def bronze_tiny(spark):
    """tests/fixtures/ais_tiny.csv read exactly like Bronze ingest reads a daily file."""
    from ais_pipeline.ingest.bronze import add_lineage, check_header, load_contract, read_raw_csv

    csv_path = FIXTURE_DIR / "ais_tiny.csv"
    with csv_path.open(encoding="utf-8-sig", newline="") as fh:
        header = next(csv.reader(fh))
    report = check_header(header, load_contract())
    raw = read_raw_csv(spark, csv_path, report)
    return add_lineage(raw, "ais_tiny.csv", "sha", TINY_DAY, "bronze-tiny").cache()


@pytest.fixture(scope="session")
def tiny_defects():
    """The planted defects of ais_tiny.csv (see tests/fixtures/README.md)."""
    return json.loads((FIXTURE_DIR / "ais_tiny_defects.json").read_text())


@pytest.fixture(scope="session")
def nav_lookup(spark):
    from ais_pipeline.silver.refs import load_nav_status

    return load_nav_status(spark, "ref/nav_status.csv")
