"""Tests for silver/zones.py: in port, at anchor, at sea, and the boundaries."""

import pytest
from pyspark.sql import functions as F

from ais_pipeline.silver.dim_port import RefDataError, load_dim_port
from ais_pipeline.silver.zones import assign, validate_no_overlap

# Esbjerg centre 55.466, 8.430; port radius 2.5 km, anchorage 12 km. 0.009 deg lat ~ 1 km.
POINTS = [
    ("centre", 55.466, 8.430, "port", "DKEBJ"),
    ("2 km north", 55.466 + 2 * 0.008993, 8.430, "port", "DKEBJ"),
    ("3 km north", 55.466 + 3 * 0.008993, 8.430, "anchorage", "DKEBJ"),
    ("11.9 km north", 55.466 + 11.9 * 0.008993, 8.430, "anchorage", "DKEBJ"),
    ("12.1 km north", 55.466 + 12.1 * 0.008993, 8.430, "at_sea", None),
    ("open sea", 57.0, 5.0, "at_sea", None),
    ("Copenhagen", 55.705, 12.605, "port", "DKCPH"),
    ("Aarhus bay", 56.150 + 5 * 0.008993, 10.230, "anchorage", "DKAAR"),
]


@pytest.fixture(scope="module")
def ports(spark):
    return load_dim_port(spark, "ref/ports.csv")


@pytest.mark.parametrize("prefilter", [True, False])
def test_zones(spark, ports, prefilter):
    df = spark.createDataFrame(
        [(name, lat, lon) for name, lat, lon, _, _ in POINTS], "name STRING, lat DOUBLE, lon DOUBLE"
    )
    got = {r.name: (r.zone, r.port_id) for r in assign(df, ports, prefilter).collect()}
    assert got == {name: (zone, port) for name, _, _, zone, port in POINTS}


def test_one_output_row_per_input_row(spark, ports):
    df = spark.range(50).select(
        (F.lit(55.40) + F.col("id") * 0.004).alias("lat"), F.lit(8.43).alias("lon")
    )
    assert assign(df, ports).count() == 50


def test_overlapping_ports_are_rejected():
    a = {"port_id": "A", "lat": 55.0, "lon": 8.0, "anchorage_radius_km": 10.0}
    b = {"port_id": "B", "lat": 55.1, "lon": 8.0, "anchorage_radius_km": 10.0}
    with pytest.raises(RefDataError, match="overlap"):
        validate_no_overlap([a, b])
