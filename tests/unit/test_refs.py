"""Tests for the reference data: dim_port and the ship-type groups."""

import pytest
from pyspark.sql import functions as F

from ais_pipeline.silver.dim_port import RefDataError, load_dim_port, validate_ports
from ais_pipeline.silver.refs import (
    PORT_SCHEMA,
    SHIP_TYPE_GROUPS,
    load_ship_type_groups,
    with_ship_type_group,
)
from ais_pipeline.silver.standardize import to_typed


@pytest.fixture(scope="module")
def groups(spark):
    return load_ship_type_groups(spark, "ref/ship_type_groups.csv")


def test_dim_port_schema_and_ports(spark):
    df = load_dim_port(spark, "ref/ports.csv")
    # CSV sources are always read as nullable, so compare names and types only.
    assert [(f.name, f.dataType) for f in df.schema] == [(f.name, f.dataType) for f in PORT_SCHEMA]
    assert sorted(r.port_id for r in df.collect()) == ["DKAAR", "DKCPH", "DKEBJ"]


def test_port_validation_rejects_bad_rows(spark):
    bad = spark.createDataFrame([("X", "x", 95.0, 8.0, 2.0, 10.0)], PORT_SCHEMA)
    with pytest.raises(RefDataError, match="invalid port"):
        validate_ports(bad)
    narrow = spark.createDataFrame([("X", "x", 55.0, 8.0, 5.0, 4.0)], PORT_SCHEMA)
    with pytest.raises(RefDataError, match="invalid port"):
        validate_ports(narrow)


def test_every_group_is_a_known_group(groups):
    assert {r.ship_type_group for r in groups.collect()} <= set(SHIP_TYPE_GROUPS)


def test_ship_types_map_one_to_one(groups):
    assert groups.groupBy("ship_type").count().where("count > 1").isEmpty()


def test_every_fixture_ship_type_maps_to_a_group(bronze_tiny, groups):
    typed = to_typed(bronze_tiny)
    unmapped = typed.where(F.col("ship_type").isNotNull()).join(
        groups, on="ship_type", how="left_anti"
    )
    assert unmapped.isEmpty()
    out = with_ship_type_group(typed, groups)
    assert out.where(F.col("ship_type_group").isNull()).isEmpty()


def test_fallbacks(make_bronze, groups):
    df = to_typed(make_bronze([{"ship_type": "Undefined"}, {"ship_type": "Hovercraft"}]))
    got = sorted(r.ship_type_group for r in with_ship_type_group(df, groups).collect())
    assert got == ["other", "unknown"]
