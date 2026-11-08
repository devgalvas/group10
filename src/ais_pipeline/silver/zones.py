"""Zone assignment: port, anchorage or at sea, from the distance to the port centres.

The port areas do not overlap (validate_no_overlap), so a position matches at most one port's
anchorage circle. That makes the assignment a single left join against the broadcast
dim_port, with no aggregation afterwards: rows that match no port are at sea.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from ais_pipeline.geo import haversine_km as haversine_km_py
from ais_pipeline.silver.dim_port import RefDataError
from ais_pipeline.silver.features import haversine_km

# A degree of latitude is ~111.2 km; dividing by 111.0 makes the box slightly larger than the
# circle, so the prefilter never drops a position that is inside it.
KM_PER_DEG_LAT = 111.0


def validate_no_overlap(ports: list[dict]) -> None:
    """Raise if two anchorage circles overlap (then a position could match two ports)."""
    for i, a in enumerate(ports):
        for b in ports[i + 1 :]:
            d = haversine_km_py(a["lat"], a["lon"], b["lat"], b["lon"])
            if d <= a["anchorage_radius_km"] + b["anchorage_radius_km"]:
                raise RefDataError(f"anchorage areas of {a['port_id']} and {b['port_id']} overlap")


def _with_bbox(dim_port: DataFrame) -> DataFrame:
    """Add a lat/lon bounding box around each anchorage circle (slightly larger than it)."""
    r = F.col("anchorage_radius_km")
    dlat = r / KM_PER_DEG_LAT
    dlon = r / (KM_PER_DEG_LAT * F.cos(F.radians("lat")))
    return dim_port.select(
        F.col("port_id"),
        F.col("lat").alias("_p_lat"),
        F.col("lon").alias("_p_lon"),
        F.col("port_radius_km").alias("_p_port_r"),
        F.col("anchorage_radius_km").alias("_p_anch_r"),
        (F.col("lat") - dlat).alias("_lat_min"),
        (F.col("lat") + dlat).alias("_lat_max"),
        (F.col("lon") - dlon).alias("_lon_min"),
        (F.col("lon") + dlon).alias("_lon_max"),
    )


def _distance() -> Column:
    return haversine_km(F.col("lat"), F.col("lon"), F.col("_p_lat"), F.col("_p_lon"))


def assign(df: DataFrame, dim_port: DataFrame, prefilter: bool = True) -> DataFrame:
    """Add zone ('port' | 'anchorage' | 'at_sea'), port_id and port_distance_km.

    With prefilter, a cheap bounding-box comparison runs before the trigonometry, so most
    open-sea rows never compute a distance. prefilter=False keeps the plain version for the
    performance comparison.
    """
    validate_no_overlap([r.asDict() for r in dim_port.collect()])  # 3 rows: bounded
    ports = F.broadcast(_with_bbox(dim_port))

    within = _distance() <= F.col("_p_anch_r")
    if prefilter:
        box = F.col("lat").between(F.col("_lat_min"), F.col("_lat_max")) & F.col("lon").between(
            F.col("_lon_min"), F.col("_lon_max")
        )
        within = box & within

    joined = df.join(ports, within, "left")
    zone = (
        F.when(F.col("port_id").isNull(), "at_sea")
        .when(_distance() <= F.col("_p_port_r"), "port")
        .otherwise("anchorage")
    )
    return (
        joined.withColumn("zone", zone)
        .withColumn("port_distance_km", F.when(F.col("port_id").isNotNull(), _distance()))
        .drop(*[c for c in ports.columns if c != "port_id"])
    )
