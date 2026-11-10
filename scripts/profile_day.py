"""Profile one Bronze day and check the reference data against it (AIS-16).

Writes a Markdown report: volume, type of mobile, missing values per column, sentinels and
invalid values, duplicates and (mmsi, ts_utc) conflicts, rows per hour, rows per vessel (skew),
the nav status and ship type texts with their mapping, and where moored vessels actually lie
around each port centre in ref/ports.csv.

    python scripts/profile_day.py --env prod --day 2026-09-01 --out docs/profiling_2026-09-01.md

Read-only: it reads bronze.ais_raw and the ref CSVs and writes only the Markdown file.
Every collect() is on an aggregate of bounded size (one row, 24 hours, top-N, distinct texts).
"""

from __future__ import annotations

import argparse
import sys
import time
from datetime import date
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ais_pipeline.config import load_settings
from ais_pipeline.silver.build import read_bronze_day
from ais_pipeline.silver.dim_port import load_dim_port
from ais_pipeline.silver.features import haversine_km
from ais_pipeline.silver.refs import load_nav_status, load_ship_type_groups
from ais_pipeline.silver.standardize import PLACEHOLDERS, SOURCE_COLS, to_typed
from ais_pipeline.spark import build_spark

TOP_N = 15
MOORED = "Moored"


def table(header: list[str], rows: list[list]) -> str:
    def fmt(v) -> str:
        if isinstance(v, float):
            return f"{v:,.2f}"
        if isinstance(v, int):
            return f"{v:,}"
        return "" if v is None else str(v)

    lines = ["| " + " | ".join(header) + " |", "|" + "---|" * len(header)]
    lines += ["| " + " | ".join(fmt(v) for v in r) + " |" for r in rows]
    return "\n".join(lines)


def pct(n: int, d: int) -> float:
    return 100.0 * n / d if d else 0.0


def volume(raw: DataFrame) -> tuple[int, str]:
    r = raw.agg(
        F.count(F.lit(1)).alias("rows"),
        F.countDistinct("mmsi").alias("mmsi"),
    ).first()
    by_type = (
        raw.groupBy("type_of_mobile")
        .agg(F.count(F.lit(1)).alias("rows"), F.countDistinct("mmsi").alias("mmsi"))
        .orderBy(F.desc("rows"))
        .collect()
    )
    n = r["rows"]
    out = f"Rows: **{n:,}** · distinct MMSI (raw text): **{r['mmsi']:,}**\n\n"
    out += table(
        ["type of mobile", "rows", "% rows", "distinct MMSI"],
        [[t["type_of_mobile"], t["rows"], pct(t["rows"], n), t["mmsi"]] for t in by_type],
    )
    return n, out


def missing(raw: DataFrame, n: int) -> str:
    """Per column: null, placeholder text and empty string, in one pass."""
    aggs = []
    for c in SOURCE_COLS:
        t = F.trim(F.col(c))
        aggs += [
            F.sum(F.col(c).isNull().cast("long")).alias(f"{c}__null"),
            F.sum((t == "").cast("long")).alias(f"{c}__empty"),
            F.sum((t.isin([p for p in PLACEHOLDERS if p])).cast("long")).alias(f"{c}__ph"),
        ]
    r = raw.agg(*aggs).first().asDict()
    rows = []
    for c in SOURCE_COLS:
        null, empty, ph = r[f"{c}__null"] or 0, r[f"{c}__empty"] or 0, r[f"{c}__ph"] or 0
        rows.append([c, null, empty, ph, pct(null + empty + ph, n)])
    rows.sort(key=lambda x: -x[4])
    return table(["column", "null", "empty", "placeholder text", "% missing (all)"], rows)


def validity(typed: DataFrame, day: date, n: int) -> str:
    """Sentinels and invalid values on the typed projection, in one pass."""
    raw = F.col("_raw")
    checks = {
        "timestamp does not parse": F.col("ts_utc").isNull() & raw["timestamp"].isNotNull(),
        "timestamp not on the file day": F.to_date("ts_utc") != F.lit(day.isoformat()).cast("date"),
        "MMSI not 9 digits starting 2-7": ~F.coalesce(
            F.trim(raw["mmsi"]).rlike("^[2-7][0-9]{8}$"), F.lit(False)
        ),
        "lat = 91 (not available)": F.col("lat") == 91.0,
        "lon = 181 (not available)": F.col("lon") == 181.0,
        "lat/lon null": F.col("lat").isNull() | F.col("lon").isNull(),
        "lat/lon out of range (excl. 91/181)": (
            ~F.col("lat").between(-90, 90) & (F.col("lat") != 91.0)
        )
        | (~F.col("lon").between(-180, 180) & (F.col("lon") != 181.0)),
        "SOG >= 102.3 (not available)": F.col("sog_kn") >= 102.3,
        "COG >= 360 (not available)": F.col("cog_deg") >= 360.0,
        "heading = 511 (not available)": F.col("heading_deg") == 511.0,
        "heading 360-510 (invalid)": (F.col("heading_deg") > 359.0)
        & (F.col("heading_deg") != 511.0),
        "SOG text that does not parse": F.col("sog_kn").isNull()
        & raw["sog"].isNotNull()
        & ~F.trim(raw["sog"]).isin(PLACEHOLDERS),
        "malformed CSV line": raw["_corrupt_record"].isNotNull(),
    }
    r = typed.agg(
        *[F.sum(F.coalesce(c, F.lit(False)).cast("long")).alias(k) for k, c in checks.items()]
    ).first()
    return table(["check", "rows", "% rows"], [[k, r[k] or 0, pct(r[k] or 0, n)] for k in checks])


def duplicates(typed: DataFrame, n: int) -> str:
    distinct_hash = typed.select("_row_hash").distinct().count()
    groups = (
        typed.dropDuplicates(["_row_hash"])
        .where(F.col("mmsi").isNotNull() & F.col("ts_utc").isNotNull())
        .groupBy("mmsi", "ts_utc")
        .agg(F.count(F.lit(1)).alias("versions"))
        .where("versions > 1")
        .agg(F.count(F.lit(1)).alias("keys"), F.sum("versions").alias("rows"))
        .first()
    )
    exact = n - distinct_hash
    return table(
        ["measure", "value", "% rows"],
        [
            ["exact duplicate rows (same 26 raw values)", exact, pct(exact, n)],
            ["(mmsi, ts_utc) keys with different contents", groups["keys"] or 0, None],
            ["rows in those conflicting keys", groups["rows"] or 0, pct(groups["rows"] or 0, n)],
        ],
    )


def per_hour(typed: DataFrame) -> str:
    rows = (
        typed.where(F.col("ts_utc").isNotNull())
        .groupBy(F.hour("ts_utc").alias("hour"))
        .agg(F.count(F.lit(1)).alias("rows"), F.countDistinct("mmsi").alias("mmsi"))
        .orderBy("hour")
        .collect()
    )
    return table(
        ["hour (UTC)", "rows", "distinct MMSI"], [[r["hour"], r["rows"], r["mmsi"]] for r in rows]
    )


def skew(typed: DataFrame) -> str:
    per = typed.groupBy("mmsi").agg(
        F.count(F.lit(1)).alias("rows"),
        F.first("name", ignorenulls=True).alias("name"),
        F.first("type_of_mobile_raw", ignorenulls=True).alias("type"),
    )
    q = per.agg(
        F.count(F.lit(1)).alias("vessels"),
        F.percentile_approx("rows", [0.5, 0.9, 0.99]).alias("q"),
        F.max("rows").alias("max"),
        F.sum("rows").alias("total"),
    ).first()
    top = per.orderBy(F.desc("rows")).limit(TOP_N).collect()
    top_sum = sum(t["rows"] for t in top)
    out = table(
        ["MMSIs", "median rows", "p90", "p99", "max", f"share of top {TOP_N}"],
        [
            [
                q["vessels"],
                q["q"][0],
                q["q"][1],
                q["q"][2],
                q["max"],
                f"{pct(top_sum, q['total']):.2f} %",
            ]
        ],
    )
    out += "\n\n" + table(
        ["MMSI", "rows", "name", "type of mobile"],
        [[str(t["mmsi"]), t["rows"], t["name"], t["type"]] for t in top],
    )
    return out


def nav_status(raw: DataFrame, settings, spark) -> str:
    ref = load_nav_status(spark, settings.refs.nav_status)
    counts = raw.groupBy(F.col("navigational_status").alias("text")).count()
    rows = (
        counts.join(F.broadcast(ref), counts["text"] == ref["source_text"], "left")
        .orderBy(F.desc("count"))
        .collect()
    )

    def label(r) -> str:
        if r["text"] is None or r["text"].strip() in PLACEHOLDERS:
            return "null (placeholder)"
        return r["nav_status"] or "**UNMAPPED**"

    unmapped = [r for r in rows if label(r) == "**UNMAPPED**"]
    head = f"Distinct texts: {len(rows)} · unmapped (not a placeholder): **{len(unmapped)}**\n\n"
    return head + table(
        ["DMA text", "rows", "label"],
        [[r["text"], r["count"], label(r)] for r in rows],
    )


def ship_types(raw: DataFrame, settings, spark) -> str:
    ref = load_ship_type_groups(spark, settings.refs.ship_types)
    counts = raw.groupBy(F.col("ship_type").alias("text")).agg(
        F.count(F.lit(1)).alias("rows"), F.countDistinct("mmsi").alias("mmsi")
    )
    rows = (
        counts.join(F.broadcast(ref), counts["text"] == ref["ship_type"], "left")
        .orderBy(F.desc("rows"))
        .collect()
    )

    def group(r) -> str:
        if r["text"] is None or r["text"].strip() in PLACEHOLDERS:
            return "unknown (missing)"
        return r["ship_type_group"] or "**other (not in ref)**"

    unmapped = [r for r in rows if group(r).startswith("**")]
    head = f"Distinct texts: {len(rows)} · not in the reference: **{len(unmapped)}**\n\n"
    return head + table(
        ["DMA ship type", "rows", "distinct MMSI", "group"],
        [[r["text"], r["rows"], r["mmsi"], group(r)] for r in rows],
    )


def port_centres(typed: DataFrame, settings, spark) -> str:
    """Where moored vessels lie relative to each configured port circle."""
    ports = load_dim_port(spark, settings.refs.ports)
    moored = typed.where(
        (F.col("_raw")["navigational_status"] == MOORED)
        & F.col("lat").between(-90, 90)
        & F.col("lon").between(-180, 180)
    ).select("mmsi", "lat", "lon")
    p = ports.select(
        "port_id",
        F.col("lat").alias("p_lat"),
        F.col("lon").alias("p_lon"),
        "port_radius_km",
        "anchorage_radius_km",
    )
    near = (
        moored.join(F.broadcast(p), how="cross")
        .withColumn(
            "d_km", haversine_km(F.col("lat"), F.col("lon"), F.col("p_lat"), F.col("p_lon"))
        )
        .where(F.col("d_km") <= F.col("anchorage_radius_km"))
    )
    rows = (
        near.groupBy("port_id", "p_lat", "p_lon", "port_radius_km", "anchorage_radius_km")
        .agg(
            F.count(F.lit(1)).alias("fixes"),
            F.countDistinct("mmsi").alias("vessels"),
            F.sum((F.col("d_km") <= F.col("port_radius_km")).cast("long")).alias("inside_port"),
            F.percentile_approx("lat", 0.5).alias("med_lat"),
            F.percentile_approx("lon", 0.5).alias("med_lon"),
            F.percentile_approx("d_km", [0.5, 0.9, 0.99]).alias("dq"),
        )
        .orderBy("port_id")
        .collect()
    )
    out = []
    for r in rows:
        shift = (
            spark.range(1)
            .select(
                haversine_km(
                    F.lit(r["p_lat"]), F.lit(r["p_lon"]), F.lit(r["med_lat"]), F.lit(r["med_lon"])
                )
            )
            .first()[0]
        )
        out.append(
            [
                r["port_id"],
                r["vessels"],
                r["fixes"],
                f"{pct(r['inside_port'], r['fixes']):.1f} %",
                f"{r['dq'][0]:.2f} / {r['dq'][1]:.2f} / {r['dq'][2]:.2f}",
                r["port_radius_km"],
                f"{r['med_lat']:.4f}, {r['med_lon']:.4f}",
                f"{shift:.2f}",
            ]
        )
    return table(
        [
            "port",
            "moored vessels",
            "moored fixes in anchorage circle",
            "inside port circle",
            "distance to centre km (p50 / p90 / p99)",
            "port radius km",
            "median moored position",
            "median vs centre (km)",
        ],
        out,
    )


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--env", default="prod")
    ap.add_argument("--day", type=date.fromisoformat, required=True)
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args(argv)

    settings = load_settings(args.env)
    spark = build_spark(settings)
    t0 = time.time()
    # A full day does not fit twice in a 5 GB driver: caching raw and typed in memory starved the
    # shuffle of the duplicate count (SparkOutOfMemoryError). raw is re-read from Delta (cheap);
    # typed is reused by most sections, so it is persisted on disk only.
    raw = read_bronze_day(spark, settings, args.day)
    typed = (
        to_typed(raw)
        .withColumn("type_of_mobile_raw", F.col("_raw")["type_of_mobile"])
        .persist(StorageLevel.DISK_ONLY)
    )

    n, vol = volume(raw)
    if n == 0:
        sys.stderr.write(f"no Bronze rows for {args.day} in env {args.env}\n")
        return 1
    sections = [
        ("Volume and type of mobile", vol),
        ("Missing values per column", missing(raw, n)),
        ("Sentinels and invalid values", validity(typed, args.day, n)),
        ("Duplicates and conflicts", duplicates(typed, n)),
        ("Rows per hour", per_hour(typed)),
        ("Rows per vessel (skew)", skew(typed)),
        ("Navigational status texts", nav_status(raw, settings, spark)),
        ("Ship type texts", ship_types(raw, settings, spark)),
        ("Port centres: where moored vessels lie", port_centres(typed, settings, spark)),
    ]
    wall = time.time() - t0
    doc = [
        f"# Profile of {args.day} ({args.env})",
        "",
        f"Generated by `scripts/profile_day.py` "
        f"in {wall:,.0f} s. Source: `bronze.ais_raw`, `_source_date = {args.day}`.",
        "",
    ]
    for title, body in sections:
        doc += [f"## {title}", "", body, ""]
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text("\n".join(doc))
    sys.stdout.write(f"wrote {args.out} ({wall:,.0f} s)\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
