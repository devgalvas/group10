"""Performance experiments P1-P6 on one real prod day (run on the VM).

Reads the prod lake; writes ONLY under --out-root (default /data/ais/perf): ops.perf_results,
and P4's full Silver rebuilds. Every variant gets a warm-up run that is not recorded, then
--runs measured runs; the report uses the median.

Usage (from the repo root, on the VM):
    unset SPARK_HOME; export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
    ~/ais-env-dev/bin/python -m perf.experiments --day 2026-09-01 P1 P2 P3 P5 P6
    ~/ais-env-dev/bin/python -m perf.experiments --day 2026-09-01 --runs 1 P4
    # P4 one variant per process (a fresh JVM each time; several Silver builds in one
    # process got the JVM OOM-killed on the 8 GB VM):
    ~/ais-env-dev/bin/python -m perf.experiments --day 2026-09-01 --runs 1 --no-warmup \
        --variants silver_sp16_coalesce_on P4
    ~/ais-env-dev/bin/python -m perf.report --env prod --out-root /data/ais/perf
"""

from __future__ import annotations

import argparse
import dataclasses
import logging
import shutil
import sys
from collections.abc import Callable
from datetime import date
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ais_pipeline.config import Settings, load_settings
from ais_pipeline.paths import table_path
from ais_pipeline.spark import build_spark
from perf.harness import PerfResult, measure_repeated, save_results

logger = logging.getLogger("perf")

ESBJERG = (55.466, 8.430)


@dataclasses.dataclass
class Ctx:
    spark: SparkSession
    src: Settings  # prod: read only
    out: Settings  # perf lake: written
    day: date
    runs: int
    variants: frozenset[str] = frozenset()  # empty = all
    warmup: bool = True

    def load(self, layer: str, name: str) -> DataFrame:
        return self.spark.read.format("delta").load(str(table_path(self.src, layer, name)))

    def day_col(self, col: str) -> F.Column:
        return F.col(col) == F.lit(self.day.isoformat()).cast("date")

    def positions(self) -> DataFrame:
        return self.load("silver", "vessel_position").where(self.day_col("date"))

    def bronze(self) -> DataFrame:
        return self.load("bronze", "ais_raw").where(self.day_col("_source_date"))

    def run(
        self,
        exp: str,
        variant: str,
        action: Callable[[], object],
        plan_df: DataFrame | None = None,
        notes: str | None = None,
        runs: int | None = None,
    ) -> list[PerfResult]:
        if self.variants and variant not in self.variants:
            logger.info("%s %s: skipped (not in --variants)", exp, variant)
            return []
        if self.warmup:
            action()  # warm-up: JIT, file cache, Delta snapshot; not recorded
        res = measure_repeated(
            self.spark, exp, variant, action, plan_df, runs=runs or self.runs, notes=notes
        )
        save_results(self.spark, self.out, res)
        return res


def noop(df: DataFrame) -> Callable[[], None]:
    """Run the whole plan without storing the output (the 'noop' data source)."""
    return lambda: df.write.format("noop").mode("overwrite").save()


def dir_bytes(path: Path) -> int:
    return sum(p.stat().st_size for p in path.rglob("*") if p.is_file())


def write_note(name: str, text: str) -> None:
    out = Path("docs/perf") / name
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(text)
    logger.info("wrote %s", out)


# --- P1: raw format and partition pruning -----------------------------------------------------


def p1(c: Ctx) -> None:
    from ais_pipeline.ingest.bronze import (
        check_header,
        extract_csv,
        landing_zip,
        load_contract,
        read_header,
        read_raw_csv,
    )

    zip_path = landing_zip(c.src, c.day)
    member, header = read_header(zip_path)
    report = check_header(header, load_contract())
    extract_dir = c.out.landing_dir / "_extract_p1"
    csv_path = extract_csv(zip_path, member, extract_dir)
    try:
        csv_df = read_raw_csv(c.spark, csv_path, report)
        moored_csv = csv_df.where(F.col("navigational_status") == "Moored")
        c.run(
            "P1",
            "a_csv_filter_count",
            moored_csv.count,
            moored_csv,
            notes=f"csv_bytes={csv_path.stat().st_size}",
        )
    finally:
        shutil.rmtree(extract_dir, ignore_errors=True)

    moored_delta = c.bronze().where(F.col("navigational_status") == "Moored")
    bronze_dir = table_path(c.src, "bronze", "ais_raw") / f"_source_date={c.day.isoformat()}"
    c.run(
        "P1",
        "b_delta_filter_count",
        moored_delta.count,
        moored_delta,
        notes=f"delta_partition_bytes={dir_bytes(bronze_dir)}",
    )

    silver = c.load("silver", "vessel_position")
    pruned = silver.where(c.day_col("date")).groupBy("zone").count()
    c.run("P1", "c_partition_filter", noop(pruned), pruned)
    unpruned = silver.where(F.to_date("ts_utc") == F.lit(c.day.isoformat()).cast("date"))
    unpruned = unpruned.groupBy("zone").count()
    c.run("P1", "d_expression_filter", noop(unpruned), unpruned)


# --- P2: join strategies ----------------------------------------------------------------------


def p2(c: Ctx) -> None:
    from ais_pipeline.silver.dim_port import load_dim_port
    from ais_pipeline.silver.dim_vessel import join_vessel
    from ais_pipeline.silver.zones import assign

    pos = c.positions().select("mmsi", "ts_utc", "lat", "lon")
    dim = c.load("silver", "dim_vessel")

    joined = join_vessel(pos, dim, broadcast=True)
    c.run("P2", "asof_broadcast", noop(joined), joined)

    threshold = c.spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
    c.spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    try:
        smj = join_vessel(pos, dim, broadcast=False)
        c.run("P2", "asof_sortmerge", noop(smj), smj)
    finally:
        c.spark.conf.set("spark.sql.autoBroadcastJoinThreshold", threshold)

    ports = load_dim_port(c.spark, c.src.refs.ports).cache()
    with_box = assign(pos, ports, prefilter=True)
    c.run("P2", "zones_bbox", noop(with_box), with_box)
    no_box = assign(pos, ports, prefilter=False)
    c.run("P2", "zones_no_bbox", noop(no_box), no_box)


# --- P3: skew in the per-vessel window --------------------------------------------------------


def p3(c: Ctx) -> None:
    from ais_pipeline.silver.build import clean, tag
    from ais_pipeline.silver.features import add_movement
    from ais_pipeline.silver.refs import load_nav_status

    pos = c.positions().select("mmsi", "ts_utc", "lat", "lon", "flags")
    per = pos.groupBy("mmsi").count()
    q = per.agg(
        F.percentile_approx("count", [0.5, 0.9, 0.99]).alias("q"),
        F.max("count").alias("max"),
        F.count(F.lit(1)).alias("vessels"),
        F.sum("count").alias("rows"),
    ).first()
    top = per.orderBy(F.desc("count")).limit(20).collect()  # 20 rows
    lines = [
        f"# P3 rows per MMSI, {c.day}",
        "",
        f"vessels {q['vessels']:,}, rows {q['rows']:,}; p50 {q['q'][0]:,}, p90 {q['q'][1]:,}, "
        f"p99 {q['q'][2]:,}, max {q['max']:,}",
        "",
        "| rank | mmsi | rows | share of day |",
        "|---:|---:|---:|---:|",
    ]
    lines += [
        f"| {i} | {r['mmsi']} | {r['count']:,} | {r['count'] / q['rows']:.2%} |"
        for i, r in enumerate(top, 1)
    ]
    write_note(f"p3_rows_per_mmsi_{c.day}.md", "\n".join(lines) + "\n")

    jump = c.src.features.jump_speed_kn
    base = add_movement(pos, jump)
    c.run("P3", "window_base", noop(base), base)

    skew_key = "spark.sql.adaptive.skewJoin.enabled"
    old = c.spark.conf.get(skew_key)
    c.spark.conf.set(skew_key, "false")
    try:
        c.run("P3", "window_aqe_skew_off", noop(add_movement(pos, jump)))
    finally:
        c.spark.conf.set(skew_key, old)

    for n in (16, 32, 64):
        rep = add_movement(pos.repartition(n, "mmsi"), jump)
        c.run("P3", f"window_repartition_{n}", noop(rep), rep)

    nav = load_nav_status(c.spark, c.src.refs.nav_status)
    tagged = tag(c.bronze(), nav)
    valid = tagged.where(F.col("reject_reason").isNull()).drop("reject_reason")
    before = add_movement(valid, jump)
    c.run("P3", "window_before_dedup", noop(before), before)
    after = add_movement(clean(tagged), jump)
    c.run("P3", "window_after_dedup", noop(after), after)


# --- P4: shuffle partitions and AQE coalescing (full Silver build) -----------------------------


def p4(c: Ctx) -> None:
    from ais_pipeline.ops.runs import new_run_id
    from ais_pipeline.silver.build import build_silver

    bronze = c.bronze()
    keys = ("spark.sql.shuffle.partitions", "spark.sql.adaptive.coalescePartitions.enabled")
    saved = {k: c.spark.conf.get(k) for k in keys}
    try:
        for coalesce in ("true", "false"):
            for n in (200, 64, 16, 8):
                c.spark.conf.set(keys[0], str(n))
                c.spark.conf.set(keys[1], coalesce)
                variant = f"silver_sp{n}_coalesce_{'on' if coalesce == 'true' else 'off'}"
                c.run(
                    "P4",
                    variant,
                    lambda: build_silver(c.spark, c.out, c.day, new_run_id(), bronze_df=bronze),
                )
    finally:
        for k, v in saved.items():
            c.spark.conf.set(k, v)


# --- P5: Python UDF vs pandas UDF vs built-in functions ----------------------------------------


def p5(c: Ctx) -> None:
    import math

    import pandas as pd
    from pyspark.sql.functions import pandas_udf, udf
    from pyspark.sql.types import DoubleType

    from ais_pipeline.geo import haversine_km as hav_py
    from ais_pipeline.silver.features import haversine_km

    lat0, lon0 = ESBJERG
    pos = c.positions().select("lat", "lon")

    builtin = pos.select(
        haversine_km(F.col("lat"), F.col("lon"), F.lit(lat0), F.lit(lon0)).alias("km")
    )
    c.run("P5", "builtin", _sum_first(builtin), builtin)

    py_udf = udf(lambda la, lo: hav_py(la, lo, lat0, lon0), DoubleType())
    plain = pos.select(py_udf("lat", "lon").alias("km"))
    c.run("P5", "python_udf", _sum_first(plain), plain)

    r = 6371.0088

    def hav_np(la, lo):
        import numpy as np

        p1, p2 = np.radians(la), math.radians(lat0)
        dp, dl = p2 - p1, math.radians(lon0) - np.radians(lo)
        a = np.sin(dp / 2) ** 2 + np.cos(p1) * math.cos(p2) * np.sin(dl / 2) ** 2
        return 2 * r * np.arcsin(np.sqrt(a))

    # Real types, not strings: this module uses `from __future__ import annotations`, and
    # pandas_udf reads the annotations to pick the Series -> Series flavour.
    hav_np.__annotations__ = {"la": pd.Series, "lo": pd.Series, "return": pd.Series}
    hav_pd = pandas_udf(hav_np, DoubleType())

    vec = pos.select(hav_pd("lat", "lon").alias("km"))
    c.run("P5", "pandas_udf", _sum_first(vec), vec)


def _sum_first(df: DataFrame) -> Callable[[], None]:
    return lambda: df.agg(F.sum("km")).first()


# --- P6: RDD vs DataFrame exact-duplicate count (AIS-22) ---------------------------------------


def p6(c: Ctx) -> None:
    from ais_pipeline.silver.rdd_check import exact_duplicates_rdd
    from ais_pipeline.silver.standardize import SOURCE_COLS

    bronze = c.bronze()
    c.run("P6", "rdd_reducebykey", lambda: exact_duplicates_rdd(bronze))
    distinct = bronze.select(*SOURCE_COLS).distinct()
    c.run("P6", "dataframe_distinct", lambda: bronze.count() - distinct.count(), distinct)


EXPERIMENTS = {"P1": p1, "P2": p2, "P3": p3, "P4": p4, "P5": p5, "P6": p6}


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("experiments", nargs="+", choices=sorted(EXPERIMENTS))
    parser.add_argument("--env", default="prod")
    parser.add_argument("--day", type=date.fromisoformat, required=True)
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--out-root", type=Path, default=Path("/data/ais/perf"))
    parser.add_argument(
        "--variants",
        default="",
        help="comma-separated variant names to run (default: all)",
    )
    parser.add_argument(
        "--no-warmup",
        action="store_true",
        help="skip the unrecorded warm-up run (P4: each Silver build is already cold)",
    )
    args = parser.parse_args(argv)

    src = load_settings(args.env)
    out = dataclasses.replace(
        src,
        data_root=args.out_root,
        landing_dir=args.out_root / "landing",
        lake_dir=args.out_root / "lake",
    )
    if out.lake_dir == src.lake_dir:
        raise SystemExit("--out-root must differ from the source data root")
    spark = build_spark(src)
    variants = frozenset(v.strip() for v in args.variants.split(",") if v.strip())
    ctx = Ctx(spark, src, out, args.day, args.runs, variants, warmup=not args.no_warmup)
    for name in args.experiments:
        logger.info("=== %s on %s ===", name, args.day)
        EXPERIMENTS[name](ctx)
    return 0


if __name__ == "__main__":
    sys.exit(main())
