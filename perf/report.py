"""Render ops.perf_results as Markdown: the median of each variant's runs.

Usage: python -m perf.report --env prod [--experiment P2]
"""

from __future__ import annotations

import argparse
import dataclasses
import sys
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ais_pipeline.config import load_settings
from ais_pipeline.spark import build_spark
from perf.harness import perf_path

MB = 1e6


def summary(df: DataFrame) -> DataFrame:
    """One row per (experiment, variant): medians over runs (small result)."""

    def med(c: str):
        return F.percentile_approx(c, 0.5).alias(c)

    return (
        df.groupBy("experiment", "variant")
        .agg(
            F.count(F.lit(1)).alias("runs"),
            med("wall_s"),
            med("input_bytes"),
            med("shuffle_read_bytes"),
            med("shuffle_write_bytes"),
            med("spill_bytes"),
            med("max_task_s"),
            med("median_task_s"),
            F.max("rows").alias("rows"),
            F.min("run_ts").alias("first_run"),
        )
        .orderBy("experiment", "first_run")
    )


def to_markdown(rows: list) -> str:
    lines = [
        "| experiment | variant | runs | median wall (s) | input (MB) | shuffle read (MB) | "
        "shuffle write (MB) | spill (MB) | max task (s) | median task (s) |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for r in rows:
        lines.append(
            f"| {r['experiment']} | {r['variant']} | {r['runs']} | {r['wall_s']:.1f} | "
            f"{(r['input_bytes'] or 0) / MB:,.0f} | {(r['shuffle_read_bytes'] or 0) / MB:,.0f} | "
            f"{(r['shuffle_write_bytes'] or 0) / MB:,.0f} | {(r['spill_bytes'] or 0) / MB:,.0f} | "
            f"{r['max_task_s'] or 0:.1f} | {r['median_task_s'] or 0:.2f} |"
        )
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env", default="prod")
    parser.add_argument("--experiment")
    parser.add_argument("--out-root", type=Path, help="perf lake root used by perf.experiments")
    args = parser.parse_args(argv)
    settings = load_settings(args.env)
    if args.out_root:
        settings = dataclasses.replace(settings, lake_dir=args.out_root / "lake")
    spark = build_spark(settings)
    df = spark.read.format("delta").load(str(perf_path(settings)))
    if args.experiment:
        df = df.where(F.col("experiment") == args.experiment)
    sys.stdout.write(to_markdown(summary(df).collect()) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
