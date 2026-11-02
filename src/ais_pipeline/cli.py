"""Command-line entry point: ais-pipeline --env {dev,prod,ci} <command>."""

from __future__ import annotations

import argparse
import logging
import platform
import sys
from collections.abc import Iterator
from datetime import date, timedelta

from ais_pipeline import __version__
from ais_pipeline.config import ENVS, Settings, load_settings

logger = logging.getLogger(__name__)


def _add_range(parser: argparse.ArgumentParser, required: bool = True) -> None:
    parser.add_argument("--start", type=date.fromisoformat, required=required)
    parser.add_argument("--end", type=date.fromisoformat, required=required)


def date_range(start: date, end: date) -> Iterator[date]:
    """Yield every date from start to end, inclusive."""
    if end < start:
        raise ValueError(f"--end {end} is before --start {start}")
    for offset in range((end - start).days + 1):
        yield start + timedelta(days=offset)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="ais-pipeline", description=__doc__)
    parser.add_argument("--env", choices=ENVS, required=True)
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    sub = parser.add_subparsers(dest="command", required=True)

    _add_range(sub.add_parser("download", help="fetch daily zips into landing"))
    bronze = sub.add_parser("bronze", help="ingest landing zips into bronze.ais_raw")
    _add_range(bronze)
    bronze.add_argument("--force", action="store_true", help="re-ingest even if hash matches")
    _add_range(sub.add_parser("silver", help="build silver tables for a date range"))
    sub.add_parser("gold", help="rebuild gold tables from silver")

    run = sub.add_parser("run", help="run the stages for a date range or the next day")
    _add_range(run, required=False)
    run.add_argument("--next-day", action="store_true")
    run.add_argument("--stages", default="download,bronze,silver,gold")

    dq = sub.add_parser("dq-report", help="print dq_metrics before/after for a date")
    dq.add_argument("--date", type=date.fromisoformat, required=True)

    sub.add_parser("check", help="smoke test: config, Spark + Delta, writable paths")
    return parser


def cmd_check(settings: Settings) -> None:
    """Start Spark, round-trip a 1-row Delta table under ${lake_dir}/_check, log versions."""
    from importlib.metadata import version

    import pyspark

    from ais_pipeline.spark import build_spark

    for folder in (settings.landing_dir, settings.lake_dir):
        folder.mkdir(parents=True, exist_ok=True)

    spark = build_spark(settings)
    check_path = str(settings.lake_dir / "_check")
    spark.range(1).withColumnRenamed("id", "ok").write.format("delta").mode("overwrite").save(
        check_path
    )
    rows = spark.read.format("delta").load(check_path).count()
    if rows != 1:
        raise RuntimeError(f"Delta round-trip returned {rows} rows, expected 1")

    java_version = spark.sparkContext._jvm.java.lang.System.getProperty("java.version")
    logger.info("env=%s data_root=%s", settings.env, settings.data_root)
    logger.info(
        "python=%s pyspark=%s delta-spark=%s java=%s",
        platform.python_version(),
        pyspark.__version__,
        version("delta-spark"),
        java_version,
    )
    logger.info("check OK: Delta round-trip at %s", check_path)


def cmd_download(settings: Settings, start: date, end: date) -> None:
    """Download every day in [start, end]; stop at the first failure."""
    from ais_pipeline.ingest.download import download_day

    for day in date_range(start, end):
        result = download_day(settings, day)
        status = "skipped" if result.skipped else "downloaded"
        logger.info(
            "%s %s %s (%.1f MB, %.1f s, sha256=%s)",
            day,
            status,
            result.path.name,
            result.file_bytes / 1e6,
            result.seconds,
            result.sha256[:12],
        )


def cmd_bronze(settings: Settings, start: date, end: date) -> None:
    """Ingest every landing zip in [start, end] into bronze.ais_raw, one partition per day."""
    from ais_pipeline.ingest.bronze import ingest_day
    from ais_pipeline.ops.runs import new_run_id
    from ais_pipeline.spark import build_spark

    spark = build_spark(settings)
    run_id = new_run_id()
    logger.info("bronze run %s for %s..%s", run_id, start, end)
    for day in date_range(start, end):
        ingest_day(spark, settings, day, run_id)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    args = build_parser().parse_args(argv)
    settings = load_settings(args.env)

    try:
        if args.command == "check":
            cmd_check(settings)
        elif args.command == "download":
            cmd_download(settings, args.start, args.end)
        elif args.command == "bronze":
            cmd_bronze(settings, args.start, args.end)
        else:
            raise NotImplementedError(f"command {args.command!r} is not implemented yet")
    except Exception:
        logger.exception("command %s failed", args.command)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
