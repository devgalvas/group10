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
    silver = sub.add_parser("silver", help="build silver tables for a date range (in order)")
    _add_range(silver)
    silver.add_argument(
        "--rdd-check", action="store_true", help="recount exact duplicates with RDDs (slow)"
    )
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


def cmd_bronze(settings: Settings, start: date, end: date, force: bool = False) -> None:
    """Ingest every landing zip in [start, end] into bronze.ais_raw, one partition per day.

    Days whose file was already ingested (same SHA-256) are skipped unless force is set.
    """
    from ais_pipeline.ingest.bronze import run_bronze_day
    from ais_pipeline.ops.runs import new_run_id
    from ais_pipeline.spark import build_spark

    spark = build_spark(settings)
    run_id = new_run_id()
    logger.info("bronze run %s for %s..%s (force=%s)", run_id, start, end, force)
    for day in date_range(start, end):
        entry = run_bronze_day(spark, settings, day, run_id, force=force)
        logger.info("%s %s %s (rows=%s)", day, entry.status, entry.file_name, entry.row_count)


def cmd_silver(settings: Settings, start: date, end: date, rdd_check: bool = False) -> None:
    """Build Silver for every day in [start, end], oldest first (SCD2 depends on the order)."""
    from ais_pipeline.ops.runs import new_run_id
    from ais_pipeline.silver.build import build_silver
    from ais_pipeline.spark import build_spark

    spark = build_spark(settings)
    run_id = new_run_id()
    logger.info("silver run %s for %s..%s", run_id, start, end)
    for day in date_range(start, end):
        build_silver(spark, settings, day, run_id, rdd_check=rdd_check)


def _log_status(spark, settings, run_id, stage, day, status, message) -> None:
    from ais_pipeline.ops.runs import run_logger

    with run_logger(spark, settings, run_id, stage, day) as rec:
        rec.status, rec.message = status, message


def gold_stage(spark, settings: Settings, run_id: str) -> None:
    """Build Gold under a pipeline_runs entry; log every gated-out date as stale."""
    from ais_pipeline.gold.build import build_gold
    from ais_pipeline.ops.runs import run_logger

    with run_logger(spark, settings, run_id, "gold", None) as rec:
        res = build_gold(spark, settings)
        rec.rows_out = res.port_calls
        rec.message = (
            f"published={len(res.published)} stale={len(res.stale)} "
            f"port_calls={res.port_calls} congestion_rows={res.congestion_rows}"
        )
        if not res.published:
            rec.status = "skipped"
    for day, gate in res.stale.items():
        _log_status(spark, settings, run_id, "gold", day, "stale", "; ".join(gate.reasons))


def cmd_gold(settings: Settings) -> None:
    from ais_pipeline.ops.runs import new_run_id, write_step_summary
    from ais_pipeline.spark import build_spark

    gold_stage(build_spark(settings), settings, new_run_id())
    write_step_summary(f"ais-pipeline gold ({settings.env})")


RUN_STAGES = ("download", "bronze", "silver", "gold")


def parse_stages(text: str) -> list[str]:
    stages = [s.strip() for s in text.split(",") if s.strip()]
    unknown = [s for s in stages if s not in RUN_STAGES]
    if unknown or not stages:
        raise ValueError(f"--stages must be a subset of {','.join(RUN_STAGES)}, got {text!r}")
    return stages


def next_day(spark, settings: Settings) -> date | None:
    """The first date of the window without a successful `run` entry in ops.pipeline_runs."""
    from pyspark.sql import functions as F

    from ais_pipeline.delta_io import table_exists
    from ais_pipeline.ops.runs import runs_path

    window = list(
        date_range(
            date.fromisoformat(settings.window.start), date.fromisoformat(settings.window.end)
        )
    )
    done: set[date] = set()
    if table_exists(spark, runs_path(settings)):
        rows = (
            spark.read.format("delta")
            .load(str(runs_path(settings)))
            .where((F.col("stage") == "run") & (F.col("status") == "success"))
            .select("source_date")
            .distinct()
            .collect()  # at most one row per window date
        )
        done = {r["source_date"] for r in rows}
    return next((d for d in window if d not in done), None)


def run_day(spark, settings: Settings, run_id: str, day: date, stages: list[str]) -> None:
    """download -> bronze -> silver for one date, each stage logged; stops at the first error."""
    from ais_pipeline.ops.runs import run_logger

    with run_logger(spark, settings, run_id, "run", day) as run_rec:
        if "download" in stages:
            from ais_pipeline.ingest.download import download_day

            with run_logger(spark, settings, run_id, "download", day) as rec:
                res = download_day(settings, day)
                rec.status = "skipped" if res.skipped else "success"
                rec.message = f"{res.path.name} {res.file_bytes:,} bytes in {res.seconds:.1f} s"
        if "bronze" in stages:
            from ais_pipeline.ingest.bronze import run_bronze_day

            with run_logger(spark, settings, run_id, "bronze", day) as rec:
                entry = run_bronze_day(spark, settings, day, run_id)
                rec.status, rec.rows_out, rec.message = entry.status, entry.row_count, entry.message
        if "silver" in stages:
            from ais_pipeline.silver.build import build_silver

            with run_logger(spark, settings, run_id, "silver", day) as rec:
                res = build_silver(spark, settings, day, run_id)
                rec.rows_in, rec.rows_out, rec.rows_rejected = (
                    res.rows_in,
                    res.rows_written,
                    res.rejected,
                )
        run_rec.message = f"stages={','.join(stages)}"


def cmd_run(
    settings: Settings, start: date | None, end: date | None, use_next_day: bool, stages_text: str
) -> None:
    """Run the stages for a date range, or for the next unprocessed day of the window."""
    from ais_pipeline.ops.runs import new_run_id, write_step_summary
    from ais_pipeline.spark import build_spark

    stages = parse_stages(stages_text)
    if use_next_day == (start is not None or end is not None):
        raise ValueError("give either --next-day or both --start and --end")
    if not use_next_day and (start is None or end is None):
        raise ValueError("--start and --end go together")

    spark = build_spark(settings)
    run_id = new_run_id()
    if use_next_day:
        day = next_day(spark, settings)
        if day is None:
            logger.info(
                "every day of the window %s..%s is done", settings.window.start, settings.window.end
            )
            _log_status(spark, settings, run_id, "run", None, "skipped", "window complete")
            write_step_summary(f"ais-pipeline run ({settings.env})")
            return
        days = [day]
    else:
        days = list(date_range(start, end))

    logger.info("run %s: %s for %s", run_id, ",".join(stages), ", ".join(map(str, days)))
    try:
        for day in days:
            run_day(spark, settings, run_id, day, stages)
        if "gold" in stages:
            gold_stage(spark, settings, run_id)
    finally:
        write_step_summary(f"ais-pipeline run ({settings.env})")


def cmd_dq_report(settings: Settings, day: date) -> None:
    """Print the latest run's dq_metrics for a day as a Markdown before/after table."""
    from ais_pipeline.ops.dq import latest_metrics, report_table
    from ais_pipeline.spark import build_spark

    metrics = latest_metrics(build_spark(settings), settings, day)
    if metrics is None:
        raise LookupError(f"no dq_metrics for {day}: run the silver stage first")
    run_id = metrics.select("run_id").first()["run_id"]
    sys.stdout.write(f"## Data quality {day} (run {run_id})\n\n{report_table(metrics)}\n")


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
            cmd_bronze(settings, args.start, args.end, args.force)
        elif args.command == "gold":
            cmd_gold(settings)
        elif args.command == "run":
            cmd_run(settings, args.start, args.end, args.next_day, args.stages)
        elif args.command == "silver":
            cmd_silver(settings, args.start, args.end, args.rdd_check)
        elif args.command == "dq-report":
            cmd_dq_report(settings, args.date)
        else:
            raise NotImplementedError(f"command {args.command!r} is not implemented yet")
    except Exception:
        logger.exception("command %s failed", args.command)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
