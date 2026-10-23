"""Build the SparkSession used by every pipeline stage."""

from __future__ import annotations

import logging
from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from ais_pipeline.config import Settings

logger = logging.getLogger(__name__)


def build_spark(settings: Settings) -> SparkSession:
    """Create (or reuse) a SparkSession with Delta, UTC time and AQE enabled."""
    cfg = settings.spark
    builder = (
        SparkSession.builder.master(cfg.master)
        .appName(cfg.app_name)
        .config("spark.driver.memory", cfg.driver_memory)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", str(cfg.shuffle_partitions))
        .config("spark.ui.showConsoleProgress", "false")
    )
    if cfg.event_log_dir:
        local_dir = cfg.event_log_dir.removeprefix("file://")
        Path(local_dir).mkdir(parents=True, exist_ok=True)
        builder = builder.config("spark.eventLog.enabled", "true").config(
            "spark.eventLog.dir", cfg.event_log_dir
        )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    logger.info("Spark %s started (master=%s, app=%s)", spark.version, cfg.master, cfg.app_name)
    return spark
