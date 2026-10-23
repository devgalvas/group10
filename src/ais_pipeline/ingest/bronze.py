"""Bronze ingestion: raw schema contract, header check and the daily zip → Delta load."""

from __future__ import annotations

import csv
import hashlib
import io
import json
import logging
import re
import shutil
import time
import zipfile
from dataclasses import dataclass, field
from datetime import date
from importlib import resources
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from ais_pipeline.config import Settings
from ais_pipeline.delta_io import write_partition
from ais_pipeline.ingest.download import file_name, sha256_file
from ais_pipeline.paths import landing_path, table_path

logger = logging.getLogger(__name__)

CONTRACT_VERSION = "v1"
CORRUPT_COL = "_corrupt_record"


class SchemaContractError(ValueError):
    """Raised when a raw file header breaks the contract (e.g. a required column is missing)."""


def sanitize_name(name: str) -> str:
    """Bronze column name: lowercase, strip '#', non-alphanumerics to '_', collapse '_'."""
    name = name.strip().lower().replace("#", "")
    name = re.sub(r"[^a-z0-9]+", "_", name)
    return re.sub(r"_+", "_", name).strip("_")


def load_contract(version: str = CONTRACT_VERSION) -> dict[str, Any]:
    """Load contracts/ais_raw_<version>.json shipped inside the package."""
    text = resources.files("ais_pipeline.contracts").joinpath(f"ais_raw_{version}.json").read_text()
    return json.loads(text)


def read_header(zip_path: Path) -> tuple[str, list[str]]:
    """Return (CSV member name, header columns) of the single CSV in a DMA zip, without Spark."""
    with zipfile.ZipFile(zip_path) as zf:
        members = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if len(members) != 1:
            raise SchemaContractError(f"{zip_path.name}: expected 1 CSV member, found {members}")
        with zf.open(members[0]) as raw:
            line = io.TextIOWrapper(raw, encoding="utf-8-sig", newline="").readline()
    if not line.strip():
        raise SchemaContractError(f"{zip_path.name}: empty header line")
    return members[0], next(csv.reader([line]))


@dataclass(frozen=True)
class HeaderReport:
    """Outcome of comparing a file header with the contract."""

    status: str  # "ok" | "extra"
    contract_version: str
    header_hash: str
    bronze_columns: list[str]  # in file order; contract names first-class, extras sanitised
    extra_columns: list[str] = field(default_factory=list)
    missing_optional: list[str] = field(default_factory=list)

    @property
    def bronze_schema(self) -> StructType:
        """All-string schema in file order, used to read the CSV positionally."""
        return StructType([StructField(c, StringType(), True) for c in self.bronze_columns])


def check_header(header: list[str], contract: dict[str, Any]) -> HeaderReport:
    """Map a header to the contract by name; raise if a required column is missing.

    Columns are matched by their sanitised name, so a reordered file still maps correctly.
    """
    by_bronze = {c["bronze_name"]: c for c in contract["columns"]}
    for c in contract["columns"]:
        for alias in c.get("aliases", []):
            by_bronze.setdefault(sanitize_name(alias), c)

    bronze_columns: list[str] = []
    extra: list[str] = []
    for source in header:
        col = by_bronze.get(sanitize_name(source))
        name = col["bronze_name"] if col else sanitize_name(source)
        if not col:
            extra.append(source)
        if not name or name in bronze_columns:
            raise SchemaContractError(f"header column {source!r} maps to duplicate name {name!r}")
        bronze_columns.append(name)

    present = set(bronze_columns)
    missing = [c["source_name"] for c in contract["columns"] if c["bronze_name"] not in present]
    missing_required = [
        c["source_name"]
        for c in contract["columns"]
        if c["required"] and c["bronze_name"] not in present
    ]
    if missing_required:
        raise SchemaContractError(f"missing required columns: {', '.join(missing_required)}")
    if extra:
        logger.warning("header has columns not in contract %s: %s", contract["version"], extra)
    if missing:
        logger.warning("header is missing optional columns: %s", missing)

    return HeaderReport(
        status="extra" if extra else "ok",
        contract_version=contract["version"],
        header_hash=hashlib.sha256(",".join(header).encode()).hexdigest(),
        bronze_columns=bronze_columns,
        extra_columns=extra,
        missing_optional=missing,
    )


def landing_zip(settings: Settings, day: date) -> Path:
    """Path of the zip to ingest for a day: the dev sample if configured, else the daily file."""
    name = settings.source.sample_file or file_name(settings, day)
    return landing_path(settings, day) / name


def extract_csv(zip_path: Path, member: str, dest_dir: Path) -> Path:
    """Extract one CSV member into dest_dir (replacing leftovers) and return its path."""
    shutil.rmtree(dest_dir, ignore_errors=True)
    dest_dir.mkdir(parents=True)
    with zipfile.ZipFile(zip_path) as zf:
        return Path(zf.extract(member, dest_dir))


def read_raw_csv(spark: SparkSession, csv_path: Path, report: HeaderReport) -> DataFrame:
    """Read the CSV with the contract's all-string schema, positionally, in PERMISSIVE mode.

    The header was already validated by check_header, so the schema (sanitised names in
    file order) is applied by position. Rows with the wrong number of fields keep their raw
    line in `_corrupt_record`; well-formed rows have it null.
    """
    schema = StructType([*report.bronze_schema.fields, StructField(CORRUPT_COL, StringType())])
    return spark.read.csv(
        str(csv_path),
        schema=schema,
        header=True,
        sep=",",
        mode="PERMISSIVE",
        columnNameOfCorruptRecord=CORRUPT_COL,
        enforceSchema=True,
    )


def add_lineage(
    df: DataFrame, source_file: str, file_sha256: str, source_date: date, run_id: str
) -> DataFrame:
    """Append the Bronze lineage columns; source values are left untouched."""
    return (
        df.withColumn("_source_file", F.lit(source_file))
        .withColumn("_file_sha256", F.lit(file_sha256))
        .withColumn("_source_date", F.lit(source_date.isoformat()).cast("date"))
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_run_id", F.lit(run_id))
    )


@dataclass(frozen=True)
class BronzeResult:
    source_date: date
    file_name: str
    file_sha256: str
    file_bytes: int
    header: HeaderReport
    rows: int
    corrupt_rows: int
    seconds: float


def ingest_day(
    spark: SparkSession, settings: Settings, day: date, run_id: str, file_sha256: str | None = None
) -> BronzeResult:
    """Load one landing zip into the `_source_date = day` partition of bronze.ais_raw."""
    started = time.monotonic()
    zip_path = landing_zip(settings, day)
    if not zip_path.is_file():
        raise FileNotFoundError(f"no landing file for {day}: {zip_path} (run download first)")

    member, header = read_header(zip_path)
    report = check_header(header, load_contract())
    sha = file_sha256 or sha256_file(zip_path)
    extract_dir = settings.landing_dir / "_extract" / day.isoformat()
    bronze_path = table_path(settings, "bronze", "ais_raw")

    try:
        csv_path = extract_csv(zip_path, member, extract_dir)
        raw = read_raw_csv(spark, csv_path, report)
        df = add_lineage(raw, zip_path.name, sha, day, run_id)
        write_partition(df, bronze_path, "_source_date", day)
    finally:
        shutil.rmtree(extract_dir, ignore_errors=True)

    # Count from the Delta partition (columnar, cheap) instead of re-scanning the CSV.
    written = (
        spark.read.format("delta")
        .load(str(bronze_path))
        .where(F.col("_source_date") == F.lit(day.isoformat()).cast("date"))
    )
    counts = written.agg(
        F.count(F.lit(1)).alias("rows"), F.count(CORRUPT_COL).alias("corrupt")
    ).first()
    result = BronzeResult(
        source_date=day,
        file_name=zip_path.name,
        file_sha256=sha,
        file_bytes=zip_path.stat().st_size,
        header=report,
        rows=counts["rows"],
        corrupt_rows=counts["corrupt"],
        seconds=time.monotonic() - started,
    )
    logger.info(
        "bronze %s: %d rows (%d corrupt) from %s in %.1f s",
        day,
        result.rows,
        result.corrupt_rows,
        result.file_name,
        result.seconds,
    )
    return result
