"""Bronze ingestion: raw schema contract and header check (Spark-free parts)."""

from __future__ import annotations

import csv
import hashlib
import io
import json
import logging
import re
import zipfile
from dataclasses import dataclass, field
from importlib import resources
from pathlib import Path
from typing import Any

from pyspark.sql.types import StringType, StructField, StructType

logger = logging.getLogger(__name__)

CONTRACT_VERSION = "v1"


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
