"""Plain-Python streaming over the raw DMA CSV inside a zip (no Spark), for dev tooling."""

from __future__ import annotations

import csv
import io
import os
import zipfile
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from pathlib import Path

from ais_pipeline.ingest.bronze import SchemaContractError


@contextmanager
def open_raw_csv(zip_path: Path) -> Iterator[tuple[str, list[str], Iterator[list[str]]]]:
    """Yield (CSV member name, header, row iterator) for the single CSV in a DMA zip."""
    with zipfile.ZipFile(zip_path) as zf:
        members = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if len(members) != 1:
            raise SchemaContractError(f"{zip_path.name}: expected 1 CSV member, found {members}")
        with zf.open(members[0]) as raw:
            text = io.TextIOWrapper(raw, encoding="utf-8-sig", newline="")
            reader = csv.reader(text)
            header = next(reader, None)
            if not header:
                raise SchemaContractError(f"{zip_path.name}: empty CSV")
            yield members[0], header, reader


def write_csv_zip(zip_path: Path, member: str, header: list[str], rows: Iterable[list[str]]) -> int:
    """Write rows as a single deflated CSV member, atomically (.part then rename). Returns rows."""
    zip_path.parent.mkdir(parents=True, exist_ok=True)
    part = zip_path.with_name(zip_path.name + ".part")
    n = 0
    try:
        with zipfile.ZipFile(part, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            with zf.open(member, "w", force_zip64=True) as raw:
                text = io.TextIOWrapper(raw, encoding="utf-8", newline="")
                writer = csv.writer(text, lineterminator="\n")
                writer.writerow(header)
                for row in rows:
                    writer.writerow(row)
                    n += 1
                text.flush()
                text.detach()
        os.replace(part, zip_path)
    except BaseException:
        part.unlink(missing_ok=True)
        raise
    return n
