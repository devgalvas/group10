"""Build the dev sample of one real AIS day (AIS-15).

Keeps every row within --radius-km of a port centre in ref/ports.csv, plus a seeded random
--rate share of the rest, and writes aisdk-<date>-sample.zip into the dev landing folder.

    python scripts/make_dev_sample.py /data/ais/prod/landing/date=2026-09-01/aisdk-2026-09-01.zip
"""

from __future__ import annotations

import argparse
import random
import re
import sys
import time
from collections import Counter
from collections.abc import Iterator
from datetime import date
from pathlib import Path

from ais_pipeline.config import load_settings
from ais_pipeline.geo import Port, load_ports, nearest_port
from ais_pipeline.ingest.bronze import sanitize_name
from ais_pipeline.ingest.raw_csv import open_raw_csv, write_csv_zip
from ais_pipeline.paths import landing_path

DEFAULT_RADIUS_KM = 25.0
DEFAULT_RATE = 0.02
DEFAULT_SEED = 535
_DATE_IN_NAME = re.compile(r"(\d{4}-\d{2}-\d{2})")


def sample_rows(
    rows: Iterator[list[str]],
    header: list[str],
    ports: list[Port],
    radius_km: float,
    rate: float,
    seed: int,
    counts: Counter[str],
) -> Iterator[list[str]]:
    """Yield the rows to keep; fill `counts` with total / near_<port> / random / kept."""
    idx = {sanitize_name(h): i for i, h in enumerate(header)}
    i_lat, i_lon = idx["latitude"], idx["longitude"]
    rng = random.Random(seed)
    # Cheap bounding box (radius in degrees, generous in lon) before the haversine call.
    pad_lat = radius_km / 111.0
    pad_lon = radius_km / 60.0
    lat_lo = min(p.lat for p in ports) - pad_lat
    lat_hi = max(p.lat for p in ports) + pad_lat
    lon_lo = min(p.lon for p in ports) - pad_lon
    lon_hi = max(p.lon for p in ports) + pad_lon

    for row in rows:
        counts["total"] += 1
        near = None
        try:
            lat, lon = float(row[i_lat]), float(row[i_lon])
        except (IndexError, ValueError):
            lat = lon = None
        if lat is not None and lat_lo <= lat <= lat_hi and lon_lo <= lon <= lon_hi:
            port, dist = nearest_port(lat, lon, ports)
            if dist <= radius_km:
                near = port.port_id
        if near is not None:
            counts[f"near_{near}"] += 1
        elif rng.random() < rate:
            counts["random"] += 1
        else:
            continue
        counts["kept"] += 1
        yield row


def source_date(zip_path: Path) -> date:
    m = _DATE_IN_NAME.search(zip_path.name)
    if not m:
        raise SystemExit(f"cannot infer the date from {zip_path.name}; pass --date")
    return date.fromisoformat(m.group(1))


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    ap.add_argument("zip", type=Path, help="real daily DMA zip")
    ap.add_argument("--date", type=date.fromisoformat, help="source date (default: from the name)")
    ap.add_argument("--env", default="dev", help="config whose landing folder receives the sample")
    ap.add_argument("--out-dir", type=Path, help="override the output folder")
    ap.add_argument("--radius-km", type=float, default=DEFAULT_RADIUS_KM)
    ap.add_argument("--rate", type=float, default=DEFAULT_RATE)
    ap.add_argument("--seed", type=int, default=DEFAULT_SEED)
    args = ap.parse_args(argv)

    day = args.date or source_date(args.zip)
    settings = load_settings(args.env)
    out_dir = args.out_dir or landing_path(settings, day)
    out_name = f"aisdk-{day.isoformat()}-sample.zip"
    if settings.source.sample_file and settings.source.sample_file != out_name:
        print(
            f"warning: config/{args.env}.yaml sample_file is {settings.source.sample_file!r}, "
            f"not {out_name!r}; Bronze will not pick this file up",
            file=sys.stderr,
        )
    ports = load_ports(settings.refs.ports)

    counts: Counter[str] = Counter()
    t0 = time.monotonic()
    with open_raw_csv(args.zip) as (member, header, rows):
        kept = sample_rows(rows, header, ports, args.radius_km, args.rate, args.seed, counts)
        out_member = member.removesuffix(".csv") + "-sample.csv"
        write_csv_zip(out_dir / out_name, out_member, header, kept)
    wall = time.monotonic() - t0

    total = counts["total"] or 1
    print(f"source      {args.zip}")
    print(f"output      {out_dir / out_name}")
    print(
        f"rule        within {args.radius_km} km of {[p.port_id for p in ports]} "
        f"+ {args.rate:.1%} random of the rest (seed {args.seed})"
    )
    print(f"rows_total  {counts['total']:>12,}")
    for p in ports:
        print(f"near_{p.port_id:<6} {counts[f'near_{p.port_id}']:>12,}")
    print(f"random      {counts['random']:>12,}")
    print(f"rows_kept   {counts['kept']:>12,}  ({counts['kept'] / total:.2%})")
    print(f"wall_s      {wall:>12.1f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
