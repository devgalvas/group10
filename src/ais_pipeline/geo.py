"""Port reference loading and great-circle distance, shared by scripts and the Silver layer."""

from __future__ import annotations

import csv
import math
from dataclasses import dataclass
from pathlib import Path

EARTH_RADIUS_KM = 6371.0088
REPO_ROOT = Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class Port:
    port_id: str
    name: str
    lat: float
    lon: float
    port_radius_km: float
    anchorage_radius_km: float


def resolve_ref(path: str | Path) -> Path:
    """Resolve a refs path from config: absolute, relative to cwd, or relative to the repo root."""
    p = Path(path)
    if p.is_absolute() or p.exists():
        return p
    return REPO_ROOT / p


def load_ports(path: str | Path) -> list[Port]:
    """Read ref/ports.csv into Port objects."""
    with resolve_ref(path).open(newline="") as fh:
        return [
            Port(
                port_id=r["port_id"],
                name=r["name"],
                lat=float(r["lat"]),
                lon=float(r["lon"]),
                port_radius_km=float(r["port_radius_km"]),
                anchorage_radius_km=float(r["anchorage_radius_km"]),
            )
            for r in csv.DictReader(fh)
        ]


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in km between two WGS84 points."""
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * EARTH_RADIUS_KM * math.asin(math.sqrt(a))


def nearest_port(lat: float, lon: float, ports: list[Port]) -> tuple[Port, float]:
    """Return (nearest port, distance in km)."""
    return min(((p, haversine_km(lat, lon, p.lat, p.lon)) for p in ports), key=lambda t: t[1])
