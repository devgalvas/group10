"""Single place for lake and landing path logic."""

from __future__ import annotations

from datetime import date
from pathlib import Path

from ais_pipeline.config import Settings

LAYERS = ("bronze", "silver", "gold", "ops")


def table_path(settings: Settings, layer: str, name: str) -> Path:
    """Return ${lake_dir}/<layer>/<name>, e.g. .../lake/silver/vessel_position."""
    if layer not in LAYERS:
        raise ValueError(f"layer must be one of {LAYERS}, got {layer!r}")
    return settings.lake_dir / layer / name


def landing_path(settings: Settings, day: date) -> Path:
    """Return the landing folder for one source date: ${landing_dir}/date=YYYY-MM-DD."""
    return settings.landing_dir / f"date={day.isoformat()}"
