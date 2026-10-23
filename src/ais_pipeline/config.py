"""Load environment settings from config/<env>.yaml."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

ENVS = ("dev", "prod", "ci")
DATA_ROOT_ENV_VAR = "AIS_DATA_ROOT"
CONFIG_DIR_ENV_VAR = "AIS_CONFIG_DIR"

_REQUIRED_KEYS = (
    "env",
    "data_root",
    "landing_dir",
    "lake_dir",
    "source",
    "window",
    "spark",
    "quality",
    "features",
    "refs",
)
_PLACEHOLDER = re.compile(r"\$\{([a-z_]+)\}")


class ConfigError(ValueError):
    """Raised when a config file is missing or invalid."""


@dataclass(frozen=True)
class SourceSettings:
    base_url: str
    file_pattern: str
    sample_file: str | None = None


@dataclass(frozen=True)
class WindowSettings:
    start: str
    end: str


@dataclass(frozen=True)
class SparkSettings:
    master: str
    app_name: str
    driver_memory: str
    shuffle_partitions: int
    event_log_dir: str | None = None


@dataclass(frozen=True)
class QualitySettings:
    max_reject_rate: float
    min_rows_vs_median: float
    min_hours_present: int


@dataclass(frozen=True)
class FeatureSettings:
    jump_speed_kn: float
    port_call_gap_h: float
    min_port_call_min: float
    anchor_max_sog_kn: float


@dataclass(frozen=True)
class RefSettings:
    ports: str
    ship_types: str


@dataclass(frozen=True)
class Settings:
    env: str
    data_root: Path
    landing_dir: Path
    lake_dir: Path
    source: SourceSettings
    window: WindowSettings
    spark: SparkSettings
    quality: QualitySettings
    features: FeatureSettings
    refs: RefSettings


def config_dir() -> Path:
    """Return the config folder: $AIS_CONFIG_DIR, ./config, or the repo's config/."""
    if override := os.environ.get(CONFIG_DIR_ENV_VAR):
        return Path(override)
    cwd_config = Path.cwd() / "config"
    if cwd_config.is_dir():
        return cwd_config
    return Path(__file__).resolve().parents[2] / "config"


def _interpolate(value: Any, scalars: dict[str, str]) -> Any:
    if isinstance(value, dict):
        return {k: _interpolate(v, scalars) for k, v in value.items()}
    if isinstance(value, str):

        def repl(match: re.Match[str]) -> str:
            key = match.group(1)
            if key not in scalars:
                raise ConfigError(f"unknown placeholder ${{{key}}}")
            return scalars[key]

        return _PLACEHOLDER.sub(repl, value)
    return value


def parse_settings(raw: dict[str, Any]) -> Settings:
    """Validate a raw config mapping and build Settings."""
    missing = [k for k in _REQUIRED_KEYS if k not in raw]
    if missing:
        raise ConfigError(f"missing config keys: {', '.join(missing)}")
    if raw["env"] not in ENVS:
        raise ConfigError(f"env must be one of {ENVS}, got {raw['env']!r}")

    raw = dict(raw)
    if override := os.environ.get(DATA_ROOT_ENV_VAR):
        raw["data_root"] = override
    scalars = {"data_root": str(raw["data_root"])}
    raw = _interpolate(raw, scalars)

    try:
        return Settings(
            env=raw["env"],
            data_root=Path(raw["data_root"]),
            landing_dir=Path(raw["landing_dir"]),
            lake_dir=Path(raw["lake_dir"]),
            source=SourceSettings(**raw["source"]),
            window=WindowSettings(**{k: str(v) for k, v in raw["window"].items()}),
            spark=SparkSettings(**raw["spark"]),
            quality=QualitySettings(**raw["quality"]),
            features=FeatureSettings(**raw["features"]),
            refs=RefSettings(**raw["refs"]),
        )
    except TypeError as exc:
        raise ConfigError(f"invalid config section: {exc}") from exc


def load_settings(env: str) -> Settings:
    """Read config/<env>.yaml, apply overrides and interpolation, and validate it."""
    if env not in ENVS:
        raise ConfigError(f"env must be one of {ENVS}, got {env!r}")
    path = config_dir() / f"{env}.yaml"
    if not path.is_file():
        raise ConfigError(f"config file not found: {path}")
    with path.open() as fh:
        raw = yaml.safe_load(fh) or {}
    return parse_settings(raw)
