from pathlib import Path

import pytest
import yaml

from ais_pipeline.config import ConfigError, load_settings, parse_settings

CONFIG_DIR = Path(__file__).resolve().parents[2] / "config"


def _raw(env: str = "ci") -> dict:
    with (CONFIG_DIR / f"{env}.yaml").open() as fh:
        return yaml.safe_load(fh)


@pytest.mark.parametrize("env", ["dev", "prod", "ci"])
def test_all_env_files_load(env, monkeypatch):
    monkeypatch.delenv("AIS_DATA_ROOT", raising=False)
    settings = load_settings(env)
    assert settings.env == env
    assert settings.spark.app_name == f"ais-pipeline-{env}"


def test_data_root_is_interpolated(monkeypatch):
    monkeypatch.delenv("AIS_DATA_ROOT", raising=False)
    settings = load_settings("dev")
    assert settings.landing_dir == Path("/data/ais/dev/landing")
    assert settings.lake_dir == Path("/data/ais/dev/lake")


def test_env_var_overrides_data_root(monkeypatch, tmp_path):
    monkeypatch.setenv("AIS_DATA_ROOT", str(tmp_path))
    settings = load_settings("ci")
    assert settings.data_root == tmp_path
    assert settings.lake_dir == tmp_path / "lake"


def test_window_dates_are_strings(monkeypatch):
    monkeypatch.delenv("AIS_DATA_ROOT", raising=False)
    raw = _raw("prod")
    raw["window"] = {"start": "2026-09-01", "end": "2026-09-28"}
    settings = parse_settings(raw)
    assert settings.window.start == "2026-09-01"


def test_missing_key_raises():
    raw = _raw()
    del raw["spark"]
    with pytest.raises(ConfigError, match="spark"):
        parse_settings(raw)


def test_unknown_placeholder_raises():
    raw = _raw()
    raw["lake_dir"] = "${nope}/lake"
    with pytest.raises(ConfigError, match="nope"):
        parse_settings(raw)


def test_unknown_section_key_raises():
    raw = _raw()
    raw["spark"] = {**raw["spark"], "typo_key": 1}
    with pytest.raises(ConfigError, match="invalid config section"):
        parse_settings(raw)


def test_unknown_env_raises():
    with pytest.raises(ConfigError, match="env must be"):
        load_settings("staging")
