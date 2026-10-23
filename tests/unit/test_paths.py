from datetime import date

import pytest

from ais_pipeline.config import load_settings
from ais_pipeline.paths import landing_path, table_path


@pytest.fixture
def settings(monkeypatch, tmp_path):
    monkeypatch.setenv("AIS_DATA_ROOT", str(tmp_path))
    return load_settings("ci")


def test_table_path(settings, tmp_path):
    path = table_path(settings, "silver", "vessel_position")
    assert path == tmp_path / "lake" / "silver" / "vessel_position"


def test_table_path_rejects_unknown_layer(settings):
    with pytest.raises(ValueError, match="layer"):
        table_path(settings, "platinum", "x")


def test_landing_path(settings, tmp_path):
    assert landing_path(settings, date(2026, 9, 1)) == tmp_path / "landing" / "date=2026-09-01"
