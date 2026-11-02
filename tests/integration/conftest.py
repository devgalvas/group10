import pytest

from ais_pipeline.config import load_settings
from ais_pipeline.spark import build_spark


@pytest.fixture
def settings(monkeypatch, tmp_path):
    monkeypatch.setenv("AIS_DATA_ROOT", str(tmp_path))
    return load_settings("ci")


@pytest.fixture(scope="session")
def spark():
    """One local[2] SparkSession with Delta for all integration tests (starting one is slow)."""
    session = build_spark(load_settings("ci"))
    # Tiny test tables: the default 50 Delta snapshot partitions only add scheduling overhead.
    session.conf.set("spark.databricks.delta.snapshotPartitions", "2")
    yield session
    session.stop()
