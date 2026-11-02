import zipfile
from datetime import date

import pytest
from pyspark.sql.types import StringType

from ais_pipeline.ingest.bronze import ingest_day
from ais_pipeline.paths import table_path

pytestmark = pytest.mark.integration

DAY = date(2026, 9, 1)
HEADER = (
    "# Timestamp,Type of mobile,MMSI,Latitude,Longitude,Navigational status,ROT,SOG,COG,"
    "Heading,IMO,Callsign,Name,Ship type,Cargo type,Width,Length,"
    "Type of position fixing device,Draught,Destination,ETA,Data source type,A,B,C,D"
)
GOOD = (
    "01/09/2026 00:00:{s:02d},Class A,219787000,57.602542,9.954098,Under way using engine,,7.1,"
    "319.5,307,Unknown,Unknown,,Undefined,,,,Undefined,,Unknown,,AIS,,,,"
)
BAD = "01/09/2026 00:00:59,Class A,219787000,too,few,fields"


def _write_landing(settings, rows: list[str]) -> None:
    folder = settings.landing_dir / f"date={DAY.isoformat()}"
    folder.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(folder / "aisdk-2026-09-01.zip", "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("aisdk-2026-09-01.csv", "\n".join([HEADER, *rows]) + "\n")


def _bronze(spark, settings):
    return spark.read.format("delta").load(str(table_path(settings, "bronze", "ais_raw")))


def test_ingest_counts_types_and_lineage(spark, settings):
    _write_landing(settings, [GOOD.format(s=i) for i in range(10)] + [BAD])
    result = ingest_day(spark, settings, DAY, run_id="test-run")

    assert result.rows == 11
    assert result.corrupt_rows == 1
    df = _bronze(spark, settings)
    source_cols = [f for f in df.schema.fields if not f.name.startswith("_")]
    assert len(source_cols) == 26
    assert all(isinstance(f.dataType, StringType) for f in source_cols)
    assert df.columns[0] == "timestamp"

    lineage = ["_source_file", "_file_sha256", "_source_date", "_ingest_ts", "_run_id"]
    assert df.where(" OR ".join(f"{c} IS NULL" for c in lineage)).count() == 0
    row = df.where("_corrupt_record IS NULL").first()
    assert row["_source_file"] == "aisdk-2026-09-01.zip"
    assert row["mmsi"] == "219787000"
    assert row["latitude"] == "57.602542"


def test_extract_dir_is_removed(spark, settings):
    _write_landing(settings, [GOOD.format(s=0)])
    ingest_day(spark, settings, DAY, run_id="r1")
    assert not any((settings.landing_dir / "_extract").glob("*"))


def test_rerun_replaces_partition(spark, settings):
    _write_landing(settings, [GOOD.format(s=i) for i in range(5)])
    ingest_day(spark, settings, DAY, run_id="r1")
    ingest_day(spark, settings, DAY, run_id="r2")
    df = _bronze(spark, settings)
    assert df.count() == 5
    assert {r["_run_id"] for r in df.select("_run_id").distinct().collect()} == {"r2"}


def test_missing_landing_file_raises(spark, settings):
    with pytest.raises(FileNotFoundError, match="run download first"):
        ingest_day(spark, settings, DAY, run_id="r1")
