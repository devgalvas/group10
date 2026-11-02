import zipfile

import pytest

from ais_pipeline.ingest.bronze import (
    SchemaContractError,
    check_header,
    load_contract,
    read_header,
    sanitize_name,
)

# Header of aisdk-2026-09-01.csv as published by DMA.
REAL_HEADER = (
    "# Timestamp,Type of mobile,MMSI,Latitude,Longitude,Navigational status,ROT,SOG,COG,"
    "Heading,IMO,Callsign,Name,Ship type,Cargo type,Width,Length,"
    "Type of position fixing device,Draught,Destination,ETA,Data source type,A,B,C,D"
).split(",")


@pytest.fixture(scope="module")
def contract():
    return load_contract()


@pytest.mark.parametrize(
    ("source", "expected"),
    [
        ("Type of mobile", "type_of_mobile"),
        ("# Timestamp", "timestamp"),
        ("Type of position fixing device", "type_of_position_fixing_device"),
        ("  Ship  type ", "ship_type"),
        ("A", "a"),
    ],
)
def test_sanitize_name(source, expected):
    assert sanitize_name(source) == expected


def test_contract_has_26_columns_and_5_required(contract):
    assert len(contract["columns"]) == 26
    assert sum(c["required"] for c in contract["columns"]) == 5


def test_exact_header_is_ok(contract):
    report = check_header(REAL_HEADER, contract)
    assert report.status == "ok"
    assert report.bronze_columns[:3] == ["timestamp", "type_of_mobile", "mmsi"]
    assert report.contract_version == "v1"
    assert len(report.bronze_schema.fields) == 26


def test_timestamp_alias_without_hash(contract):
    header = ["Timestamp", *REAL_HEADER[1:]]
    report = check_header(header, contract)
    assert report.status == "ok"
    assert report.bronze_columns[0] == "timestamp"


def test_extra_column_is_kept_with_warning(contract, caplog):
    report = check_header([*REAL_HEADER, "New Field"], contract)
    assert report.status == "extra"
    assert report.extra_columns == ["New Field"]
    assert report.bronze_columns[-1] == "new_field"
    assert "not in contract" in caplog.text


def test_missing_required_raises(contract):
    header = [c for c in REAL_HEADER if c != "MMSI"]
    with pytest.raises(SchemaContractError, match="MMSI"):
        check_header(header, contract)


def test_missing_optional_is_reported(contract):
    header = [c for c in REAL_HEADER if c != "ETA"]
    report = check_header(header, contract)
    assert report.missing_optional == ["ETA"]


def test_reordered_columns_are_mapped_by_name(contract):
    header = list(reversed(REAL_HEADER))
    report = check_header(header, contract)
    assert report.bronze_columns[0] == "d"
    assert report.bronze_columns[-1] == "timestamp"
    assert report.header_hash != check_header(REAL_HEADER, contract).header_hash


def test_duplicate_column_raises(contract):
    with pytest.raises(SchemaContractError, match="duplicate"):
        check_header([*REAL_HEADER, "mmsi"], contract)


def test_read_header_from_zip(tmp_path):
    zip_path = tmp_path / "aisdk-2026-09-01.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("aisdk-2026-09-01.csv", ",".join(REAL_HEADER) + "\n01/09/2026 00:00:00,x\n")
    member, header = read_header(zip_path)
    assert member == "aisdk-2026-09-01.csv"
    assert header == REAL_HEADER


def test_read_header_rejects_multi_member_zip(tmp_path):
    zip_path = tmp_path / "bad.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("a.csv", "x\n")
        zf.writestr("b.csv", "y\n")
    with pytest.raises(SchemaContractError, match="expected 1 CSV"):
        read_header(zip_path)
