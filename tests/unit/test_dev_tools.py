"""Tests for scripts/make_fixture.py and scripts/make_dev_sample.py on a synthetic DMA zip."""

from __future__ import annotations

import csv
import importlib.util
import io
import json
import sys
import zipfile
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from ais_pipeline.geo import haversine_km, load_ports, nearest_port
from ais_pipeline.ingest.bronze import check_header, load_contract
from ais_pipeline.ingest.raw_csv import open_raw_csv

REPO_ROOT = Path(__file__).resolve().parents[2]
CONTRACT = load_contract()
HEADER = ["# Timestamp", *[c["source_name"] for c in CONTRACT["columns"][1:]]]
COL = {name: i for i, name in enumerate(HEADER)}
TS_FMT = "%d/%m/%Y %H:%M:%S"
DAY = datetime(2026, 9, 1)


def _load_script(name: str):
    spec = importlib.util.spec_from_file_location(name, REPO_ROOT / "scripts" / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module  # dataclasses look the module up while the class is built
    spec.loader.exec_module(module)
    return module


make_fixture = _load_script("make_fixture")
make_dev_sample = _load_script("make_dev_sample")


def _row(ts: datetime, mobile: str, mmsi: str, lat: float, lon: float, sog: float, name: str):
    r = [""] * len(HEADER)
    r[COL["# Timestamp"]] = ts.strftime(TS_FMT)
    r[COL["Type of mobile"]] = mobile
    r[COL["MMSI"]] = mmsi
    r[COL["Latitude"]] = f"{lat:.6f}"
    r[COL["Longitude"]] = f"{lon:.6f}"
    r[COL["SOG"]] = f"{sog:.1f}"
    r[COL["COG"]] = "90.0"
    r[COL["Heading"]] = "90"
    r[COL["Name"]] = name
    return r


def _synthetic_day() -> list[list[str]]:
    rows = []
    for k in range(40):  # every 2 min
        ts = DAY + timedelta(minutes=2 * k)
        rows.append(_row(ts, "Class A", "219000001", 55.466, 8.430, 0.0, "MOORED"))
        rows.append(_row(ts, "Class A", "219000002", 57.0 + k * 0.004, 11.0, 12.0, "FAST ONE"))
        # fastest coherent track but no name: must not be picked (rename would be null -> value)
        rows.append(_row(ts, "Class A", "219000005", 55.0, 15.0 - k * 0.01, 25.0, ""))
        rows.append(_row(ts, "Class A", "219000003", 54.5, 11.5 + k * 0.005, 9.0, "FAST TWO"))
        rows.append(_row(ts, "Class A", "219000004", 56.0, 4.0, 0.1, "DRIFTER"))
        # shared default MMSI: two transmitters far apart, highest SOG, must not be picked
        rows.append(_row(ts, "Class A", "226000000", 43.0 + 7 * (k % 2), -1.0, 30.0, "DEFAULT"))
        # dirty rows the selection must skip
        rows.append(_row(ts, "Class A", "111", 56.0, 4.0, 3.0, "BAD MMSI"))
        rows.append(_row(ts, "Class A", "219000009", 91.0, 181.0, 3.0, "SENTINEL"))
    for k in range(300):
        ts = DAY + timedelta(seconds=17 * k)
        rows.append(_row(ts, "Class B", f"2570{k:05d}", 58.0, 6.0 + k * 0.001, 4.0, f"B{k}"))
    for k in range(3):
        rows.append(
            _row(DAY + timedelta(minutes=k), "Base Station", f"00219{k:04d}", 55.0, 9.0, 0, "")
        )
    rows.append(_row(DAY, "AtoN", "992190001", 55.1, 9.1, 0, "BUOY"))
    # Near Aarhus but not in the port radius, and far offshore, for the dev sample.
    rows.append(_row(DAY, "Class A", "219000010", 56.25, 10.23, 5.0, "NEAR AAR"))
    rows.append(_row(DAY, "Class A", "219000011", 60.0, 2.0, 5.0, "FAR"))
    rows.sort(key=lambda r: datetime.strptime(r[0], TS_FMT))
    return rows


@pytest.fixture
def day_zip(tmp_path: Path) -> Path:
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\r\n")
    w.writerow(HEADER)
    w.writerows(_synthetic_day())
    path = tmp_path / "aisdk-2026-09-01.zip"
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("aisdk-2026-09-01.csv", buf.getvalue())
    return path


# --- geo -------------------------------------------------------------------------------------


def test_haversine_known_distance():
    # Esbjerg -> Copenhagen centres, about 264 km.
    assert haversine_km(55.466, 8.430, 55.705, 12.605) == pytest.approx(264, abs=10)
    assert haversine_km(55.0, 9.0, 55.0, 9.0) == 0


def test_ports_ref_and_nearest():
    ports = load_ports("ref/ports.csv")
    assert [p.port_id for p in ports] == ["DKEBJ", "DKAAR", "DKCPH"]
    port, dist = nearest_port(56.151, 10.231, ports)
    assert port.port_id == "DKAAR" and dist < 0.5


# --- make_fixture ----------------------------------------------------------------------------


@pytest.fixture
def fixture_out(day_zip: Path, tmp_path: Path):
    out = tmp_path / "fixtures"
    assert make_fixture.main([str(day_zip), "--out-dir", str(out), "--filler", "120"]) == 0
    meta = json.loads((out / "ais_tiny_defects.json").read_text())
    with (out / "ais_tiny.csv").open(newline="") as fh:
        lines = list(csv.reader(fh))
    return out, meta, lines


def test_fixture_header_passes_contract(fixture_out):
    _out, _meta, lines = fixture_out
    assert lines[0] == HEADER
    assert check_header(lines[0], CONTRACT).missing_optional == []


def test_fixture_picks_expected_vessels(fixture_out):
    _out, meta, lines = fixture_out
    assert meta["vessels"] == {
        "in_port": "219000001",
        "moving_a": "219000002",
        "moving_b": "219000003",
    }
    per_mmsi = Counter(r[COL["MMSI"]] for r in lines[1:])
    for mmsi in meta["vessels"].values():
        assert per_mmsi[mmsi] >= 20
    mobiles = Counter(r[COL["Type of mobile"]] for r in lines[1:])
    assert mobiles["Base Station"] + mobiles["AtoN"] >= 2


def test_fixture_has_no_unplanned_dirty_rows(fixture_out):
    _out, _meta, lines = fixture_out
    mmsis = {r[COL["MMSI"]] for r in lines[1:]}
    assert "111" not in mmsis and "219000009" not in mmsis


def test_fixture_defect_lines_match_content(fixture_out):
    _out, meta, lines = fixture_out
    by_defect: dict[str, list[dict]] = {}
    for d in meta["defects"]:
        by_defect.setdefault(d["defect"], []).append(d)
        row = lines[d["line"] - 1]
        assert row[COL["MMSI"]] == d["mmsi"]
        assert row[COL["# Timestamp"]] == d["timestamp"]

    def row(defect: str, i: int = 0) -> list[str]:
        return lines[by_defect[defect][i]["line"] - 1]

    def prev(defect: str, i: int = 0) -> list[str]:
        return lines[by_defect[defect][i]["line"] - 2]

    assert len(by_defect["exact_duplicate"]) == 2
    for i in range(2):
        assert row("exact_duplicate", i) == prev("exact_duplicate", i)
    conflict, before = row("ts_conflict"), prev("ts_conflict")
    assert conflict[:3] == before[:3] and conflict[COL["SOG"]] != before[COL["SOG"]]
    assert row("position_sentinel")[COL["Latitude"]] == "91.000000"
    assert row("position_sentinel")[COL["Longitude"]] == "181.000000"
    assert row("bad_mmsi")[COL["MMSI"]] == "12345"
    with pytest.raises(ValueError):
        datetime.strptime(row("bad_timestamp")[0], TS_FMT)
    assert row("sog_sentinel")[COL["SOG"]] == "102.3"
    assert row("heading_sentinel")[COL["Heading"]] == "511"
    jump_line = by_defect["position_jump"][0]["line"]
    jump = lines[jump_line - 1]
    same_vessel = [r for r in lines[1 : jump_line - 1] if r[COL["MMSI"]] == jump[COL["MMSI"]]]
    assert float(jump[COL["Latitude"]]) - float(same_vessel[-1][COL["Latitude"]]) > 0.4
    assert row("ts_outside_file_day")[0].startswith("31/08/2026")
    names = {lines[d["line"] - 1][COL["Name"]] for d in by_defect["name_change"]}
    assert names == {"FAST TWO II"} and len(by_defect["name_change"]) == 5
    assert len(by_defect["out_of_scope_mobile"]) == 3


def test_fixture_only_planted_duplicates_and_conflicts(fixture_out):
    _out, _meta, lines = fixture_out
    exact = Counter(tuple(r) for r in lines[1:])
    assert sum(n - 1 for n in exact.values()) == 2
    distinct = set(map(tuple, lines[1:]))
    keys = Counter((r[COL["MMSI"]], r[0]) for r in distinct)
    assert sum(n > 1 for n in keys.values()) == 1  # only the planted TS_CONFLICT


def test_fixture_is_deterministic_and_small(day_zip: Path, tmp_path: Path, fixture_out):
    out, meta, _ = fixture_out
    out2 = tmp_path / "again"
    make_fixture.main([str(day_zip), "--out-dir", str(out2), "--filler", "120"])
    assert (out / "ais_tiny.csv").read_bytes() == (out2 / "ais_tiny.csv").read_bytes()
    assert meta["bytes"] < make_fixture.MAX_FIXTURE_BYTES
    readme = (out / "README.md").read_text()
    assert all(f"| {d['line']} |" in readme for d in meta["defects"])


# --- make_dev_sample -------------------------------------------------------------------------


def _sample(day_zip: Path, out: Path, rate: str) -> list[list[str]]:
    rc = make_dev_sample.main([str(day_zip), "--env", "ci", "--out-dir", str(out), "--rate", rate])
    assert rc == 0
    with open_raw_csv(out / "aisdk-2026-09-01-sample.zip") as (_m, header, rows):
        assert header == HEADER
        return list(rows)


def test_dev_sample_keeps_rows_near_ports_only_at_rate_zero(day_zip: Path, tmp_path: Path):
    rows = _sample(day_zip, tmp_path / "s0", "0")
    mmsis = {r[COL["MMSI"]] for r in rows}
    assert mmsis == {"219000001", "219000010"}  # Esbjerg berth + ~11 km from Aarhus
    assert len(rows) == 41


def test_dev_sample_keeps_everything_at_rate_one(day_zip: Path, tmp_path: Path):
    rows = _sample(day_zip, tmp_path / "s1", "1")
    assert len(rows) == len(_synthetic_day())


def test_dev_sample_is_seeded(day_zip: Path, tmp_path: Path):
    a = _sample(day_zip, tmp_path / "a", "0.3")
    b = _sample(day_zip, tmp_path / "b", "0.3")
    assert a == b and 41 < len(a) < len(_synthetic_day())
