"""Cut tests/fixtures/ais_tiny.csv from one real AIS day and plant known defects (AIS-15).

One streaming pass over the zip (no Spark) picks:
  * one Class A vessel inside a port radius and two moving Class A vessels, each with
    >= 20 positions at least 60 s apart;
  * up to 2 Base Station rows and 1 AtoN row;
  * a seeded random filler of other clean Class A/B rows.
Only clean rows are selected, so the planted defects are the only defects. The script then
writes the CSV, ais_tiny_defects.json (machine-readable) and README.md (the defect table).

    python scripts/make_fixture.py /data/ais/prod/landing/date=2026-09-01/aisdk-2026-09-01.zip
"""

from __future__ import annotations

import argparse
import csv
import json
import random
import re
import statistics
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path

from ais_pipeline.geo import Port, haversine_km, load_ports, nearest_port
from ais_pipeline.ingest.bronze import sanitize_name
from ais_pipeline.ingest.raw_csv import open_raw_csv

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUT_DIR = REPO_ROOT / "tests" / "fixtures"
TS_FMT = "%d/%m/%Y %H:%M:%S"
VESSEL_TYPES = ("Class A", "Class B")
OTHER_TYPES = {"Base Station": 2, "AtoN": 1}
MIN_TRACK = 20
MAX_TRACK = 25
MIN_GAP_S = 60
MAX_TRACKED_VESSELS = 20_000
MIN_MOVING_SOG = 5.0
MAX_IMPLIED_KN = 50.0  # = features.jump_speed_kn; base tracks must not trip POSITION_JUMP
KM_PER_NM = 1.852
DEFAULT_FILLER = 180
DEFAULT_SEED = 535
MAX_FIXTURE_BYTES = 100_000
_MMSI_OK = re.compile(r"^[2-7]\d{8}$")


@dataclass
class Row:
    fields: list[str]
    ts: datetime  # sort key; for the bad-timestamp row this is the source row's time
    note: dict | None = None  # set on planted defects
    seq: int = 0  # input order; a planted row shares its source row's seq
    sub: int = 0  # 1 for planted rows, so they sort right after their source row


@dataclass
class Selection:
    header: list[str]
    idx: dict[str, int]
    tracks: dict[str, list[Row]] = field(default_factory=dict)
    others: list[Row] = field(default_factory=list)
    filler: list[Row] = field(default_factory=list)
    rows_seen: int = 0


def _clean_position(row: list[str], idx: dict[str, int]) -> tuple[datetime, float, float] | None:
    """Parsed (ts, lat, lon) if the row has no defect we plant ourselves, else None."""
    try:
        ts = datetime.strptime(row[idx["timestamp"]], TS_FMT)
        lat, lon = float(row[idx["latitude"]]), float(row[idx["longitude"]])
    except (IndexError, ValueError):
        return None
    if not (-90 <= lat <= 90 and -180 <= lon <= 180):
        return None
    if not _MMSI_OK.match(row[idx["mmsi"]]):
        return None
    sog, heading = row[idx["sog"]], row[idx["heading"]]
    if sog and _float_or(sog, 0) >= 102.3:
        return None
    if heading and _float_or(heading, 0) > 359:
        return None
    return ts, lat, lon


def _float_or(value: str, default: float) -> float:
    try:
        return float(value)
    except ValueError:
        return default


def select(zip_path: Path, filler_size: int, rng: random.Random) -> Selection:
    """One pass: per-vessel tracks, a few base station / AtoN rows and a filler reservoir."""
    with open_raw_csv(zip_path) as (_member, header, rows):
        idx = {sanitize_name(h): i for i, h in enumerate(header)}
        sel = Selection(header=header, idx=idx)
        other_left = dict(OTHER_TYPES)
        last_ts: dict[str, datetime] = {}
        seen_filler = 0
        for raw in rows:
            sel.rows_seen += 1
            if len(raw) != len(header):
                continue
            mobile = raw[idx["type_of_mobile"]]
            if other_left.get(mobile, 0) > 0:
                ts = _parse_ts(raw[idx["timestamp"]])
                if ts is not None:
                    other_left[mobile] -= 1
                    sel.others.append(Row(raw, ts))
                continue
            if mobile not in VESSEL_TYPES:
                continue
            parsed = _clean_position(raw, idx)
            if parsed is None:
                continue
            ts = parsed[0]
            mmsi = raw[idx["mmsi"]]

            # Reservoir sample of the filler (Algorithm R).
            seen_filler += 1
            if len(sel.filler) < filler_size:
                sel.filler.append(Row(raw, ts))
            else:
                j = rng.randrange(seen_filler)
                if j < filler_size:
                    sel.filler[j] = Row(raw, ts)

            if mobile != "Class A":
                continue
            track = sel.tracks.get(mmsi)
            if track is None:
                if len(sel.tracks) >= MAX_TRACKED_VESSELS:
                    continue
                track = sel.tracks[mmsi] = []
            if len(track) >= MAX_TRACK:
                continue
            if mmsi in last_ts and (ts - last_ts[mmsi]).total_seconds() < MIN_GAP_S:
                continue
            track.append(Row(raw, ts))
            last_ts[mmsi] = ts
    return sel


def _parse_ts(value: str) -> datetime | None:
    try:
        return datetime.strptime(value, TS_FMT)
    except ValueError:
        return None


def max_implied_kn(track: list[Row], idx: dict[str, int]) -> float:
    """Highest speed implied by consecutive fixes; catches shared/default MMSIs that jump."""
    worst = 0.0
    for a, b in zip(track, track[1:], strict=False):
        hours = (b.ts - a.ts).total_seconds() / 3600
        km = haversine_km(
            float(a.fields[idx["latitude"]]),
            float(a.fields[idx["longitude"]]),
            float(b.fields[idx["latitude"]]),
            float(b.fields[idx["longitude"]]),
        )
        worst = max(worst, km / KM_PER_NM / hours)
    return worst


def choose_vessels(sel: Selection, ports: list[Port]) -> tuple[str, str, str]:
    """(port vessel, moving vessel A, moving vessel B), deterministic."""
    idx = sel.idx
    full = {
        m: t
        for m, t in sel.tracks.items()
        if len(t) >= MIN_TRACK and max_implied_kn(t, idx) <= MAX_IMPLIED_KN
    }

    def in_port(track: list[Row]) -> int:
        n = 0
        for r in track:
            lat, lon = float(r.fields[idx["latitude"]]), float(r.fields[idx["longitude"]])
            port, dist = nearest_port(lat, lon, ports)
            n += dist <= port.port_radius_km
        return n

    def median_sog(track: list[Row]) -> float:
        return statistics.median(_float_or(r.fields[idx["sog"]], 0.0) for r in track)

    port_scores = sorted(((in_port(t), m) for m, t in full.items()), key=lambda s: (-s[0], s[1]))
    if not port_scores or port_scores[0][0] == 0:
        raise SystemExit("no Class A vessel with >= 20 positions inside a port radius")
    port_vessel = port_scores[0][1]

    moving = sorted(
        ((median_sog(t), m) for m, t in full.items() if m != port_vessel),
        key=lambda s: (-s[0], s[1]),
    )
    moving = [m for sog, m in moving if sog >= MIN_MOVING_SOG][:2]
    if len(moving) < 2:
        raise SystemExit("fewer than 2 moving Class A vessels with >= 20 positions")
    return port_vessel, moving[0], moving[1]


def build_rows(sel: Selection, vessels: tuple[str, str, str]) -> list[Row]:
    """Assemble base rows, drop filler collisions, plant the defects, sort by time."""
    idx = sel.idx
    vp, va, vb = vessels
    tp, ta, tb = sel.tracks[vp], sel.tracks[va], sel.tracks[vb]
    rows: list[Row] = [*tp, *ta, *tb, *sel.others]

    def key(r: Row) -> tuple[str, str]:
        return r.fields[idx["mmsi"]], r.fields[idx["timestamp"]]

    used = {key(r) for r in rows}
    filler_mmsi: set[str] = set()
    for r in sel.filler:
        mmsi = r.fields[idx["mmsi"]]
        if mmsi in vessels or mmsi in filler_mmsi or key(r) in used:
            continue  # one row per MMSI: no unplanned duplicates, conflicts or jumps
        filler_mmsi.add(mmsi)
        used.add(key(r))
        rows.append(r)

    for i, r in enumerate(rows):
        r.seq = i
    planted: list[Row] = []

    def plant(src: Row, defect: str, expected: str, note: str, shift_s: int = 0, **changes):
        fields = list(src.fields)
        ts = src.ts + timedelta(seconds=shift_s)
        if shift_s:
            fields[idx["timestamp"]] = ts.strftime(TS_FMT)
        for col, value in changes.items():
            fields[idx[col]] = value
        new = Row(
            fields,
            ts,
            note={"defect": defect, "expected": expected, "note": note},
            seq=src.seq,
            sub=1,
        )
        if shift_s and key(new) in used:
            raise SystemExit(f"planted {defect} collides with an existing (mmsi, ts)")
        used.add(key(new))
        planted.append(new)
        return new

    plant(ta[3], "exact_duplicate", "EXACT_DUPLICATE", "verbatim copy of the previous line")
    plant(tp[5], "exact_duplicate", "EXACT_DUPLICATE", "verbatim copy of the previous line")
    sog = _float_or(ta[6].fields[idx["sog"]], 0.0)
    plant(
        ta[6],
        "ts_conflict",
        "TS_CONFLICT",
        "same (mmsi, ts) as the previous line, SOG differs",
        sog=f"{sog + 1.3:.1f}",
    )
    plant(
        tb[4],
        "position_sentinel",
        "POSITION_SENTINEL",
        "lat 91 and lon 181 (DMA 'not available')",
        shift_s=1,
        latitude="91.000000",
        longitude="181.000000",
    )
    plant(tb[8], "bad_mmsi", "BAD_MMSI", "MMSI with 5 digits", shift_s=1, mmsi="12345")
    plant(
        ta[10],
        "bad_timestamp",
        "BAD_TIMESTAMP",
        "ISO 8601 instead of dd/MM/yyyy HH:mm:ss",
        timestamp=ta[10].ts.strftime("%Y-%m-%dT%H:%M:%S"),
    )
    plant(
        ta[12],
        "sog_sentinel",
        "SOG_SENTINEL",
        "SOG 102.3 (DMA 'not available')",
        shift_s=1,
        sog="102.3",
    )
    plant(
        tb[12],
        "heading_sentinel",
        "HEADING_SENTINEL",
        "heading 511 (DMA 'not available')",
        shift_s=1,
        heading="511",
    )
    jump_lat = float(ta[15].fields[idx["latitude"]]) + 0.5
    plant(
        ta[15],
        "position_jump",
        "POSITION_JUMP",
        "+0.5 deg latitude 30 s after the previous fix (~3600 kn); the next fix jumps back",
        shift_s=30,
        latitude=f"{jump_lat:.6f}",
    )
    prev_day = tp[2].ts - timedelta(days=1)
    plant(
        tp[2],
        "ts_outside_file_day",
        "TS_OUTSIDE_FILE_DAY",
        "timestamp on the previous day",
        timestamp=prev_day.strftime(TS_FMT),
    )

    # Name change: the last 5 positions of vessel B carry a new name (feeds dim_vessel SCD2).
    old_name = tb[0].fields[idx["name"]]
    if old_name in ("", "Unknown", "Unknown value", "Undefined", "N/A"):
        old_name = "FIXTURE VESSEL"
    new_name = f"{old_name} II"
    for r in tb[-5:]:
        r.fields[idx["name"]] = new_name
        r.note = {
            "defect": "name_change",
            "expected": "SCD2_NEW_VERSION",
            "note": f"name {old_name!r} -> {new_name!r} from here on",
        }

    for r in sel.others:
        r.note = {
            "defect": "out_of_scope_mobile",
            "expected": "OUT_OF_SCOPE_MOBILE",
            "note": f"type_of_mobile {r.fields[idx['type_of_mobile']]!r}",
        }

    rows.extend(planted)
    return sorted(rows, key=lambda r: (r.ts, r.seq, r.sub))


def write_outputs(
    out_dir: Path,
    header: list[str],
    rows: list[Row],
    vessels: tuple[str, str, str],
    zip_path: Path,
    rows_seen: int,
    seed: int,
) -> dict:
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / "ais_tiny.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh, lineterminator="\n")
        writer.writerow(header)
        for r in rows:
            writer.writerow(r.fields)

    idx = {sanitize_name(h): i for i, h in enumerate(header)}
    defects = [
        {
            "line": n,
            "mmsi": r.fields[idx["mmsi"]],
            "timestamp": r.fields[idx["timestamp"]],
            **r.note,
        }
        for n, r in enumerate(rows, start=2)
        if r.note
    ]
    meta = {
        "source_file": zip_path.name,
        "source_rows": rows_seen,
        "seed": seed,
        "rows": len(rows),
        "bytes": csv_path.stat().st_size,
        "vessels": {"in_port": vessels[0], "moving_a": vessels[1], "moving_b": vessels[2]},
        "defects": defects,
    }
    (out_dir / "ais_tiny_defects.json").write_text(json.dumps(meta, indent=2) + "\n")
    (out_dir / "README.md").write_text(render_readme(meta))
    return meta


def render_readme(meta: dict) -> str:
    v = meta["vessels"]
    lines = [
        "# Test fixtures",
        "",
        "## `ais_tiny.csv`",
        "",
        f"Cut from `{meta['source_file']}` ({meta['source_rows']:,} rows) by "
        f"`scripts/make_fixture.py` (seed {meta['seed']}). Do not edit by hand: re-run the script.",
        f"{meta['rows']} data rows, {meta['bytes']:,} bytes. "
        "Line numbers count the header as line 1.",
        "",
        "Only clean rows were selected, so the defects below are the only ones in the file.",
        "`ais_tiny_defects.json` holds the same table for tests.",
        "",
        "**Vessels with full tracks** (>= 20 positions, >= 60 s apart):",
        f"- `{v['in_port']}`: inside a port radius;",
        f"- `{v['moving_a']}` and `{v['moving_b']}`: moving (median SOG >= {MIN_MOVING_SOG} kn).",
        "",
        "| Line | MMSI | Defect | Expected handling | Note |",
        "|---:|---|---|---|---|",
    ]
    for d in meta["defects"]:
        lines.append(
            f"| {d['line']} | `{d['mmsi']}` | {d['defect']} | `{d['expected']}` | {d['note']} |"
        )
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    ap.add_argument("zip", type=Path, help="real daily DMA zip")
    ap.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    ap.add_argument("--ports", default="ref/ports.csv")
    ap.add_argument("--filler", type=int, default=DEFAULT_FILLER)
    ap.add_argument("--seed", type=int, default=DEFAULT_SEED)
    args = ap.parse_args(argv)

    rng = random.Random(args.seed)
    sel = select(args.zip, args.filler, rng)
    vessels = choose_vessels(sel, load_ports(args.ports))
    rows = build_rows(sel, vessels)
    meta = write_outputs(
        args.out_dir, sel.header, rows, vessels, args.zip, sel.rows_seen, args.seed
    )

    print(f"wrote {args.out_dir / 'ais_tiny.csv'}: {meta['rows']} rows, {meta['bytes']:,} bytes")
    print(f"vessels: {meta['vessels']}")
    print(f"planted/marked rows: {len(meta['defects'])}")
    if meta["bytes"] > MAX_FIXTURE_BYTES:
        print(f"warning: fixture is over {MAX_FIXTURE_BYTES:,} bytes; lower --filler")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
