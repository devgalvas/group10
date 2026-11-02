# Test fixtures

## `ais_tiny.csv`

Cut from `aisdk-2026-09-01.zip` (18,658,332 rows) by `scripts/make_fixture.py` (seed 535). Do not edit by hand: re-run the script.
259 data rows, 50,010 bytes. Line numbers count the header as line 1.

Only clean rows were selected, so the defects below are the only ones in the file.
`ais_tiny_defects.json` holds the same table for tests.

**Vessels with full tracks** (>= 20 positions, >= 60 s apart):
- `212209000`: inside a port radius;
- `212112000` and `311066400`: named, moving (median SOG >= 5.0 kn).

| Line | MMSI | Defect | Expected handling | Note |
|---:|---|---|---|---|
| 2 | `2190064` | out_of_scope_mobile | `OUT_OF_SCOPE_MOBILE` | type_of_mobile 'Base Station' |
| 3 | `2194006` | out_of_scope_mobile | `OUT_OF_SCOPE_MOBILE` | type_of_mobile 'Base Station' |
| 4 | `992111888` | out_of_scope_mobile | `OUT_OF_SCOPE_MOBILE` | type_of_mobile 'AtoN' |
| 14 | `212112000` | exact_duplicate | `EXACT_DUPLICATE` | verbatim copy of the previous line |
| 18 | `311066400` | position_sentinel | `POSITION_SENTINEL` | lat 91 and lon 181 (DMA 'not available') |
| 23 | `212112000` | ts_conflict | `TS_CONFLICT` | same (mmsi, ts) as the previous line, SOG differs |
| 28 | `212209000` | ts_outside_file_day | `TS_OUTSIDE_FILE_DAY` | timestamp on the previous day |
| 31 | `12345` | bad_mmsi | `BAD_MMSI` | MMSI with 5 digits |
| 36 | `212112000` | bad_timestamp | `BAD_TIMESTAMP` | ISO 8601 instead of dd/MM/yyyy HH:mm:ss |
| 42 | `311066400` | heading_sentinel | `HEADING_SENTINEL` | heading 511 (DMA 'not available') |
| 43 | `212112000` | sog_sentinel | `SOG_SENTINEL` | SOG 102.3 (DMA 'not available') |
| 50 | `212209000` | exact_duplicate | `EXACT_DUPLICATE` | verbatim copy of the previous line |
| 53 | `212112000` | position_jump | `POSITION_JUMP` | +0.5 deg latitude 30 s after the previous fix (~3600 kn); the next fix jumps back |
| 64 | `311066400` | name_change | `SCD2_NEW_VERSION` | name 'SONANGOL HUILA' -> 'SONANGOL HUILA II' from here on |
| 67 | `311066400` | name_change | `SCD2_NEW_VERSION` | name 'SONANGOL HUILA' -> 'SONANGOL HUILA II' from here on |
| 70 | `311066400` | name_change | `SCD2_NEW_VERSION` | name 'SONANGOL HUILA' -> 'SONANGOL HUILA II' from here on |
| 72 | `311066400` | name_change | `SCD2_NEW_VERSION` | name 'SONANGOL HUILA' -> 'SONANGOL HUILA II' from here on |
| 73 | `311066400` | name_change | `SCD2_NEW_VERSION` | name 'SONANGOL HUILA' -> 'SONANGOL HUILA II' from here on |
