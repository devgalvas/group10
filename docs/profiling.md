# Profiling and data samples

## Dev sample (AIS-15)

**Purpose:** a small but real input for the `dev` environment, so a full pipeline run takes
minutes instead of the ~18.7 M rows of a prod day.

**Rule** (`scripts/make_dev_sample.py`): keep every row within **15 km** of a port centre in
`ref/ports.csv`, plus a seeded (535) **0.5 %** random sample of the remaining rows. 15 km is
above the largest anchorage radius (12 km, Esbjerg), so every port and anchorage position is
kept and port calls and congestion in dev are complete; the random part keeps open-sea
traffic for the cleaning rules and skew checks.

**Source:** `aisdk-2026-09-01.zip`, 18,658,332 rows. Output:
`/data/ais/dev/landing/date=2026-09-01/aisdk-2026-09-01-sample.zip`.

The target was 0.3–1 M rows. Three runs on the VM, 2 Nov 2026:

| Radius | Random rate | Near DKEBJ | Near DKAAR | Near DKCPH | Random | Kept | Share | Wall (s) |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 25 km | 2 % | 349,195 | 229,457 | 509,565 | 351,147 | 1,439,364 | 7.71 % | 76.7 |
| 15 km | 2 % | 335,902 | 196,501 | 355,517 | 355,191 | 1,243,111 | 6.66 % | 72.7 |
| **15 km** | **0.5 %** | **335,902** | **196,501** | **355,517** | **89,065** | **976,985** | **5.24 %** | **71.1** |

**Finding:** traffic is concentrated close to the ports. Going from 25 to 15 km removes only
18 % of the near-port rows, mostly around Copenhagen (the Øresund lanes), and 4.8 % of the whole
day sits within 15 km of the three ports. The random share was the lever that reached the
target. The last row is the default.
