# Maritime Traffic & Port Congestion Pipeline (AIS)

DAT535 Data Engineering, University of Stavanger, autumn 2026 — Group 10.
Author: Lucas Galvão Freitas. Victor Peugnet co-designed the project and left the group on
2 November 2026; the parts built after that date are Lucas's.

Repository: <https://github.com/devgalvas/group10>

> **Draft status.** Every number marked **[VM]** comes from the runs on the VM and is filled
> in after them. All other numbers are measured and reproducible from the repository.

---

## 1. Problem and outcome

Ports need to know how busy they are: how many vessels are berthed or waiting at anchor, hour
by hour, and how long a call takes. The raw signal exists: every ship broadcasts its position
over AIS every few seconds. However, it reaches us as tens of millions of noisy messages per day.

The project turns the Danish Maritime Authority (DMA) daily AIS files into a trusted Silver
dataset of vessel positions and two Gold tables for **Esbjerg (DKEBJ), Aarhus (DKAAR) and
Copenhagen (DKCPH)**:

- `gold.port_calls`: one row per vessel visit (arrival, departure, dwell time);
- `gold.port_congestion_hourly`: vessels in port, vessels at anchor, arrivals and departures
  per port, hour and ship type group.

A Streamlit dashboard is the consumer. The pipeline is batch, one day at a time, scheduled
by GitHub Actions on a single OpenStack VM.

**Out of scope:** streaming, ports outside the three, berth-level detail, ETA prediction.

## 2. Dataset and data understanding

| | |
|---|---|
| Source | DMA, `aisdk-YYYY-MM-DD.zip`, one CSV per day, public licence |
| Volume | 1.5–2.6 GB unzipped per day; **18,658,332 rows** on 2026-09-01 |
| Refresh | daily, published with a delay of a few days |
| One record | one AIS message as received by a DMA base station or satellite |
| Columns | 26: timestamp, type of mobile, MMSI, position, navigational status, ROT, SOG, COG, heading, IMO, call sign, name, ship type, cargo type, dimensions, draught, destination, ETA, data source type, … |

What makes it suitable for this course is that it is **large and dirty in realistic ways**
(D-01):

- **Timestamps** are day-first text (`01/09/2026 00:00:00`) and must be parsed explicitly.
- **Several sentinel encodings:** SOG 102.3 kn, COG 360°, heading 511 and latitude 91° all
  mean "not available".
- **Placeholder text** in numeric and text columns: `Unknown`, `Unknown value`, `Undefined`,
  empty strings.
- **Non-ships in the stream:** base stations, aids to navigation and SAR aircraft are mixed
  with ships.
- **Duplicates:** the same message heard by several stations arrives as an exact copy.
  Different contents for the same `(mmsi, ts_utc)` are also common.
- **Static attributes change over time** (name, call sign, dimensions), partly by real
  renaming, partly by typing errors.
- **GPS jumps:** consecutive positions that imply impossible speeds.

**Profiling of 2026-09-01** (full day, 18,658,332 rows, `docs/profiling_2026-09-01.md`):
- **Type of mobile:** Class A 82.3 % of rows (2,472 MMSIs), Class B 9.6 % (3,152), base
  stations 6.0 %, AtoN 2.1 %, SAR aircraft and transponders < 0.1 %. Base stations and AtoN
  are out of scope for vessel traffic: 1,510,373 rows are rejected as `OUT_OF_SCOPE_MOBILE`.
- **Missing values** are frequent but concentrated in voyage and static fields: `cargo_type`
  84 %, `imo` 48 % (as placeholder text), `eta` 43 %, `heading` 27 %, `navigational_status`
  23 % ("Unknown value"). Position, timestamp and MMSI are never missing.
- **Sentinels:** `lat = 91` ("not available") in 82,708 rows, SOG ≥ 102.3 in 2,497,
  heading 360–510 in 10,765. The high SOG values are not sentinels: 2,454 come from SAR
  aircraft (real speeds of 106–165 kn, out of scope) and 43 from invalid MMSIs. No in-scope
  row reaches 102.3; the maximum `sog_kn` in Silver is 102.2. 8.8 % of rows have an MMSI that is not 9 digits starting 2–7,
  mostly base stations and AtoN.
- **Duplicates:** 7,243,526 rows (38.8 %) are exact copies of another row, because several
  receivers hear the same broadcast. 56,978 `(mmsi, ts_utc)` keys carry different contents
  (114,878 rows, 0.6 %).
- **Rows per hour** follow the working day: 656,661 at 21:00 UTC to 863,978 at 11:00. All 24
  hours are present.
- **Distinct MMSIs:** 5,369 in the raw text. Per MMSI, a median of 806 rows, p99 28,957, max
  77,938. The top MMSI is a base station, and the top 15 hold 4.1 % of the rows. After
  cleaning, the largest vessel has 0.22 % of the day (P3).
- **Reference text:** one navigational status (`AIS-SART (active)`, 1 row) was missing from
  the mapping and was added. Every ship-type text was already mapped.

One profile result is already measured (`docs/profiling.md`): traffic concentrates near the
ports. **4.8 %** of the day lies within 15 km of the three ports, and going from 25 km to
15 km removes only 18 % of the near-port rows. That is why the dev sample is "15 km around
the ports + 0.5 % random" (976,985 rows, D-07).

## 3. Architecture

```
DMA zip ─► landing ─► bronze.ais_raw ─► silver.vessel_position ─► gold.port_calls
                       (all strings,     silver.dim_vessel (SCD2)   gold.port_congestion_hourly
                        lineage)         silver.dim_port                    │
                                         silver.rejects                     ▼
                       ops.ingestion_log · ops.dq_metrics · ops.pipeline_runs   dashboard
```

**Runtime:**
- PySpark 3.5 with Delta Lake 3.2 on one OpenStack VM (4 vCPU, 8 GB), as `local[4]` with 5 GB
  of driver memory (D-03);
- separate dev and prod lakes under `/data/ais/{dev,prod}`, each with its own pinned venv (D-04);
- every table is a Delta table, partitioned by date (D-02).

The code is a Python package (`src/ais_pipeline`) with a CLI (`ais-pipeline`). The layers are
built from pure transformation functions (`DataFrame → DataFrame`, no I/O inside), so each rule
is unit-tested on a hand-built DataFrame. I/O lives in `build.py` and `delta_io.py`.

## 4. Bronze: preserve and trace

| Decision | Evidence | Result |
|---|---|---|
| Every column as `StringType`, explicit schema from a versioned contract (D-06) | header checked against `contracts/ais_raw_v1.json` before reading | nothing is coerced or lost at ingestion; 18,658,332 Bronze rows = CSV data lines, 0 malformed |
| Delta, partitioned by `_source_date` (D-02) | 548 MB of Delta vs 3.34 GB of CSV for 2026-09-01 | 6× less to scan for every later stage |
| Lineage on every row | `_source_file`, `_file_sha256`, `_source_date`, `_ingest_ts`, `_run_id` | any Silver or reject row traces back to its file and run |
| Idempotent ingest | `ops.ingestion_log` keeps the sha256 of each ingested zip; the same file again is `skipped`, a new file or `--force` rewrites the date with `replaceWhere` | reruns never duplicate a day |

## 5. Silver: the trusted layer

### 5.1 Model and grain

`silver.vessel_position` has **one row per `(mmsi, ts_utc)`**, partitioned by `date`. Three
other tables complete the model:
- `silver.dim_vessel`: vessel attributes as SCD Type 2;
- `silver.dim_port`: port reference data;
- `silver.rejects`: every rejected Bronze row, kept verbatim with its reason.

The ER diagram is in `docs/silver_model.md`.

**Processing order for one day:**
1. Bronze partition.
2. Standardise.
3. Reject rules (rejected rows → `rejects`).
4. Exact dedup.
5. `(mmsi, ts_utc)` conflict resolution.
6. Sentinels.
7. `dim_vessel` MERGE.
8. Movement features.
9. Zones.
10. As-of join to `dim_vessel`.
11. Write `vessel_position` with `replaceWhere date = D`.

### 5.2 Standardisation

- **Placeholders → null** (`Unknown`, `Unknown value`, `Undefined`, `N/A`, empty).
- **Guarded casts:** a value that does not parse becomes null instead of failing the job.
- **Timestamps:** parsed as `dd/MM/yyyy HH:mm:ss` in UTC, with `timeParserPolicy=CORRECTED`,
  so a malformed timestamp becomes null and is counted, instead of failing the day (D-08).
- **Navigational status** mapped to snake_case labels from `ref/nav_status.csv`, and ship
  types to groups from `ref/ship_type_groups.csv`, both through broadcast lookups.
- **Units in column names:** `sog_kn`, `step_km`, `gap_s`, `length_m`.

The verbatim 26 Bronze columns travel with each row as a `_raw` struct, together with a
null-safe SHA-256 `_row_hash` (D-10). Without them, "exact duplicate" would compare cleaned
values, and `Unknown` and an empty field would look the same. Rejects would also no longer be
stored as received.

### 5.3 Cleaning: reject, deduplicate, flag

Each problem gets one of three treatments: **reject** (the row cannot be used), **resolve**
(keep one of several versions), or **flag and null** (the row is usable, but one field is not).

**Reject rules** apply in priority order; a row gets the first code that matches (D-09). The
same rules exist twice:
- as DataFrame `Column` expressions (`silver/rules.py`);
- as one Spark SQL `CASE WHEN` (`silver/rules.sql`).

A test proves that the two give identical results:

```sql
CASE
  WHEN _raw._corrupt_record IS NOT NULL                       THEN 'MALFORMED_ROW'
  WHEN mobile_class IS NULL                                   THEN 'OUT_OF_SCOPE_MOBILE'
  WHEN ts_utc IS NULL                                         THEN 'BAD_TIMESTAMP'
  WHEN to_date(ts_utc) <> _source_date                        THEN 'TS_OUTSIDE_FILE_DAY'
  WHEN lat = 91.0 OR lon = 181.0                              THEN 'POSITION_SENTINEL'
  WHEN lat IS NULL OR lon IS NULL OR … NOT BETWEEN …          THEN 'BAD_POSITION'
  WHEN NOT coalesce(trim(_raw.mmsi) RLIKE '^[2-7][0-9]{8}$', false) THEN 'BAD_MMSI'
END
```

**Deduplication** has two steps:
- *Exact copies:* `dropDuplicates(["_row_hash"])`.
- *Conflicts:* for the same `(mmsi, ts_utc)` with different contents, a window keeps the row
  with the most non-null fields; ties go to the smallest hash, so the result does not depend
  on input order. The kept row is flagged `TS_CONFLICT`.

**Sentinels:** SOG ≥ 102.3, COG ≥ 360 and heading 511 (or > 359) are flagged
(`SOG_SENTINEL`, …) and then set to null. A sentinel averaged as a real speed would corrupt
every Gold measure.

### 5.4 Quality evidence: before and after

`ops.dq_metrics` is filled at two checkpoints:
- `bronze_in`: before cleaning;
- `silver_out`: after cleaning.

Each checkpoint is **one aggregation pass** of conditional sums, so the metrics cost two scans
instead of one per metric. Row conservation is **measured, not derived** (D-11): each term is
counted by a different measurement, and the logged `unaccounted` must be 0.

| | fixture (259 rows, `tests/fixtures/ais_tiny.csv`) | dev sample of 2026-09-01 (real rows, D-07) | full day 2026-09-01 (run `20261109T153958Z-9c7062ec`) |
|---|---:|---:|---:|
| rows in | 259 | 976,985 | 18,658,332 |
| rejected | 7 | 33,178 (3.4 %) | 1,663,609 (8.9 %) |
| · `OUT_OF_SCOPE_MOBILE` | | 16,080 | 1,510,373 |
| · `BAD_MMSI` | | 17,010 | 135,890 |
| · `POSITION_SENTINEL` | | 88 | 17,346 |
| · `BAD_TIMESTAMP`, `TS_OUTSIDE_FILE_DAY`, `BAD_POSITION`, `MALFORMED_ROW` | | 0 | 0 |
| exact duplicates removed | 2 | 230,211 (23.6 %) | 6,211,222 (33.3 %) |
| conflicts dropped | 1 | 3,427 | 47,734 |
| accepted | **249** | **710,169** | **10,735,767** |
| unaccounted | 0 | **0** | **0** |
| heading sentinels nulled (`HEADING_SENTINEL`) | 1 | 5,973 | 5,976 |
| SOG / COG sentinels nulled | 1 / 0 | 0 / 0 | 0 / 0 |
| `POSITION_JUMP` flagged | 2 | 1,269 | 26,356 |
| in-scope reject rate (gate ≤ 5 %) | 1.56 % | 1.78 % | 0.89 % |
| distinct MMSI in → out | 178 → 174 | 4,171 → 3,948 | 5,369 → 5,040 |
| `dim_vessel` versions inserted | 4 | 3,574 | 5,687 |
| Silver wall time | | 60.1 s | 459 s |

The dev-sample column comes from the first automated run of `dat535-dev.yml` on the VM
(8 Nov 2026, run 42524555514, 2 min 17 s end to end, bronze → silver → gold). Three
observations from it:
- **Exact duplicates are a quarter of the rows near the ports and a third of the full day**
  (23.6 % vs 33.3 %). Several base stations hear the same ship, which is why exact dedup comes
  before every other step.
- **`BAD_MMSI` is the largest in-scope reject code, and it comes from very few transmitters.**
  On the full day, its 135,890 rows come from 25 MMSIs: 12 that report as Class A (121,263
  rows) and 13 as Class B (14,627). Most are special-purpose identifiers, not vessels: the
  `111` prefix (SAR aircraft), `99` (aids to navigation), `970` (AIS-SART) and a 7-digit
  base-station number. One misconfigured transponder (`3638`) alone sends 43,944 rows.
  Rejecting them is correct: none can be joined to a vessel.
- **`exact_duplicates` differs between the two checkpoints.** At `bronze_in` it counts copies
  among all rows (237,613); `exact_duplicates_removed` counts them only among the valid rows
  (230,211). The 7,402 difference are copies of rows that were rejected anyway.

`ais-pipeline dq-report --date D` prints the full before/after table from `ops.dq_metrics`.

### 5.5 Enrichment

**`dim_vessel`, SCD Type 2 (D-12).** The first design kept the most frequent combination of
attributes per vessel and day. The fixture shows why that is wrong: a name change at 00:20,
with 22 reports before it and 5 after, would vanish. The implementation works like this:
- every combination seen at least twice in a day opens a version at its first sighting;
- one Delta `MERGE`, written in Spark SQL, closes the old version and inserts the new one, so
  a reader never sees zero or two current versions;
- versions not newer than the current one are skipped, so a rerun is a no-op.

```sql
MERGE INTO delta.`…/silver/dim_vessel` AS t
USING staged AS s ON t.vessel_sk = s._merge_key
WHEN MATCHED AND s._action = 'close' THEN UPDATE SET t.valid_to = s.valid_to, t.is_current = false
WHEN NOT MATCHED AND s._action = 'insert' THEN INSERT (…) VALUES (…)
```

Positions find their version with an **as-of join**: `ts_utc` between `valid_from` and
`valid_to`. On 2026-09-01, 45,797 of 10,735,767 positions (0.43 %, metric `null_vessel_sk`)
have no version. Either the vessel sent no combination of static attributes twice that day,
or the fix comes before its first sighting. These positions are kept with a null key, not
rejected: the position itself is valid. Gold builds port calls and congestion by `mmsi`, so
these rows still count. They only lack the vessel type in the per-type breakdowns.

**Movement features:**
- `gap_s`, `step_km` and `implied_speed_kn` come from `lag` over a per-MMSI window;
- `POSITION_JUMP` flags implied speeds above 50 kn;
- the haversine is written only with built-in functions, so Catalyst can optimise it (a UDF is
  measured as the alternative in P5);
- the previous day's last fix of each vessel is added as context, so the first fix of the day
  also has a gap and a step. It is found with one `max(struct(ts_utc, lat, lon))` per vessel,
  with no sort of the whole day.

**Zones (D-13).** The plan was a cross join with every port and a "nearest port" aggregation,
which adds a shuffle. The three anchorage circles do not overlap, and `dim_port` validates
that at load time. So one **broadcast left join** on "inside the anchorage circle" gives
`port`, `anchorage` or `at_sea` directly. A bounding-box comparison runs before the
trigonometry; its gain is measured in P2.

### 5.6 Three Spark APIs, each where it fits

| API | where | why |
|---|---|---|
| DataFrame | almost all transformations | composable, testable functions; Catalyst optimises them |
| Spark SQL | `rules.sql`, the SCD2 `MERGE`, `congestion.sql` | the clearest form for set logic, `CASE` rules and `MERGE` |
| RDD | `rdd_check.py`: `map → reduceByKey → filter → sum` | an independent cross-check of the duplicate count (`--rdd-check`); its cost vs DataFrame is P6 |

### 5.7 Tests

| | count | covers |
|---|---:|---|
| unit | 145 | every reject code (positive and negative case), priority, DataFrame = SQL, dedup tie-break, sentinels at their boundaries, haversine, zones, gates, CLI, perf harness and event-log reader |
| integration | 36 | Bronze ingest, idempotency and schema evolution, row conservation recounted term by term, SCD2 with a planted name change, full Silver rerun gives identical rows, Gold reconciliation, stale gate, dashboard render |
| **total** | **181** | run on every PR by `ci.yml` (about 2 min for the setup PR, about 10 min for the full suite) |

### 5.8 Scalability: volume, late data, schema change

**Volume.** One day is 18.7 M rows: 3.34 GB of CSV, 548 MB of Bronze Delta. Every stage works
on one `_source_date`/`date` partition and writes it with `replaceWhere`, so a run touches one
day plus the previous day's last fix per vessel (one aggregation, §5.5). The cost of a day does
not grow with the size of the lake, and a backfill is a loop over days. The limit is memory per
day, not the number of days: a full day needed the three changes of D-17 to fit in the 8 GB
VM. At 10× the daily volume, the first things to revisit are the 32 MB read partitions, the
`DISK_ONLY` persists and the 16 shuffle partitions (P4), on more than one node.

**Late or corrected data.** The DMA publishes one file per day, so data arrives late as a
corrected file for a past day:
- A row whose timestamp is not on the file's day is rejected as `TS_OUTSIDE_FILE_DAY`, not
  written to the wrong partition. There were 0 such rows on 2026-09-01.
- `ops.ingestion_log` keeps the SHA-256 of each zip. The same file again is `skipped`; a
  different file for the same day is re-ingested and replaces only that Bronze partition.
- Re-running Silver for that day replaces its partition. Gold recomputes `port_calls` from
  every gated date and rewrites `port_congestion_hourly` for the gated dates (D-15).
- Two limits remain. The SCD2 merge skips versions that are not newer than the current one,
  so a correction to an old day cannot insert an older vessel version. And the first fix of
  the following day keeps the gap computed from the old data until that day is also re-run.

**Schema change.** The raw header is checked against the versioned contract
`contracts/ais_raw_v1.json` before any row is read. Columns are matched by sanitised name,
with aliases (`# Timestamp` and `Timestamp`), so reordered columns still map correctly.

| header change | behaviour | where it is recorded |
|---|---|---|
| a required column is missing (5 of 26) | `SchemaContractError`, the day fails, other days are untouched | `failed` row in `ops.ingestion_log` |
| an optional column is missing | warning; the column reads as null for that day | `missing_optional` in the header report |
| a new column appears | warning; Bronze adds it with `mergeSchema` (null on earlier days); Silver ignores it until a contract v2 maps it | `extra_columns` in `ops.ingestion_log` |

The third row was a gap found while writing this section. The contract accepted the extra
column, but the Bronze write had no `mergeSchema`, and Delta rejected the day with a schema
mismatch. The fix is two lines, and two integration tests cover both optional cases
(`test_extra_source_column_is_added_to_bronze`, `test_missing_optional_column_reads_as_null`).

## 6. Performance: measured, not assumed

**Method (D-16).**
- Each variant runs in its own Spark job group and ends in a `noop` write, so the timing
  covers the full computation but no output I/O.
- After the run, the harness sums the stage metrics from the driver REST API: input bytes,
  shuffle read/write, spill, max and median task time.
- The formatted physical plan of each variant is saved.
- Each variant gets one unrecorded warm-up run, then 3 measured runs; the results are
  appended to `ops.perf_results`, and the report uses the median.
- P4 is the exception: each variant is a full Silver build (~8 min). It runs once, without
  warm-up, in its own process. Several builds in one process exhausted the VM's 8 GB.
- The input is the prod day 2026-09-01. Experiments write only under `/data/ais/perf`.

All numbers are medians from `ops.perf_results` (run 9–10 Nov 2026). The full table and the
saved physical plans are in `docs/performance.md` and `docs/perf/plans/`.

| exp | hypothesis | best vs baseline (median wall) | verdict |
|---|---|---|---|
| P1 | Delta + partition pruning reads far less than the CSV | Delta 0.8 s / 13 MB vs CSV 5.4 s / 3,343 MB | confirmed |
| P2 | broadcasting small dimensions avoids a shuffle | broadcast 1.4 s vs sort-merge 2.3 s (190 MB shuffle) | confirmed, kept |
| P2 | the bounding box skips most trigonometry | 1.2 s vs 1.4 s | small gain, kept |
| P3 | the per-vessel window is skewed by a few chatty MMSIs | largest MMSI = 0.22 % of the day | rejected |
| P4 | 200 shuffle partitions is too many for 4 cores | all six variants 480–525 s; the same variant varied 16 % between runs | not supported; 16 kept |
| P5 | a Python UDF is slower than built-ins | built-in 0.8 s · pandas UDF 1.9 s · Python UDF 6.1 s | confirmed |
| P6 | the RDD path costs more than DataFrame | `distinct` 27.8 s vs `reduceByKey` 203.0 s | confirmed |

**P1: raw format and pruning.** The same filter and count (`navigational_status = 'Moored'`)
reads the whole 3,343 MB CSV in 5.4 s. On Bronze Delta it takes 0.8 s and reads 13 MB.
Parquet reads only the needed column, and the filter is pushed down to the scan
(`PushedFilters: EqualTo(navigational_status,Moored)` in the plan). On Silver, a filter on the
partition column `date` reads 1 MB (`PartitionFilters` in the plan). The same day written as a
range on `ts_utc` reads 27 MB: Delta's file statistics still skip files, but the partition
column skips them for free. Silver holds one day so far, so the time difference (0.5 vs 0.6 s)
is noise. **[repeat P1 c/d after the backfill: with 8 days, pruning should read 1/8 of the
table]**

**P2: joins.** The as-of join of positions to `dim_vessel` takes 1.4 s as a
`BroadcastHashJoin`. Forced to a `SortMergeJoin`, it takes 2.3 s and shuffles 190 MB on each
side. `dim_vessel` has 5,687 rows, so broadcasting it costs nothing. The code keeps explicit
`F.broadcast` hints on `dim_vessel`, `dim_port` and both reference tables. The bounding-box
prefilter in the zone join cuts the time from 1.4 to 1.2 s (median task 0.48 → 0.28 s). With
three ports the gain is small, but it grows with the number of ports, so it stays.

**P3: skew.** The hypothesis was wrong. On 2026-09-01, 5,040 vessels report a median of 705
positions each (p99 12,184). The largest MMSI has 23,213 rows, 0.22 % of the day
(`docs/perf/p3_rows_per_mmsi_2026-09-01.md`). Turning AQE skew handling off changes nothing
(3.1 vs 3.5 s). The skew optimisation acts on joins, not windows. An explicit
`repartition(16, "mmsi")` does help: 2.6 vs 3.5 s, max task 1.5 → 0.6 s. Both plans shuffle on
`hashpartitioning(mmsi, 16)` with the same 193 MB. The difference is the exchange type:
`ENSURE_REQUIREMENTS` in the base plan, which AQE may coalesce into fewer, larger partitions,
and `REPARTITION_BY_NUM` with the explicit repartition, which it may not. The event log
confirms it: AQE merges the 16 partitions of 193 MB into 5 tasks. On 4 cores that is two
waves, and the second runs a single task. The explicit repartition keeps 16 balanced tasks.
32 and 64 partitions give the same 2.6–2.7 s. On the full Silver build (P4), the same effect shows as spill, not time.
The last pair compares the window on valid rows before and after deduplication, both built
from Bronze. Deduplicating first costs 383 s vs 312 s, but it is required for correctness: an
exact duplicate gives a gap of 0 s, so `implied_speed_kn` is null and the `POSITION_JUMP`
check cannot see that row. Deduplication stays before the window.

**P4: shuffle partitions.** Each variant is one full Silver build of the day, run once in its
own process (10 Nov 2026). Task counts and the bytes written to disk by spills come from
the Spark event log of each run:

| shuffle partitions | AQE coalescing | wall (s) | tasks in the 3 large shuffles | tasks in the last stage | spilled to disk (MB) |
|---:|---|---:|---|---:|---:|
| 200 | on | 493 / 570 | 25 / 50 / 200 | 7 | 130 |
| 64 | on | 525 | 32 / 64 / 64 | 6 | 129 |
| 16 | on | 503 | 16 / 16 / 16 | 6 | 301 |
| 8 | on | 503 | 8 / 8 / 8 | 8 | 4,811 |
| 200 | off | 510 | 200 / 200 / 200 | 200 | 0 |
| 16 | off | 480 | 16 / 16 / 16 | 16 | 31 |

The hypothesis is not supported. Every variant takes 480–525 s. The one configuration measured
twice (200, coalescing on: 493 s after a warm-up on 9 Nov, 570 s as the first build of the
day on 10 Nov) varies more than the variants differ from each other. With one run per
variant, no setting is measurably faster. The wall time is set by the longest tasks
(45–55 s): parsing the raw strings and sorting for the deduplication. The number of
partitions after the shuffle does not change those tasks.
What the setting does change is the size of each task, and so the **spill**. The three
large shuffles move 1.5, 2.8 and 2.5 GB. With 8 partitions each task holds ~350 MB and
spills 4.8 GB to disk in total; with 200 and no coalescing nothing spills. The spill costs
little time here because the VM's local disk absorbs it. Coalescing leaves the large
shuffles alone at 16 partitions: each partition is already above AQE's 64 MB target. It
only shrinks the small last stage, the per-vessel window before the write, from 16 tasks to
6. That is the P3 effect again, and the extra spill (301 vs 31 MB) comes from it.
`config/prod.yaml` keeps 16 partitions: it is the fastest of the six runs (16, off), and the
production build of the same day took 459 s with it. This is a choice within the noise, not
a measured gain. A firm answer would need 3+ runs per variant, about 35 minutes of VM time
each (3.5 hours for the six).

**P5: UDFs.** Haversine distance to Esbjerg over 10.7 M positions: built-in functions 0.8 s,
pandas UDF 1.9 s (`ArrowEvalPython`, vectorised batches through Arrow), plain Python UDF 6.1 s
(`BatchEvalPython`, one Python call per row). The pipeline uses only the built-in version
(`silver/features.py`). UDFs exist only in `perf/` as baselines.

**P6: RDD vs DataFrame.** Counting exact duplicates in the 18.7 M Bronze rows takes 203.0 s with
`map` + `reduceByKey` and 27.8 s with `distinct()`. The RDD version shuffles less (1,269 vs
2,519 MB), because `reduceByKey` combines on the map side. It is still 7.3× slower: every row
becomes a pickled Python tuple, and the shuffle spills: about 1.9 GB written to disk per
run, against none for `distinct()`. Its first attempt
had the JVM killed by the kernel. It finished with `spark.python.worker.memory=256m`. The
pipeline counts duplicates with DataFrames. The RDD count runs only behind `--rdd-check`, as an
independent cross-check of the same number (§5.6).

## 7. Gold: serve the outcome

**Why Gold differs from Silver:**
- Silver holds every position (reusable for any question). Gold holds two small,
  purpose-built tables that the dashboard reads directly, without Spark.

**Port calls (D-14):**
- A first version split calls on Silver's `gap_s`. That failed: a ship reporting every 10 s
  at sea never shows a gap, even after it leaves the port.
- The fix filters to `zone = 'port'` first and runs gaps-and-islands on the in-port fixes. A
  new call starts when the port changes or after more than 2 h out of the port.
- A running sum of the "new call" marks numbers the calls. Calls under 30 min are passing
  ships, not visits, and are dropped.

**Congestion** is Spark SQL over temporary views:
- `COUNT(DISTINCT mmsi)` in port, and at anchor (anchorage zone and SOG < 1 kn), per port,
  hour and ship type group;
- arrivals and departures come from `port_calls`.

**Quality gates (D-15).** A Silver date reaches Gold only if three checks on `ops.dq_metrics`
pass:
- all 24 hours are present;
- the in-scope reject rate is ≤ 5 %;
- the row count is ≥ 0.5 × the median of the other days.

`port_calls` is recomputed from the gated dates; congestion is rewritten with `replaceWhere`
only for gated dates. A failing date keeps its previous Gold rows and is logged `stale` in
`ops.pipeline_runs`, which the dashboard shows.

**Correctness evidence:**
- the integration test recomputes the hourly in-port counts directly from Silver and gets
  the same numbers as Gold;
- every call lies within the vessel's Silver track;
- a forced gate failure logs `stale` and leaves Gold unchanged.

On the dev sample of 2026-09-01, the gates passed (24 hours present, 1.78 % in-scope reject
rate) and Gold published **333 port calls** and 526 hourly congestion rows. Silver zones on that
sample: 420,401 positions in port, 135,378 at anchor, 154,390 at sea.

**[VM: number of calls per port over the backfill window, dwell-time distribution, dashboard
screenshot.]**

## 8. End-to-end delivery

**Environments:**
- `config/{dev,prod,ci}.yaml` with `${data_root}` interpolation;
- separate lakes and venvs for dev and prod;
- `ci` uses a temporary lake.

**Workflows:**

| workflow | trigger | runner | does |
|---|---|---|---|
| `ci.yml` | every PR to `dev`/`main` | GitHub-hosted | ruff + 175 tests, junit summary on the run page (D-05) |
| `dat535-dev.yml` | push to `dev` | VM | bronze → silver → gold on the dev sample, dq-report in the step summary |
| `dat535-prod.yml` | push to `main`, **approval required** | VM | installs the commit to `/opt/ais-pipeline/releases/<sha>`, switches the `current` symlink, runs `check`, keeps 3 releases |
| `scheduled.yml` | cron every 6 h, or dispatch with a date range | VM | `run --next-day` (or a backfill) from the deployed release |

The three VM workflows share `concurrency: vm-pipeline`, so two runs never write the same
Delta table at once.

**Observability:**
- `ops.pipeline_runs`: one row per stage, with status, row counts and the git SHA;
- `ops.ingestion_log`, `ops.dq_metrics` and `silver.rejects`;
- a Markdown summary on every Actions run page;
- the Spark History Server for every run.

**Reproducibility:**
- `README.md` takes a clean clone to a full run on the fixture;
- `docs/runbook.md` covers the VM: setup, backfill, rollback.

**[VM: screenshots of a red and a green CI run, the prod approval dialog, a scheduled run
summary, the backfill row counts.]**

## 9. Limitations

- **Port areas are circles** around a hand-picked centre. Berths and anchorages are
  irregular, so a few positions near the edge are misassigned. The profile checked the centres
  against where `moored` vessels actually lie: the median moored position is 0.32 km (DKEBJ),
  0.67 km (DKCPH) and 0.96 km (DKAAR) from the centre, so the centres were kept. The radius
  fits Aarhus (100 % of moored fixes inside the port circle) and Esbjerg (87.5 %), but not
  Copenhagen: its quays spread along the coast (p90 6.18 km from the centre), and only 66 % of
  its moored fixes fall inside the 3 km circle. Port calls in Copenhagen are therefore
  undercounted; one polygon per port (or several circles) would fix it.
- **One region and a short window:** three Danish ports and **[VM: N]** days. Seasonal
  patterns are out of reach.
- **Static attribute noise:** with a minimum of 2 reports per version, a persistent typing
  error still opens a version. **[VM: versions per vessel per day.]**
- **Single node:** performance conclusions hold for `local[4]`. At 10× the volume they would
  need re-measuring on a cluster (stretch AIS-45 not done).
- **Processing order:** days must be processed in date order for the SCD2 no-op rule. A late
  day older than the current version is ignored, not merged into the history.

## 10. Reflection

> **To be written by Lucas, in his own words.** Prompts, with facts to draw on:
>
> - *What worked:* measuring conservation term by term caught … ; the fixture with planted
>   defects made every rule testable; the decision log made trade-offs explicit.
> - *What did not, and why:* the first port-call split on `gap_s` (D-14); the daily-mode SCD2
>   (D-12); the cross join for zones (D-13); a build-time `Column` needing a SparkContext.
> - *Running the project alone after 2 November:* what was cut or kept, and how.
> - *At 10× volume:* a Spark cluster instead of `local[4]`, object storage instead of a local
>   volume, incremental Gold instead of recomputing `port_calls`, streaming ingestion of the
>   live AIS feed.
> - *What I would change if I started again.*

## Appendix A: decision index

| id | decision |
|---|---|
| D-01 | DMA AIS over NYC TLC |
| D-02 | Delta Lake over plain Parquet |
| D-03 | single-node `local[4]` |
| D-04 | pinned project venvs, Java 17 |
| D-05 | CI on GitHub-hosted runners, pipeline on the VM |
| D-06 | Bronze keeps every column as a string |
| D-07 | dev sample: 15 km around the ports + 0.5 % random |
| D-08 | unparseable timestamps become null |
| D-09 | `MALFORMED_ROW` reject code |
| D-10 | verbatim raw values travel with each row |
| D-11 | row conservation is measured, not derived |
| D-12 | `dim_vessel` versions can change within a day |
| D-13 | zones by one broadcast left join |
| D-14 | port calls split on time away from the port |
| D-15 | Gold publishes only gated dates |
| D-16 | experiments never write to the prod lake |
| D-17 | Silver fits a full day in 8 GB by staying out of the heap |

Full entries: `docs/decisions.md`.
