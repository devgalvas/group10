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

**Profiling of 2026-09-01 [VM]:**
- share of each type of mobile;
- null rate per column;
- sentinel counts;
- exact duplicate rate;
- distinct MMSIs;
- rows per hour;
- top MMSIs by row count (skew).

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

| | fixture (259 rows, `tests/fixtures/ais_tiny.csv`) | dev sample of 2026-09-01 (real rows, D-07) | full day 2026-09-01 **[VM]** |
|---|---:|---:|---:|
| rows in | 259 | 976,985 | 18,658,332 |
| rejected | 7 | 33,178 (3.4 %) | |
| · `OUT_OF_SCOPE_MOBILE` | | 16,080 | |
| · `BAD_MMSI` | | 17,010 | |
| · `POSITION_SENTINEL` | | 88 | |
| · `BAD_TIMESTAMP`, `TS_OUTSIDE_FILE_DAY`, `BAD_POSITION`, `MALFORMED_ROW` | | 0 | |
| exact duplicates removed | 2 | 230,211 (23.6 %) | |
| conflicts dropped | 1 | 3,427 | |
| accepted | **249** | **710,169** | |
| unaccounted | 0 | **0** | |
| heading sentinels nulled (`HEADING_SENTINEL`) | 1 | 5,973 | |
| SOG / COG sentinels nulled | 1 / 0 | 0 / 0 | |
| `POSITION_JUMP` flagged | 2 | 1,269 | |
| in-scope reject rate (gate ≤ 5 %) | 1.56 % | 1.78 % | |
| distinct MMSI in → out | 178 → 174 | 4,171 → 3,948 | |
| `dim_vessel` versions inserted | 4 | 3,574 | |
| Silver wall time | | 60.1 s | |

The dev-sample column comes from the first automated run of `dat535-dev.yml` on the VM
(8 Nov 2026, run 42524555514, 2 min 17 s end to end, bronze → silver → gold). Three
observations from it:
- **Exact duplicates are about a quarter of the rows near the ports.** Several base stations
  hear the same ship, which is why exact dedup comes before every other step.
- **`BAD_MMSI` is the largest reject code.** **[VM: check on the full day whether these are
  invalid MMSIs from Class B transponders.]**
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
`valid_to`.

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
| unit | 142 | every reject code (positive and negative case), priority, DataFrame = SQL, dedup tie-break, sentinels at their boundaries, haversine, zones, gates, CLI, perf harness |
| integration | 33 | Bronze ingest and idempotency, row conservation recounted term by term, SCD2 with a planted name change, full Silver rerun gives identical rows, Gold reconciliation, stale gate, dashboard render |
| **total** | **175** | run on every PR by `ci.yml` (about 2 min for the setup PR, about 10 min for the full suite) |

## 6. Performance: measured, not assumed

**Method (D-16).**
- Each variant runs in its own Spark job group and ends in a `noop` write, so the timing
  covers the full computation but no output I/O.
- After the run, the harness sums the stage metrics from the driver REST API: input bytes,
  shuffle read/write, spill, max and median task time.
- The formatted physical plan of each variant is saved.
- Each variant gets one unrecorded warm-up run, then 3 measured runs; the results are
  appended to `ops.perf_results`, and the report uses the median.
- The input is the prod day 2026-09-01. Experiments write only under `/data/ais/perf`.

| exp | hypothesis | variants | result **[VM]** | verdict |
|---|---|---|---|---|
| P1 | Delta + partition pruning reads far less than the CSV | CSV vs Bronze Delta; partition filter vs an expression on the date | | |
| P2 | broadcasting small dimensions avoids a shuffle; the bounding box skips most trigonometry | as-of join broadcast vs sort-merge; zones with/without the bbox | | |
| P3 | the per-vessel window is skewed by a few chatty MMSIs | base, AQE skew off, explicit repartition, before vs after dedup | | |
| P4 | 200 shuffle partitions is too many for 4 cores | full Silver with 200 / 64 / 16 / 8, coalescing on/off | | |
| P5 | a Python UDF is slower than built-ins | haversine built-in vs Python UDF vs pandas UDF | | |
| P6 | the RDD path costs more than DataFrame | duplicates: `reduceByKey` vs `distinct` | | |

**[VM: one paragraph per experiment: the number that decided it, a plan excerpt or Spark UI
screenshot, and what was kept in the code.]**

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
  irregular, so a few positions near the edge are misassigned. **[VM: centre refinement
  result.]**
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

Full entries: `docs/decisions.md`.
