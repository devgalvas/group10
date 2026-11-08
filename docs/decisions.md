# Decision log

One entry per decision that had real alternatives. Newest at the bottom. Format:
context → options → decision → consequence.

## D-01 Dataset: DMA AIS over NYC TLC trips
- Date: 2026-10-14 · Author: Lucas, Victor · Issue: project definition
- Context: the course asks for a dataset big enough to justify Spark, with real quality
  problems for a Silver layer to solve.
- Options: (a) NYC TLC taxi trips, (b) Danish Maritime Authority (DMA) AIS archive.
- Decision: (b). One day is a 1.5–2.6 GB CSV (18,658,332 rows on 2026-09-01), and the data
  has the problems a Silver layer exists for: day-first timestamps, sentinel codes, placeholder
  text, base stations mixed with ships, duplicates heard by several stations, GPS jumps and
  static attributes that change. TLC trips are already well curated and widely used.
- Consequence: no ready-made answers to compare against; port areas and ship-type groups
  had to be built as reference data.

## D-02 Delta Lake over plain Parquet
- Date: 2026-10-14 · Author: Lucas · Issue: AIS-02, AIS-13
- Context: reruns must be idempotent, `dim_vessel` needs a Type 2 history, and Gold must not
  be half-written when a gate fails.
- Options: (a) Parquet with dynamic partition overwrite, (b) Delta Lake.
- Decision: (b). ACID commits, `replaceWhere` per date partition, `MERGE` for SCD2 and time
  travel for before/after checks. Parquet with partition overwrite stays the fallback.
- Consequence: a pinned `delta-spark` (3.2.x, for Spark 3.5) in every environment. Bronze for
  2026-09-01 is 548 MB against 3.34 GB of CSV.

## D-03 Single-node Spark, `local[4]`, on the VM
- Date: 2026-10-23 · Author: Lucas · Issue: AIS-08
- Context: one OpenStack VM, `m1.large` (4 vCPU, 8 GB RAM); the quota allows one floating IP.
- Options: (a) standalone master and worker on the same host, (b) `local[4]`,
  (c) extra worker VMs.
- Decision: (b), driver memory 5 GB. A standalone cluster on one host adds daemons and memory
  overhead without adding cores. (c) is kept as a stretch goal (AIS-45).
- Consequence: shuffle partitions are sized for 4 cores (tested in AIS-33); the Spark event
  log and History Server keep the UI of every run.

## D-04 Project venvs with pinned versions and Java 17
- Date: 2026-10-23 · Author: Lucas · Issue: AIS-08
- Context: the lab setup has Java 8, Spark 3.5.0 in `/opt/spark` and an unpinned `pyspark`
  loaded through `findspark`.
- Options: (a) reuse the lab environment, (b) separate venvs with pinned dependencies.
- Decision: (b). `~/ais-env-dev` and `~/ais-env-prod`, `pyspark>=3.5.3,<3.6`,
  `delta-spark>=3.2,<3.3`, OpenJDK 17 (same as CI), `SPARK_HOME` unset so the pip PySpark uses
  its own jars.
- Consequence: the labs keep working untouched; dev and prod cannot break each other.

## D-05 CI on GitHub-hosted runners, pipeline on the self-hosted runner
- Date: 2026-10-23 · Author: Lucas · Issue: AIS-04, AIS-10
- Context: there is one self-hosted runner, on the same VM that runs the pipeline.
- Options: (a) everything on the VM runner, (b) lint and tests on `ubuntu-latest`,
  pipeline runs on the VM.
- Decision: (b). Pull-request checks never queue behind a pipeline run, and they prove the
  package installs on a clean machine. Pipeline workflows share `concurrency: vm-pipeline` so
  two runs never write the same Delta table.
- Consequence: tests must be self-contained (tiny fixtures, temp lake), with no access to `/data`.

## D-06 Bronze keeps every column as a string
- Date: 2026-10-23 · Author: Lucas · Issue: AIS-13
- Context: day-first timestamps and placeholder text (`Unknown value`, `Undefined`) in
  numeric columns.
- Options: (a) infer the schema, (b) a typed schema in Bronze, (c) all strings in Bronze.
- Decision: (c). Nothing is coerced or lost at ingestion; every parsing rule lives in Silver,
  where it is tested and counted.
- Consequence: Silver casts explicitly. Ingesting 2026-09-01 gave 18,658,332 rows, equal to the
  CSV data lines, with 0 malformed rows.

## D-07 Dev sample: 15 km around the ports plus 0.5 % random
- Date: 2026-11-02 · Author: Lucas · Issue: AIS-15
- Context: a prod day is 18.7 M rows; the dev workflow needs a real input that runs in minutes
  but still produces port calls.
- Options: (a) a pure random sample, (b) a spatial cut around the ports, (c) (b) plus a random
  share of the rest.
- Decision: (c), 15 km and 0.5 %, seed 535: 976,985 rows (5.24 % of the day). 15 km covers the
  largest anchorage radius (12 km, Esbjerg), so port calls are complete in dev. A random
  sample would break vessel tracks. Measurements: [profiling.md](profiling.md).
- Consequence: dev numbers are not representative of open-sea volume; performance is measured
  on prod days only.

## D-08 Unparseable timestamps become null, not errors
- Date: 2026-11-08 · Author: Lucas · Issue: AIS-17
- Context: Spark 3.5's default `timeParserPolicy=EXCEPTION` can raise on a value that the
  old parser would have read, which would fail a whole day for one bad row.
- Options: (a) keep the default, (b) `LEGACY`, (c) `CORRECTED`.
- Decision: (c), set in `build_spark`. A value that does not match `dd/MM/yyyy HH:mm:ss`
  becomes null and is rejected as `BAD_TIMESTAMP`, so it is counted.
- Consequence: the fixture's ISO-8601 timestamp lands in `silver.rejects` with its reason.

## D-09 A reject code for malformed CSV lines
- Date: 2026-11-08 · Author: Lucas · Issue: AIS-18
- Context: Bronze keeps lines with the wrong field count in `_corrupt_record` (PERMISSIVE mode).
  No A.4 rule covered them, so they would have fallen into a misleading code
  (e.g. `BAD_POSITION`).
- Decision: `MALFORMED_ROW`, checked first. 2026-09-01 had 0 such lines, but the rule makes the
  outcome explicit if a future file has them.

## D-10 Verbatim raw values travel with each row
- Date: 2026-11-08 · Author: Lucas · Issue: AIS-17, AIS-19
- Context: rejects must be stored exactly as received, and "exact duplicate" must mean the
  same message, not the same values after cleaning (`Unknown` and an empty field both become
  null).
- Decision: `to_typed` packs the 26 Bronze columns into a `_raw` struct and adds `_row_hash`
  (SHA-256 of the verbatim columns, null-safe). Exact dedup is `dropDuplicates(_row_hash)`;
  the conflict tie-break uses the same hash.
- Consequence: one wide struct column until the final projection, which drops it.

## D-11 Row conservation is measured, not derived
- Date: 2026-11-08 · Author: Lucas · Issue: AIS-21
- Context: `rows_in = rejects + exact duplicates + dropped conflicts + accepted` proves nothing
  if one term is computed by subtraction.
- Decision: each term comes from a different measurement: rejects and distinct valid hashes
  in the bronze_in pass, the `(mmsi, ts_utc)` group sizes kept on the surviving row in the
  silver_out pass, accepted rows in the same pass. `unaccounted` must be 0 and is logged.
  Each checkpoint is one `agg` (conditional sums), so the metrics cost two scans, not one per
  metric.
- Consequence: fixture 259 = 7 + 2 + 1 + 249 exactly; the integration test recounts every term
  with separate jobs.

## D-12 dim_vessel versions can change within a day
- Date: 2026-11-08 · Author: Lucas · Issue: AIS-25
- Context: the plan was one "most frequent" attribute combination per vessel and day. The
  fixture has a name change at 00:20 (22 rows before, 5 after) that this would hide.
- Options: (a) daily mode, (b) every distinct combination seen at least N times in the day
  opens a version at its first sighting.
- Decision: (b) with `features.scd2_min_reports = 2`; a single garbled message does not open a
  version. A vessel's first version starts at midnight of its first day, so earlier positions
  of that day still join. Closing the old version and inserting the new one is a single Delta
  `MERGE` (Spark SQL), so readers never see zero or two current versions. Versions not later
  than the current one are ignored, which makes a rerun a no-op (days run in date order).
- Consequence: noisy static data could create many versions; the number of versions per
  vessel and day is measured on real data (dq metric `dim_vessel_inserted`).

## D-13 Zones by one broadcast left join
- Date: 2026-11-08 · Author: Lucas · Issue: AIS-27
- Context: the plan was a cross join with all ports, then the nearest port per row, which
  needs an aggregation (a shuffle) after the join.
- Decision: the three anchorage circles do not overlap (checked at load), so a position matches
  at most one port. A left join against the broadcast `dim_port` with the condition
  "inside the anchorage circle" gives the answer directly; no match means `at_sea`. A
  bounding-box comparison runs before the trigonometry (measured in P2).
- Consequence: adding a port whose area overlaps another fails fast with `RefDataError`.

## D-14 Port calls split on time away from the port
- Date: 2026-11-08 · Author: Lucas · Issue: AIS-34
- Context: Silver's `gap_s` is the time since the previous fix anywhere, so a ship reporting
  every 10 s at sea never shows a gap, even after it has left the port.
- Decision: the call detector filters `zone = 'port'` first and measures the gap between
  consecutive in-port fixes: a new call starts when the port changes or the vessel was not
  in port for more than `port_call_gap_h` (2 h). Calls under 30 min are dropped.
- Consequence: a ship that leaves and returns within 2 h stays one call (e.g. a shift
  between berths).

## D-15 Gold publishes only gated dates
- Date: 2026-11-08 · Author: Lucas · Issue: AIS-36
- Context: a broken or partial day must not reach the dashboard.
- Decision: a Silver date is published only if it passes three gates from `ops.dq_metrics`:
  24 hours present, in-scope reject rate ≤ 5 %, and rows ≥ 0.5 × the median of the other
  days. `port_calls` is fully recomputed from the gated dates (small after the zone filter);
  `port_congestion_hourly` is rewritten with `replaceWhere` only for gated dates, so a failing
  date keeps its previous Gold rows and is logged `stale` in `ops.pipeline_runs`.

## D-16 Experiments never write to the prod lake
- Date: 2026-11-08 · Author: Lucas · Issue: AIS-29
- Context: P4 rebuilds Silver eight times; the other experiments only read.
- Decision: `perf.experiments` reads `/data/ais/prod` and writes only under
  `/data/ais/perf` (`ops.perf_results`, P4 tables). Each variant has an unrecorded warm-up run,
  then 3 measured runs; the report uses the median.
