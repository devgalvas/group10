# Performance experiments

Every optimisation is measured, including the ones that do not help. Each entry follows the
same format: hypothesis → baseline → change → result → verdict. Timings are wall-clock on the
VM (`local[4]`, 5 GB driver), from the Spark event log in the History Server (port 18080).

| ID | Topic | Issue | Verdict |
|---|---|---|---|
| P1 | Raw format (CSV vs Delta) and partition pruning | AIS-30 | confirmed: 0.8 s / 13 MB vs 5.4 s / 3,343 MB; pruning to repeat after the backfill |
| P2 | Broadcast vs sort-merge joins to the dimensions | AIS-31 | confirmed: 1.4 vs 2.3 s, no 190 MB shuffle; bbox 1.2 vs 1.4 s |
| P3 | Skew in the per-vessel window | AIS-32 | rejected: largest MMSI 0.22 % of the day; `repartition(16)` 2.6 vs 3.5 s via the exchange type |
| P4 | Shuffle partitions sized for 4 cores | AIS-33 | not supported: 480–525 s for all six variants, within run-to-run noise; spill grows as partitions shrink |
| P5 | Python UDF vs built-in functions (haversine) | AIS-33 | confirmed: 0.8 s built-in, 1.9 s pandas UDF, 6.1 s Python UDF |
| P6 | RDD vs DataFrame duplicate count | AIS-22 | confirmed: 27.8 vs 203.0 s; the RDD spills ~1.9 GB to disk per run, `distinct()` none |

The reasoning for each experiment (hypothesis → baseline → change → result → verdict) is in
`docs/report.md` §6. The physical plans are in `docs/perf/plans/`, and the per-MMSI
distribution for P3 is in `docs/perf/p3_rows_per_mmsi_2026-09-01.md`.

## Results, 2026-09-01

Output of `python -m perf.report --env prod --out-root /data/ais/perf` after the runs of
9–10 Nov 2026. Times are medians in seconds; sizes in MB summed over all stages.

- **P4:** one run per variant, each in its own process, without warm-up. The
  `silver_sp200_coalesce_on` row has two runs: 493.2 s (9 Nov, after a warm-up, in a shared
  session) and 570.5 s (10 Nov, first cold build). The report shows the lower one.
- **`spill (MB)`** is `memoryBytesSpilled + diskBytesSpilled` from the REST API. The memory
  term is the deserialized size of the spilled data, so the column overstates what reaches
  the disk. The bytes actually written to disk, read from the event logs
  (`/data/spark-events`): P4 130 (200 on), 129 (64 on), 301 (16 on), 4,811 (8 on), 0 (200 off),
  31 (16 off); P6 RDD ~1,900 per run; P3 before/after dedup ~880 / ~24 per run.
- **Task counts** of the P4 shuffle stages (event log): see the table in `docs/report.md` §6.
  The P3 base window runs 5 tasks after AQE coalescing; `repartition(n)` keeps n.
- **P5 `builtin`:** 6 runs, because it was measured in both P5 runs. The P5 UDF variants
  failed in the first run (`ModuleNotFoundError` in the Python workers) and passed in the
  second.

| experiment | variant | runs | median wall (s) | input (MB) | shuffle read (MB) | shuffle write (MB) | spill (MB) | max task (s) | median task (s) |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| P1 | a_csv_filter_count | 3 | 5.4 | 3,343 | 0 | 0 | 0 | 0.4 | 0.20 |
| P1 | b_delta_filter_count | 3 | 0.8 | 13 | 0 | 0 | 0 | 0.1 | 0.05 |
| P1 | c_partition_filter | 3 | 0.5 | 1 | 0 | 0 | 0 | 0.2 | 0.09 |
| P1 | d_expression_filter | 3 | 0.6 | 27 | 0 | 0 | 0 | 0.2 | 0.13 |
| P2 | asof_broadcast | 3 | 1.4 | 144 | 0 | 0 | 0 | 0.5 | 0.25 |
| P2 | asof_sortmerge | 3 | 2.3 | 144 | 190 | 190 | 0 | 0.7 | 0.54 |
| P2 | zones_bbox | 3 | 1.2 | 143 | 0 | 0 | 0 | 0.6 | 0.28 |
| P2 | zones_no_bbox | 3 | 1.4 | 143 | 0 | 0 | 0 | 0.7 | 0.48 |
| P3 | window_base | 3 | 3.5 | 143 | 193 | 193 | 0 | 1.5 | 1.29 |
| P3 | window_aqe_skew_off | 3 | 3.1 | 143 | 193 | 193 | 0 | 1.3 | 1.09 |
| P3 | window_repartition_16 | 3 | 2.6 | 143 | 193 | 193 | 0 | 0.6 | 0.37 |
| P3 | window_repartition_32 | 3 | 2.6 | 143 | 193 | 193 | 0 | 0.6 | 0.36 |
| P3 | window_repartition_64 | 3 | 2.7 | 143 | 193 | 193 | 0 | 0.5 | 0.34 |
| P3 | window_before_dedup | 3 | 312.5 | 573 | 4,757 | 4,757 | 12,503 | 50.8 | 43.21 |
| P3 | window_after_dedup | 3 | 382.9 | 573 | 6,772 | 6,772 | 5,670 | 55.1 | 48.67 |
| P4 | silver_sp200_coalesce_on | 2 | 493.2 | 19,880 | 7,639 | 7,639 | 2,586 | 45.0 | 37.68 |
| P4 | silver_sp64_coalesce_on | 1 | 525.0 | 19,621 | 7,505 | 7,505 | 1,914 | 50.1 | 44.78 |
| P4 | silver_sp16_coalesce_on | 1 | 503.4 | 19,418 | 7,405 | 7,405 | 10,352 | 51.2 | 40.10 |
| P4 | silver_sp8_coalesce_on | 1 | 502.9 | 19,342 | 7,341 | 7,341 | 25,627 | 47.9 | 41.74 |
| P4 | silver_sp200_coalesce_off | 1 | 510.0 | 19,893 | 7,651 | 7,651 | 0 | 45.4 | 42.11 |
| P4 | silver_sp16_coalesce_off | 1 | 480.3 | 19,421 | 7,405 | 7,405 | 5,670 | 45.5 | 38.14 |
| P5 | builtin | 6 | 0.8 | 116 | 0 | 0 | 0 | 0.3 | 0.13 |
| P5 | python_udf | 3 | 6.1 | 116 | 0 | 0 | 0 | 3.9 | 2.54 |
| P5 | pandas_udf | 3 | 1.9 | 116 | 0 | 0 | 0 | 1.1 | 0.55 |
| P6 | rdd_reducebykey | 3 | 203.0 | 573 | 1,269 | 1,269 | 15,397 | 25.5 | 23.41 |
| P6 | dataframe_distinct | 3 | 27.8 | 574 | 2,519 | 2,519 | 0 | 3.9 | 3.27 |
