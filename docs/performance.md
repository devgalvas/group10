# Performance experiments

Every optimisation is measured, including the ones that do not help. Each entry follows the
same format: hypothesis → baseline → change → result → verdict. Timings are wall-clock on the
VM (`local[4]`, 5 GB driver), from the Spark event log in the History Server (port 18080).

| ID | Topic | Issue | Status |
|---|---|---|---|
| P1 | Raw format (CSV vs Delta) and partition pruning | AIS-30 | todo |
| P2 | Broadcast vs sort-merge joins to the dimensions | AIS-31 | todo |
| P3 | Skew in the per-vessel window | AIS-32 | todo |
| P4 | Shuffle partitions sized for 4 cores | AIS-33 | todo |
| P5 | Python UDF vs built-in functions (haversine) | AIS-33 | todo |
