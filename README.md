# Maritime Traffic & Port Congestion Pipeline (AIS)

DAT535 Data Engineering, University of Stavanger, Group 10.

A batch Medallion pipeline in PySpark and Delta Lake. It ingests the daily AIS files published by
the Danish Maritime Authority, cleans and models them into a Silver layer of vessel positions and
vessel/port dimensions, and builds Gold tables of port calls and hourly congestion for Esbjerg,
Aarhus and Copenhagen. A Streamlit dashboard reads the Gold tables.

```
DMA zip ─► landing ─► bronze.ais_raw ─► silver.vessel_position ─► gold.port_calls
                       (all strings,     silver.dim_vessel (SCD2)   gold.port_congestion_hourly
                        lineage)         silver.dim_port                    │
                                         silver.rejects                     ▼
                       ops.ingestion_log · ops.dq_metrics · ops.pipeline_runs   dashboard
```

## Quick start (laptop, on the bundled fixture)

Needs Python 3.11 and Java 17.

```bash
git clone https://github.com/devgalvas/group10.git && cd group10
python3.11 -m venv .venv && source .venv/bin/activate
make install                     # pip install -e ".[dev,dashboard]"
make lint test-all               # ruff + every test (about 10 minutes)
```

Run the whole pipeline on the 259-row fixture in a throwaway lake:

```bash
export AIS_DATA_ROOT=/tmp/ais-demo
mkdir -p $AIS_DATA_ROOT/landing/date=2026-09-01
python -c "import zipfile; z = zipfile.ZipFile('$AIS_DATA_ROOT/landing/date=2026-09-01/aisdk-2026-09-01.zip', 'w', zipfile.ZIP_DEFLATED); z.write('tests/fixtures/ais_tiny.csv', 'aisdk-2026-09-01.csv'); z.close()"
ais-pipeline --env ci run --start 2026-09-01 --end 2026-09-01 --stages bronze,silver,gold
ais-pipeline --env ci dq-report --date 2026-09-01
streamlit run dashboard/app.py -- --env ci
```

## Real data

Two values to set, in `config/<env>.yaml` or the environment:

| Setting | Where | Example |
|---|---|---|
| data root | `data_root:` or `$AIS_DATA_ROOT` | `/data/ais/prod` |
| date window | `window: {start, end}` | `2026-09-01` … `2026-09-28` |

```bash
ais-pipeline --env prod run --start 2026-09-01 --end 2026-09-01   # download → bronze → silver → gold
ais-pipeline --env prod run --next-day                            # what the schedule runs
```

| Command | Does |
|---|---|
| `download` / `bronze` / `silver` / `gold` | one stage for a date range |
| `run` | the stages in order, logged to `ops.pipeline_runs` |
| `dq-report --date D` | data quality before and after Silver |
| `check` | Spark + Delta smoke test |

## Documentation

- [Runbook](docs/runbook.md): VM, workflows, backfills, rollback
- [Decision log](docs/decisions.md)
- [Silver model](docs/silver_model.md)
- [Profiling](docs/profiling.md) and [performance](docs/performance.md)
- [Team guide and original plan](docs/TEAM_GUIDE.md)
- Archived course lab material: [labs/](labs/)
