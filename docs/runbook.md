# Runbook: operating the pipeline

Everything runs on the course VM (`ssh dat535-vm`, reachable from the UiS network or VPN).

## Environment

```bash
unset SPARK_HOME                                   # use the pip PySpark jars, not /opt/spark
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
source ~/ais-env-dev/bin/activate                  # or ~/ais-env-prod
```

| Path | What |
|---|---|
| `/data/ais/dev`, `/data/ais/prod` | data roots: `landing/` (zips) and `lake/` (Delta) |
| `/data/ais/perf` | performance experiments only |
| `/data/spark-events` | Spark event logs (History Server) |
| `/opt/ais-pipeline/releases/<sha>` | deployed releases; `current` points to the live one |

## One-time setup

```bash
sudo bash scripts/vm/setup_data_volume.sh       # /data (AIS-07)
bash scripts/vm/setup_python_envs.sh            # venvs + Java 17 (AIS-08)
sudo bash scripts/vm/setup_history_server.sh    # :18080 (AIS-09)
sudo bash scripts/vm/setup_release_dirs.sh      # /opt/ais-pipeline + dashboard unit (AIS-38/39)
```

## Services

| Service | Port | Command |
|---|---|---|
| Spark History Server | 18080 | `sudo systemctl status spark-history` |
| Dashboard (Streamlit) | 8501 | `sudo systemctl restart ais-dashboard` |

Both ports are open only to the UiS CIDR in the OpenStack security group.

## Workflows

| Workflow | Trigger | What it runs |
|---|---|---|
| `ci.yml` | pull request to `dev`/`main` | ruff + pytest on GitHub-hosted runners |
| `dat535-dev.yml` | push to `dev` | bronze → silver → gold on the dev sample, dq-report |
| `dat535-prod.yml` | push to `main` (approval) | deploys a release to `/opt/ais-pipeline` |
| `scheduled.yml` | every 6 h, or manual | next unprocessed day of the prod window, or a range |
| `vm-smoke.yml` | manual | runner + venv + Spark/Delta check |

All VM workflows share `concurrency: vm-pipeline`, so two runs never write the same table.

## Common tasks

| Task | Command (from `/opt/ais-pipeline/current` for prod) |
|---|---|
| Next day of the window | `ais-pipeline --env prod run --next-day` |
| Backfill a range | `ais-pipeline --env prod run --start 2026-09-01 --end 2026-09-07` |
| Re-run one stage | `ais-pipeline --env prod run --start D --end D --stages silver,gold` |
| Force a re-ingest | `ais-pipeline --env prod bronze --start D --end D --force` |
| Quality report | `ais-pipeline --env prod dq-report --date D` |
| Smoke check | `ais-pipeline --env prod check` |

Reruns are safe: Bronze skips a file with the same SHA-256, Silver and Gold replace their
date partitions, and `dim_vessel` ignores versions it already has. Silver dates must be
processed in date order (SCD2 and the previous-day context depend on it); `run` does that.

A backfill from GitHub: Actions → *DAT535 — Scheduled Pipeline* → *Run workflow*, with
`start`, `end` and `stages`.

## Reading the logs

```python
spark.read.format("delta").load("/data/ais/prod/lake/ops/pipeline_runs").orderBy(
    "ended_at", ascending=False
).show(20, truncate=False)
```

`status` is `success`, `failed` (with the error in `message`), `skipped` (nothing to do) or
`stale` (the date failed a quality gate; Gold kept its previous rows). The dashboard's
*Pipeline health* tab shows the same table.

## Rolling back a release

```bash
ls -1dt /opt/ais-pipeline/releases/*/            # newest first; the last 3 are kept
ln -sfn /opt/ais-pipeline/releases/<old-sha> /opt/ais-pipeline/current.tmp
mv -Tf /opt/ais-pipeline/current.tmp /opt/ais-pipeline/current
~/ais-env-prod/bin/pip install "/opt/ais-pipeline/current[dashboard]"
sudo systemctl restart ais-dashboard
```

## Performance experiments

```bash
cd ~/group10 && ~/ais-env-dev/bin/pip install -e ".[perf]"
~/ais-env-dev/bin/python -m perf.experiments --day 2026-09-01 P1 P2 P3 P5 P6
~/ais-env-dev/bin/python -m perf.experiments --day 2026-09-01 --runs 1 P4
~/ais-env-dev/bin/python -m perf.report --out-root /data/ais/perf
```

They read the prod lake and write only under `/data/ais/perf` and `docs/perf/`.
