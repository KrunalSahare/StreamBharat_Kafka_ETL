# StreamBharat Real-Time ETL: Kafka + PySpark + Delta Lake

A complete, laptop-sized data platform for the StreamBharat OTT service. The player SDK, billing, the payment gateways, search, ratings and the ad servers publish events to **Kafka**; your **PySpark** jobs build a **Delta Lake** (bronze → silver → gold), publish live trending titles and ad-pacing alerts back to Kafka, load a **Postgres** warehouse for **Metabase** dashboards, and are scheduled by **Airflow**.

The infrastructure is ready. The ETL jobs are yours to build: follow the 20 questions in the workbook
("StreamBharat Real-Time ETL"), which has hints but no solutions.

```
 Source systems (producers/)        Kafka topics                       Your Spark jobs (sbetl/jobs/)        Serving
 ───────────────────────────        ───────────────────────            ─────────────────────────────        ─────────────
 Player SDK ──────────────────────► ott.player.events ───────┐
 Billing / payment gateways ──────► ott.billing.subscriptions, ott.billing.settlements
 Search / ratings ────────────────► ott.app.search, ott.app.ratings ──► bronze ─► silver ─► gold ─────────► Postgres (ottdw)
 Ad decision server / tracking ───► ott.ads.breaks, ott.ads.impressions   (Delta Lake in ./lake)            └► Metabase
 Live feeds (live.py) ────────────► ott.live.plays, ott.live.ads ──► real-time jobs ───────────────────────► ott.metrics.trending
 Batch files (catalog, licences, users, CDN logs, reference, history) ──► batch jobs                       ott.alerts.ad_pacing
                                                                         Airflow schedules the batch jobs   bad records ► ott.dlq
```

## What you need

- Docker Desktop (Windows/Mac) or Docker Engine + Compose v2 (Linux)
- **8 GB RAM free for Docker** for the core stack; 12–16 GB to also run Airflow and Metabase
- About 6 GB of disk for images
- The StreamBharat dataset (`StreamBharat_dataset.zip`)

Apple silicon (M1–M4) Macs work: every image used has an arm64 build.

## Setup (first time, about 15 minutes)

1. Unzip this kit. Open a terminal in the `StreamBharat_Kafka_ETL` folder.
2. Unzip `StreamBharat_dataset.zip` and copy its `data/raw` folder here, so that data/raw/player/, data/raw/content/, data/raw/billing/ and data/raw/reference/ exist.
3. Build the Spark image (downloads Spark, the Kafka/Delta/Postgres jars and Python packages):
   ```
   docker compose build
   ```
4. Start the core stack:
   ```
   docker compose up -d
   ```
   `kafka-init` creates the topics and exits; that is normal.
5. Check everything works:
   ```
   docker compose exec workspace python -m sbetl.smoke
   ```
   You should see `ALL CHECKS PASSED` (Kafka will show 0 messages until you replay).
6. Start the source systems (in a second terminal; leave it running):
   ```
   docker compose exec workspace python producers/replay.py
   ```
   This replays **October 2026** at one simulated day per minute (about 31 minutes in total).

With `make` installed, steps 3–6 are `make build`, `make up`, `make check`, `make replay`.
On Windows without `make`, use the `docker compose ...` commands shown here and in the `Makefile`.

## Web pages

| What | URL | Login |
| --- | --- | --- |
| Jupyter Lab (your workspace) | http://localhost:8888 | none |
| Kafka UI (topics, messages, consumer lag) | http://localhost:8080 | none |
| Spark master UI | http://localhost:8081 | none |
| Spark job UI (while a job runs in the workspace) | http://localhost:4040 | none |
| Airflow (profile `airflow`) | http://localhost:8088 | admin / admin |
| Metabase (profile `bi`) | http://localhost:3000 | create on first visit |
| Postgres `ottdw` (from your laptop) | localhost:5432 | ott / ott |
| Kafka (from your laptop) | localhost:9094 | none |

Inside the Docker network the addresses are `kafka:9092`, `postgres:5432` and `spark://spark-master:7077`.

## Folder layout

```
StreamBharat_Kafka_ETL/
├── docker-compose.yml        the whole stack
├── docker/                   Spark image, topic creation script, Postgres init
├── conf/pipeline.yaml        topic names, paths, warehouse connection (one place for names)
├── producers/
│   ├── replay.py             replays the dataset into Kafka (the source systems)
│   ├── sources.py            which files feed which topic, with which key
│   ├── replay_engine.py      the replay clock and Kafka producer (no need to change)
│   ├── live.py               live feeds with the current time
│   └── live_sink.py          Kafka output for live.py
├── sbetl/
│   ├── common/spark.py       get_spark(): Spark session with Delta, Kafka and Postgres ready
│   ├── common/config.py      load_config()
│   ├── smoke.py              stack check
│   └── jobs/                 one file per job: empty, for you to build (each says which question)
├── dags/                     Airflow DAGs (sb_stack_check.py is a working example)
├── notebooks/                00_start_here.ipynb
├── tests/                    pytest (conftest.py gives you a `spark` fixture)
├── sql/                      put your warehouse DDL and views here
├── data/raw/                 the StreamBharat dataset (you copy it here)
└── lake/                     your Delta tables and checkpoints (created by your jobs)
```

The whole folder is mounted at `/opt/project` in every Spark container, so a path like
`/opt/project/lake/silver/...` is the same on the workspace, the worker and Airflow's job containers.

## The source systems (producers)

`producers/replay.py` merges the sources below in time order and plays them on a simulated clock.

| Topic | Partitions | Key | Source data | October 2026 volume |
| --- | --- | --- | --- | --- |
| ott.player.events | 6 | user_id | player/ (sent at server_ts) | about 425,000 |
| ott.billing.subscriptions | 3 | user_id | billing/subscription_events.json | about 2,000 |
| ott.billing.settlements | 3 | user_id | billing/gateway_settlements.csv | about 1,650 |
| ott.app.search | 3 | profile_id | search/search_logs.json | about 11,900 |
| ott.app.ratings | 1 | profile_id | app/ratings.json | about 1,200 |
| ott.ads.breaks | 3 | user_id | ads/ad_breaks.csv | about 10,700 |
| ott.ads.impressions | 3 | user_id | ads/ad_impressions.csv (sent when the ad ends) | about 9,100 |

Every message carries the headers `source_system`, `schema_version` and `sent_at`.
Like real upstream systems, the feed is not perfect. Expect to find things your pipeline must handle;
which things, and how many, is part of the work.

Useful options:

```
python producers/replay.py --from 2026-10-01 --to 2026-10-03 --speed 0     # three days, no waiting
python producers/replay.py --sources player,impressions                                # only some sources
python producers/replay.py --speed 360                                      # slower: 1 day every 4 minutes
python producers/replay.py --no-chaos                                       # a clean feed (for debugging only)
python producers/replay.py --dry-run --speed 0                              # no Kafka: writes _dryrun/*.jsonl
```

Replaying the same days twice puts the same events into Kafka twice. Your pipeline should cope with that,
but if you want a clean start use the reset below.

### Live feeds

`producers/live.py` sends events stamped with the current Indian time (Q14 and Q15):

```
python producers/live.py --stream both
python producers/live.py --help
```

| Topic | Partitions | Key | Contents |
| --- | --- | --- | --- |
| ott.live.plays | 6 | user_id | event_id, user_id, profile_id, content_id, language, event_ts |
| ott.live.ads | 3 | user_id | impression_id, user_id, campaign_id, ad_break_ts, duration_sec, cpm_inr |

### Topics your jobs write

| Topic | Partitions | Contents |
| --- | --- | --- |
| ott.dlq | 1 | messages your jobs cannot parse (Q4) |
| ott.metrics.trending | 1 | live top titles per language (Q14) |
| ott.alerts.ad_pacing | 1 | ad overspend and frequency-cap alerts (Q15) |
| ott.ref.subscription_state | 3 | current subscription state per user, compacted (Q8) |

## Your jobs

| File in sbetl/jobs/ | Question | What it does |
| --- | --- | --- |
| bronze_ingest.py | Q3, Q4 | Kafka topics -> bronze Delta tables, with a dead-letter path for broken messages. |
| silver_player.py | Q5 | bronze player events -> silver.player_events: clocks corrected, de-duplicated, both schema versions. |
| batch_master.py | Q6 | Catalog snapshots, licences, users, profiles and reference data -> silver (batch). |
| backfill_player.py | Q7 | July - September player files + October from Kafka -> one silver.player_events. |
| subscription_state.py | Q8 | Subscription events -> silver.subscriptions (SCD2) and the compacted state topic. |
| sessions.py | Q9 | Player events -> viewing sessions and watch time in real time. |
| ad_matching.py | Q10 | Match ad breaks with impressions; fill rate and frequency caps. |
| quality.py | Q11 | Reusable data quality rules, quarantine and a quality metrics table. |
| gold_watch_time.py | Q12 | gold.watch_time_daily, gold.dau_mau, gold.top_titles. |
| gold_revenue.py | Q13 | gold.revenue_reconciliation and gold.royalties. |
| trending.py | Q14 | Live trending titles from ott.live.plays -> ott.metrics.trending. |
| ad_pacing.py | Q15 | Ad pacing and frequency-cap alerts from ott.live.ads -> ott.alerts.ad_pacing. |
| serve_postgres.py | Q16 | Load gold tables into Postgres (ottdw) for dashboards. |
| monitoring.py | Q18 | Streaming query metrics and lag -> a monitoring table. |

## Running your jobs

```
docker compose exec workspace bash               # a shell in the workspace
python -m sbetl.jobs.bronze_ingest               # run a job (local[*] Spark inside the workspace)
SPARK_MASTER=spark://spark-master:7077 python -m sbetl.jobs.gold_watch_time    # run on the Spark cluster
spark-submit --master spark://spark-master:7077 sbetl/jobs/gold_watch_time.py  # the spark-submit way
pytest -q                                        # tests
```

Notebooks: open http://localhost:8888, and in the first cell use `from sbetl.common.spark import get_spark`.

Keep each streaming query's checkpoint under `lake/_checkpoints/<query name>`. Never share a checkpoint between two queries.

## Airflow and Metabase

```
cp .env.example .env          # then set PROJECT_DIR to the absolute path of this folder
docker compose --profile airflow --profile bi up -d
```

Airflow does not run Spark itself: each task starts a short-lived container from the Spark image through
the `docker-proxy` service (see `dags/sb_stack_check.py`). Trigger `sb_stack_check` in the Airflow UI to
confirm it works before writing your own DAGs.

In Metabase, add a PostgreSQL database: host `postgres`, port `5432`, database `ottdw`, user `ott`, password `ott`.

## Stopping and resetting

```
docker compose stop                  # pause; everything is kept
docker compose down                  # remove containers: Kafka topics are emptied, lake/ and Postgres are kept
make reset                           # wipe Kafka, lake/ and all checkpoints, then start again
```

After `docker compose down`, Kafka starts empty but your checkpoints still remember old offsets.
Either `make reset`, or delete the affected `lake/_checkpoints/...` folders before restarting your streams.

## Troubleshooting

| Problem | Fix |
| --- | --- |
| Containers exit with code 137 | Docker is out of memory. Give it more RAM (Docker Desktop → Settings → Resources) or stop Airflow/Metabase. |
| `port is already allocated` | Something else uses that port (often 8080 or 5432). Stop it or change the left-hand port in docker-compose.yml. |
| Smoke test: `missing topics` | Run `docker compose up kafka-init` and check its logs. |
| Smoke test: dataset not found | `data/raw/reference/plans.csv` must exist in this folder. |
| Stream fails with offsets `out of range` / data loss | Kafka was recreated but the checkpoint is old. Delete that query's checkpoint folder (or `make reset`). |
| Airflow task fails with a mount error | `PROJECT_DIR` in `.env` is not the absolute path of this folder. On Windows use forward slashes. |
| Jupyter kernel dies on big reads | Use `get_spark(master="spark://spark-master:7077")` or read fewer days at a time. |
| Build fails downloading jars | Your network blocks Maven Central; retry on another network. |

Only one kit can run at a time (they use the same ports). Run `docker compose down` in one kit's folder before starting another.

## Notes

- All names and numbers are synthetic.
- Versions: Spark 3.5.3, Delta Lake 3.2.1, Kafka 3.8 (KRaft), Postgres 16, Airflow 2.10, Metabase 0.50.
"# StreamBharat_Kafka_ETL" 
