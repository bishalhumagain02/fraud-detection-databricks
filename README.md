# Real-Time Fraud Detection on Databricks (Free Edition)

A data engineering portfolio project: a live-replayed transaction stream flows
through a bronze/silver/gold Delta Lake pipeline on Databricks, with streaming
feature engineering and near-real-time fraud scoring.

## Why this project

Real fraud data is never public, so this project uses the industry-standard
approach: replay a realistic historical dataset ([PaySim][paysim]) as a live
feed, and build the same streaming pipeline shape a payments company would
run against a real transaction stream.

[paysim]: https://www.kaggle.com/datasets/ealaxi/paysim1

## Architecture

```
[replay simulator] -> [UC volume, raw files]
        |
        v
   [bronze: Auto Loader, raw + ingestion metadata]
        |
        v
   [silver: cleaned, deduped, typed, DQ expectations]
        |
        v
   [streaming feature engineering: rolling windows, watermarking]
        |
        v
   [streaming scoring: MLflow pyfunc model]
        |
        v
   [gold: scored_transactions, aggregates] -> [Databricks SQL dashboard]

[offline: historical data -> MLflow training -> Unity Catalog model registry]
        (registered model is loaded by the streaming scoring step)
```

The replay simulator runs **outside** the Databricks workspace (locally, or
on a scheduled GitHub Actions job) and lands files into a Unity Catalog
Volume. This sidesteps Databricks Free Edition's restricted outbound
internet — the Databricks side never needs to reach an external API itself.

## 7-week plan

| Week | Focus | Deliverable |
|---|---|---|
| 1 | Foundations & live ingestion | Replay simulator landing files continuously; Auto Loader picking them up |
| 2 | Bronze & silver | Bronze + silver Delta tables updating continuously, DQ summary |
| 3 | Streaming feature engineering | Feature table streaming continuously (velocity, balance-delta z-scores, time-since-last-txn) |
| 4 | Offline model training | Registered model in Unity Catalog, honest precision/recall/PR-AUC evaluation, model card |
| 5 | Streaming scoring | End-to-end flow: simulator -> features -> scored predictions |
| 6 | Dashboard, orchestration & monitoring | Scheduled pipeline (Lakeflow/Jobs), live SQL dashboard, basic DQ/drift monitoring |
| 7 | Polish & packaging | README/architecture diagram, decisions doc, demo recording, GitHub push |

See `docs/DECISIONS.md` for architecture decisions and findings as the build
progresses (filled in week by week).

## Repo layout

```
src/simulator/   replay simulator (external ingestion)
src/features/    streaming feature engineering
src/ml/          offline training pipeline (MLflow)
src/serving/     streaming scoring
notebooks/       exploratory notebooks (PaySim EDA, feature validation)
jobs/            Databricks job / Lakeflow pipeline definitions
docs/            architecture decisions, model card
tests/           unit tests
data/raw/        local landing zone (gitignored) — PaySim CSV goes here
```

## Setup

1. **Databricks Free Edition** — sign up at databricks.com/learn/free-edition
   if you haven't already. Create a catalog/schema for this project and a
   Unity Catalog Volume to use as the landing zone (e.g.
   `fraud_project.raw.landing`).
2. **PaySim dataset** — download from Kaggle (`ealaxi/paysim1`) and place the
   CSV at `data/raw/paysim.csv` (gitignored, not committed).
3. **Environment variables** — copy `.env.example` to `.env` and fill in:
   - `DATABRICKS_HOST` — your workspace URL
   - `DATABRICKS_TOKEN` — a personal access token
   - `UC_VOLUME_PATH` — the Volume path to land files in, e.g.
     `/Volumes/fraud_project/raw/landing`
4. **Install dependencies**: `pip install -e .`
5. **Run the simulator**: `python -m src.simulator.replay_simulator`

## Status

Week 1 in progress — see `docs/DECISIONS.md` for the running log.
