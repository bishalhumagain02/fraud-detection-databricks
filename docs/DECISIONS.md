Week 1 in progress.

## Week 1
- Unity Catalog structure: catalog `fraud_project`, schemas `raw`/`bronze`/`silver`/`gold`, landing volume `raw.landing`
- Replay simulator confirmed working end-to-end: uploads batches of PaySim rows as timestamped JSONL files to the landing volume via the Databricks SDK
- Bronze ingestion (`jobs/01_bronze_ingestion.py`): Auto Loader (`cloudFiles`) reads JSON from `raw.landing`, appends into `fraud_project.bronze.transactions` with `_ingested_at` and `_source_file` metadata columns, no transformation. Uses `trigger(availableNow=True)` for iterative testing; will switch to a scheduled `processingTime` trigger once orchestration (Week 6) is in place.
