Week 1 in progress.

## Week 1
- Unity Catalog structure: catalog `fraud_project`, schemas `raw`/`bronze`/`silver`/`gold`, landing volume `raw.landing`
- Replay simulator confirmed working end-to-end: uploads batches of PaySim rows as timestamped JSONL files to the landing volume via the Databricks SDK
- Bronze ingestion (`jobs/01_bronze_ingestion.py`): Auto Loader (`cloudFiles`) reads JSON from `raw.landing`, appends into `fraud_project.bronze.transactions` with `_ingested_at` and `_source_file` metadata columns, no transformation. Uses `trigger(availableNow=True)` for iterative testing; will switch to a scheduled `processingTime` trigger once orchestration (Week 6) is in place.

## Week 2
- Silver transform (`jobs/02_silver_transform.py`): reads bronze as a stream, applies defensive type casting (try_cast, so bad values become null instead of failing the job), builds a synthetic `transaction_id` (sha2 hash of business fields, since PaySim has no natural unique ID), dedupes with `dropDuplicatesWithinWatermark` watermarked on `_ingested_at` (PaySim's own `step` field is a simulated hour index, not a real clock, so it can't be used as a watermark)
- DQ checks (amount > 0, type in known set, non-null names, valid fraud flag) split the stream into `silver.transactions` (clean) and `silver.transactions_quarantine` (failed rows, tagged with a reason) — nothing is silently dropped
- Fixed a real runtime bug: `pyspark.sql.functions.try_cast` (the Python wrapper) isn't available on the Databricks serverless runtime in use — swapped to a small `try_cast()` helper using `F.expr("try_cast(col AS type)")`, which calls the same underlying SQL function through a route that's supported everywhere
- Investigated the bronze(1400) -> silver-clean(1000) gap: confirmed it's genuine, not a bug. Distinct business-field combinations in bronze = exactly 1000, matching silver's clean count exactly. PaySim's `step` field only spans 744 values (31 simulated days x 24h) with limited account/amount granularity, so identical-value collisions across otherwise-different transactions are statistically expected in this synthetic dataset — a known limitation of using PaySim rather than real transaction data (which would have a real transaction ID and finer timestamp precision). Documented here rather than treated as a defect. Quarantine table is empty (0 rows) — PaySim is clean enough that no rows failed the DQ checks on this run.

Week 2 complete.
