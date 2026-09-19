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

## Week 3
- Streaming features (`jobs/03_streaming_features.py`) split into two categories deliberately: row-level features (balance deltas, `balance_mismatch_orig`) need no windowing; account velocity (txn count/volume over trailing 1h/24h) needs real stateful streaming aggregation with a watermark. Order-dependent features like time-since-last-transaction are deferred to the offline training pipeline (Week 4) since they need unbounded lookback, not a bounded window — an online/offline feature split, not a shortcut.
- Derived `event_time` from PaySim's `step` field (hours since simulation start) rather than using `_ingested_at`, so velocity windows reflect the transaction's own simulated time, not simulator/wall-clock arrival time.
- Flagged for the model card: PaySim zeroes destination balances specifically on fraudulent transactions, so balance-based features will likely correlate with `isFraud` more strongly than real bank data would support — a known artifact of the synthetic dataset.
- Known practical limitation of the current simulator: PaySim's rows are ordered by `step` with ~8,500 rows per simulated hour on average, so at the default `BATCH_SIZE=200`, watermark progress is very slow and 1h/24h velocity windows won't finalize without running a large number of batches. Testing workaround documented in the notebook (temporarily raise `BATCH_SIZE`); a cleaner permanent fix (batching by `step` instead of fixed row count) is a candidate for later polish, not done yet.

## Correction to the Week 2 finding
While building Week 3, found a real gap in the simulator: `run()` always
read the CSV from the top on every script invocation, with no memory of
prior progress. Since the simulator was restarted multiple times during
Week 1-2 debugging (host typo fix, initial testing), some of the
"1000 distinct combinations" finding from Week 2 was likely caused by
this — genuine restart-duplication — not purely PaySim's own value
collisions as originally documented. Both effects are probably real and
overlapping; the exact split isn't knowable retroactively from the data
already ingested. Fixed going forward: the simulator now persists
`rows_emitted`/`batch_index` to `data/.simulator_state.json` and resumes
from there on restart, so this won't recur. Added `--reset` to
intentionally start over when wanted.
- Verified the streaming feature engineering works end-to-end: row-level features populated (6000 rows), and 1h account velocity windows finalized correctly (14,888 windows) — confirming the watermarked stateful aggregation genuinely works, not just runs without erroring. 24h velocity table remained empty on this run, which is expected (a 24h window needs the watermark to pass ~50 simulated hours before the first window closes) rather than a defect — the same logic that proved out on 1h windows applies directly once the feed runs long enough. Revisit observing a populated 24h table once Week 6 orchestration has the pipeline running continuously.

Week 3 complete.
