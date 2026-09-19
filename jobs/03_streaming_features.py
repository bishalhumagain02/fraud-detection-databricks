# Databricks notebook source
# MAGIC %md
# MAGIC # Streaming feature engineering — fraud_project
# MAGIC
# MAGIC Two kinds of features, computed differently on purpose:
# MAGIC
# MAGIC 1. **Row-level features** (`gold.transaction_features`) — simple
# MAGIC    per-transaction derived values (balance deltas, a balance-mismatch
# MAGIC    flag). No windowing needed, so these're just a `withColumn` chain.
# MAGIC 2. **Windowed account velocity** (`gold.account_velocity_1h`,
# MAGIC    `gold.account_velocity_24h`) — transaction count/volume per account
# MAGIC    over trailing time windows. This is real stateful streaming: Spark
# MAGIC    has to hold partial window state until the watermark says a window
# MAGIC    is done, then emit it.
# MAGIC
# MAGIC `time_since_last_transaction` and similar arbitrary-lookback features
# MAGIC are deliberately **not** computed here — they need unbounded history per
# MAGIC account, which doesn't fit a watermarked streaming aggregation. Those
# MAGIC get computed in the offline training pipeline (Week 4) using window
# MAGIC functions over the full historical table instead. This split — cheap,
# MAGIC bounded-lookback features online; expensive, unbounded-lookback
# MAGIC features offline — mirrors how real feature stores are usually split.
# MAGIC
# MAGIC ### Why event_time instead of _ingested_at here
# MAGIC The velocity features are meant to answer "how active was this account
# MAGIC in the hour *around this transaction*", which has to be measured in the
# MAGIC transaction's own simulated time (PaySim's `step`, hours since
# MAGIC simulation start) — not in wall-clock arrival time, which only reflects
# MAGIC when the replay simulator happened to send the batch.
# MAGIC
# MAGIC ### Testing tip
# MAGIC PaySim's ~6.3M rows are ordered by `step` and span only ~744 distinct
# MAGIC hour values — so with the default `BATCH_SIZE=200`, most batches land
# MAGIC well within a single simulated hour and the watermark barely moves.
# MAGIC To actually see a finalized 1h/24h window without running hundreds of
# MAGIC batches, temporarily raise `BATCH_SIZE` in `.env` (e.g. to 5000+) so
# MAGIC each batch spans multiple simulated hours, then set it back afterward.

# COMMAND ----------

CATALOG = "fraud_project"
SILVER_TABLE = f"{CATALOG}.silver.transactions"
FEATURES_TABLE = f"{CATALOG}.gold.transaction_features"
VELOCITY_1H_TABLE = f"{CATALOG}.gold.account_velocity_1h"
VELOCITY_24H_TABLE = f"{CATALOG}.gold.account_velocity_24h"

FEATURES_CHECKPOINT = f"/Volumes/{CATALOG}/raw/landing/_checkpoints/gold_transaction_features"
VELOCITY_1H_CHECKPOINT = f"/Volumes/{CATALOG}/raw/landing/_checkpoints/gold_velocity_1h"
VELOCITY_24H_CHECKPOINT = f"/Volumes/{CATALOG}/raw/landing/_checkpoints/gold_velocity_24h"

# Arbitrary reference point — PaySim's `step` is hours since simulation
# start, not a real date, so this just anchors it to something timestamp
# arithmetic can work with.
SIMULATION_START = "2026-01-01T00:00:00"

CASH_OUT_LIKE = ["CASH_OUT", "TRANSFER", "DEBIT", "PAYMENT"]

# COMMAND ----------

import datetime

from pyspark.sql import functions as F

base_epoch = int(
    datetime.datetime.fromisoformat(SIMULATION_START)
    .replace(tzinfo=datetime.timezone.utc)
    .timestamp()
)

silver_stream = spark.readStream.table(SILVER_TABLE).withColumn(
    "event_time", F.timestamp_seconds(F.lit(base_epoch) + F.col("step") * 3600)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Row-level features
# MAGIC
# MAGIC `balance_mismatch_orig` compares the balance PaySim actually recorded
# MAGIC against what simple arithmetic (old balance minus the transaction
# MAGIC amount) would predict. This is a real, well-known engineered feature
# MAGIC from published PaySim analyses — it's derived entirely from fields
# MAGIC known at transaction time, so it's a legitimate feature, not a leak.
# MAGIC
# MAGIC Worth flagging for the model card later: PaySim's own fraud-injection
# MAGIC logic zeroes out destination balances specifically for fraudulent
# MAGIC transactions, which makes balance-based features correlate with
# MAGIC `isFraud` more strongly than they realistically would with real bank
# MAGIC data. That's a known artifact of the synthetic dataset, not something
# MAGIC this pipeline introduces — but it likely means model metrics in Week 4
# MAGIC will look better than a production model would actually achieve.

# COMMAND ----------

row_features = (
    silver_stream.withColumn(
        "balance_delta_orig", F.col("newbalanceOrig") - F.col("oldbalanceOrg")
    )
    .withColumn("balance_delta_dest", F.col("newbalanceDest") - F.col("oldbalanceDest"))
    .withColumn(
        "expected_new_balance_orig",
        F.when(
            F.col("type").isin(CASH_OUT_LIKE), F.col("oldbalanceOrg") - F.col("amount")
        ).otherwise(F.col("oldbalanceOrg")),
    )
    .withColumn(
        "balance_mismatch_orig",
        F.abs(F.col("expected_new_balance_orig") - F.col("newbalanceOrig")),
    )
    .select(
        "transaction_id",
        "event_time",
        "step",
        "type",
        "amount",
        "nameOrig",
        "nameDest",
        "balance_delta_orig",
        "balance_delta_dest",
        "balance_mismatch_orig",
        "isFraud",
        "_ingested_at",
    )
)

features_query = (
    row_features.writeStream.format("delta")
    .option("checkpointLocation", FEATURES_CHECKPOINT)
    .outputMode("append")
    .trigger(availableNow=True)
    .toTable(FEATURES_TABLE)
)
features_query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Windowed account velocity
# MAGIC
# MAGIC `outputMode("append")` on a watermarked window aggregation only emits
# MAGIC a window once the watermark has passed its end — so with a short test
# MAGIC run, the most recent window(s) won't appear yet. That's expected
# MAGIC streaming behavior, not a bug: those windows are still "open" and
# MAGIC waiting for potentially-late data.

# COMMAND ----------


def build_velocity_query(window_duration, slide_duration, watermark_delay, table, checkpoint):
    agg = (
        silver_stream.withWatermark("event_time", watermark_delay)
        .groupBy(
            F.window("event_time", window_duration, slide_duration).alias("w"),
            F.col("nameOrig").alias("account_id"),
        )
        .agg(
            F.count("*").alias("txn_count"),
            F.sum("amount").alias("txn_amount_sum"),
            F.avg("amount").alias("txn_amount_avg"),
        )
        .select(
            F.col("w.start").alias("window_start"),
            F.col("w.end").alias("window_end"),
            "account_id",
            "txn_count",
            "txn_amount_sum",
            "txn_amount_avg",
        )
    )
    query = (
        agg.writeStream.format("delta")
        .option("checkpointLocation", checkpoint)
        .outputMode("append")
        .trigger(availableNow=True)
        .toTable(table)
    )
    query.awaitTermination()


build_velocity_query("1 hour", "15 minutes", "2 hours", VELOCITY_1H_TABLE, VELOCITY_1H_CHECKPOINT)
build_velocity_query("24 hours", "1 hour", "26 hours", VELOCITY_24H_TABLE, VELOCITY_24H_CHECKPOINT)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify

# COMMAND ----------

display(spark.table(FEATURES_TABLE).limit(20))

# COMMAND ----------

print(f"Row-level features: {spark.table(FEATURES_TABLE).count()}")

for label, table in [("1h velocity windows", VELOCITY_1H_TABLE), ("24h velocity windows", VELOCITY_24H_TABLE)]:
    try:
        count = spark.table(table).count()
        print(f"{label}: {count}")
        if count == 0:
            print(f"  -> table exists but is empty: no window has closed past its watermark yet")
    except Exception:
        print(f"{label}: table not created yet (no window finalized on this run)")
