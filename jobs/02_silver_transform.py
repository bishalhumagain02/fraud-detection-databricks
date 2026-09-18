# Databricks notebook source
# MAGIC %md
# MAGIC # Silver transform — fraud_project
# MAGIC
# MAGIC Reads `bronze.transactions` and produces two outputs:
# MAGIC - `silver.transactions` — cleaned, typed, deduped rows that pass DQ checks
# MAGIC - `silver.transactions_quarantine` — rows that failed a check, with the
# MAGIC   reason recorded, so nothing is silently dropped
# MAGIC
# MAGIC ### Why a synthetic transaction_id
# MAGIC PaySim has no natural unique transaction identifier — `step` is a
# MAGIC simulated hour index, not a real timestamp, and no row-level ID column
# MAGIC exists. We build one by hashing the business fields together. This
# MAGIC catches exact-duplicate rows (e.g. from a checkpoint replay) but can't
# MAGIC distinguish two genuinely different transactions that happen to share
# MAGIC every field by coincidence — a known limitation of the source data,
# MAGIC not a bug in this pipeline.
# MAGIC
# MAGIC ### Why dropDuplicatesWithinWatermark on _ingested_at
# MAGIC Streaming dedup needs a watermark to bound how much state Spark keeps.
# MAGIC PaySim's own `step` field isn't a real clock, so we watermark on
# MAGIC `_ingested_at` (the real wall-clock time bronze recorded on ingest)
# MAGIC instead — that's a legitimate proxy since it's monotonically
# MAGIC increasing with the stream.

# COMMAND ----------

CATALOG = "fraud_project"
BRONZE_TABLE = f"{CATALOG}.bronze.transactions"
SILVER_TABLE = f"{CATALOG}.silver.transactions"
QUARANTINE_TABLE = f"{CATALOG}.silver.transactions_quarantine"
SILVER_CHECKPOINT = f"/Volumes/{CATALOG}/raw/landing/_checkpoints/silver_transactions"
QUARANTINE_CHECKPOINT = f"/Volumes/{CATALOG}/raw/landing/_checkpoints/silver_quarantine"

VALID_TYPES = ["CASH_IN", "CASH_OUT", "DEBIT", "PAYMENT", "TRANSFER"]

BUSINESS_COLS = [
    "step",
    "type",
    "amount",
    "nameOrig",
    "oldbalanceOrg",
    "newbalanceOrig",
    "nameDest",
    "oldbalanceDest",
    "newbalanceDest",
    "isFraud",
    "isFlaggedFraud",
]

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StringType


def try_cast(col_name: str, sql_type: str):
    """TRY_CAST via SQL expr — the pyspark.sql.functions.try_cast Python
    wrapper isn't available on all Databricks runtimes yet, but the
    underlying SQL function is universally supported this way."""
    return F.expr(f"try_cast(`{col_name}` AS {sql_type})")


bronze_stream = spark.readStream.table(BRONZE_TABLE)

# COMMAND ----------

# Defensive typing — don't trust Auto Loader's inferred schema to stay
# stable forever. try_cast returns null instead of failing the job on a
# bad value, which the DQ checks below then catch explicitly.
typed = (
    bronze_stream.withColumn("step", try_cast("step", "BIGINT"))
    .withColumn("type", F.col("type").cast(StringType()))
    .withColumn("amount", try_cast("amount", "DOUBLE"))
    .withColumn("nameOrig", F.col("nameOrig").cast(StringType()))
    .withColumn("oldbalanceOrg", try_cast("oldbalanceOrg", "DOUBLE"))
    .withColumn("newbalanceOrig", try_cast("newbalanceOrig", "DOUBLE"))
    .withColumn("nameDest", F.col("nameDest").cast(StringType()))
    .withColumn("oldbalanceDest", try_cast("oldbalanceDest", "DOUBLE"))
    .withColumn("newbalanceDest", try_cast("newbalanceDest", "DOUBLE"))
    .withColumn("isFraud", try_cast("isFraud", "INT"))
    .withColumn("isFlaggedFraud", try_cast("isFlaggedFraud", "INT"))
)

# COMMAND ----------

with_id = typed.withColumn(
    "transaction_id", F.sha2(F.concat_ws("||", *BUSINESS_COLS), 256)
)

deduped = with_id.withWatermark("_ingested_at", "10 minutes").dropDuplicatesWithinWatermark(
    ["transaction_id"]
)

# COMMAND ----------

# DQ checks — each a named boolean column, so quarantined rows carry a
# clear reason rather than just vanishing.
checked = (
    deduped.withColumn("dq_valid_amount", F.col("amount") > 0)
    .withColumn("dq_valid_type", F.col("type").isin(VALID_TYPES))
    .withColumn(
        "dq_valid_names",
        F.col("nameOrig").isNotNull() & F.col("nameDest").isNotNull(),
    )
    .withColumn(
        "dq_valid_fraud_flag",
        F.col("isFraud").isin([0, 1]) | F.col("isFraud").isNull(),
    )
)

is_clean = (
    F.col("dq_valid_amount")
    & F.col("dq_valid_type")
    & F.col("dq_valid_names")
    & F.col("dq_valid_fraud_flag")
)

failure_reason = F.concat_ws(
    ",",
    F.when(~F.col("dq_valid_amount"), F.lit("invalid_amount")),
    F.when(~F.col("dq_valid_type"), F.lit("invalid_type")),
    F.when(~F.col("dq_valid_names"), F.lit("invalid_names")),
    F.when(~F.col("dq_valid_fraud_flag"), F.lit("invalid_fraud_flag")),
)

clean_rows = checked.filter(is_clean).drop(
    "dq_valid_amount", "dq_valid_type", "dq_valid_names", "dq_valid_fraud_flag"
)
quarantined_rows = checked.filter(~is_clean).withColumn(
    "dq_failure_reason", failure_reason
)

# COMMAND ----------

clean_query = (
    clean_rows.writeStream.format("delta")
    .option("checkpointLocation", SILVER_CHECKPOINT)
    .outputMode("append")
    .trigger(availableNow=True)
    .toTable(SILVER_TABLE)
)
clean_query.awaitTermination()

# COMMAND ----------

quarantine_query = (
    quarantined_rows.writeStream.format("delta")
    .option("checkpointLocation", QUARANTINE_CHECKPOINT)
    .outputMode("append")
    .trigger(availableNow=True)
    .toTable(QUARANTINE_TABLE)
)
quarantine_query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ## DQ summary

# COMMAND ----------

clean_count = spark.table(SILVER_TABLE).count()
try:
    quarantine_count = spark.table(QUARANTINE_TABLE).count()
except Exception:
    quarantine_count = 0

total = clean_count + quarantine_count
pct_clean = (clean_count / total * 100) if total else 0.0

print(f"Silver (clean):     {clean_count}")
print(f"Quarantined:        {quarantine_count}")
print(f"Pass rate:          {pct_clean:.2f}%")

# COMMAND ----------

if quarantine_count:
    display(
        spark.table(QUARANTINE_TABLE)
        .groupBy("dq_failure_reason")
        .count()
        .orderBy(F.desc("count"))
    )

# COMMAND ----------

display(spark.table(SILVER_TABLE).limit(20))
