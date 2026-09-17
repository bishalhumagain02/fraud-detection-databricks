# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze ingestion — fraud_project
# MAGIC
# MAGIC Auto Loader watches `raw.landing` for new files from the replay simulator
# MAGIC and appends them into `bronze.transactions`, unchanged, plus ingestion
# MAGIC metadata. Nothing is cleaned or deduped here — that's the silver layer
# MAGIC (Week 2). Bronze exists so you always have the original data to replay
# MAGIC downstream fixes against.
# MAGIC
# MAGIC Run this notebook attached to a serverless compute (the only option on
# MAGIC Free Edition).

# COMMAND ----------

CATALOG = "fraud_project"
LANDING_VOLUME = f"/Volumes/{CATALOG}/raw/landing"
BRONZE_TABLE = f"{CATALOG}.bronze.transactions"
CHECKPOINT_PATH = f"/Volumes/{CATALOG}/raw/landing/_checkpoints/bronze_transactions"
SCHEMA_LOCATION = f"/Volumes/{CATALOG}/raw/landing/_schema/bronze_transactions"

# COMMAND ----------

from pyspark.sql import functions as F

raw_stream = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", SCHEMA_LOCATION)
    .option("cloudFiles.inferColumnTypes", "true")
    .load(LANDING_VOLUME)
)

bronze_stream = raw_stream.withColumn("_ingested_at", F.current_timestamp()).withColumn(
    "_source_file", F.col("_metadata.file_path")
)

# COMMAND ----------

# MAGIC %md
# MAGIC `trigger(availableNow=True)` processes everything that's landed so far,
# MAGIC then stops — good for iterative testing like this. Once Week 6's
# MAGIC orchestration is in place, this becomes a `processingTime` trigger on a
# MAGIC schedule instead, so it keeps running continuously.

# COMMAND ----------

query = (
    bronze_stream.writeStream.format("delta")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .outputMode("append")
    .trigger(availableNow=True)
    .toTable(BRONZE_TABLE)
)

query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify

# COMMAND ----------

display(spark.table(BRONZE_TABLE).limit(20))

# COMMAND ----------

print(f"Row count: {spark.table(BRONZE_TABLE).count()}")
