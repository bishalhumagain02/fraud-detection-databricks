# Databricks notebook source
# MAGIC %md
# MAGIC # Offline model training — fraud_project
# MAGIC
# MAGIC Builds the features that streaming (Week 3) deliberately couldn't
# MAGIC compute — order-dependent, unbounded-lookback features per account —
# MAGIC trains a classifier with proper class-imbalance handling, and
# MAGIC registers the result in the Unity Catalog Model Registry.
# MAGIC
# MAGIC ### Leakage safety
# MAGIC Every "prior activity" feature below uses `rowsBetween(unboundedPreceding, -1)`
# MAGIC — strictly rows *before* the current one in account history, never
# MAGIC including it. This is the same guard used in the bikeshare project's
# MAGIC feature engineering: a feature computed from data that wouldn't have
# MAGIC existed yet at prediction time is a leak, however small.
# MAGIC
# MAGIC ### Split strategy
# MAGIC A random train/test split would leak future account behavior into
# MAGIC training. Splitting by `step` (simulated time) instead means the model
# MAGIC is tested on transactions that happen strictly after everything it was
# MAGIC trained on — the same rolling-origin principle as the bikeshare ML
# MAGIC backtest, simplified to a single cut since this dataset is much smaller.

# COMMAND ----------

# MAGIC %pip install lightgbm
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

CATALOG = "fraud_project"
FEATURES_TABLE = f"{CATALOG}.gold.transaction_features"
REGISTERED_MODEL_NAME = f"{CATALOG}.gold.fraud_classifier"

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql import functions as F

base = spark.table(FEATURES_TABLE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Order-dependent account-history features (leakage-safe)

# COMMAND ----------

acct_window = Window.partitionBy("nameOrig").orderBy("step", "transaction_id")
acct_history = acct_window.rowsBetween(Window.unboundedPreceding, -1)

with_history = (
    base.withColumn("txn_seq_num_orig", F.row_number().over(acct_window))
    .withColumn("prev_step_orig", F.lag("step").over(acct_window))
    .withColumn(
        "steps_since_last_txn_orig",
        F.coalesce(F.col("step") - F.col("prev_step_orig"), F.lit(-1)),
    )
    .withColumn("prior_txn_count_orig", F.count("*").over(acct_history))
    .withColumn(
        "prior_avg_amount_orig", F.coalesce(F.avg("amount").over(acct_history), F.lit(0.0))
    )
    .withColumn(
        "prior_max_amount_orig", F.coalesce(F.max("amount").over(acct_history), F.lit(0.0))
    )
    .drop("prev_step_orig")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Time-based train/test split

# COMMAND ----------

split_step = with_history.approxQuantile("step", [0.8], 0.01)[0]
print(f"Split at step <= {split_step} (train) / step > {split_step} (test)")

FEATURE_COLS = [
    "amount",
    "balance_delta_orig",
    "balance_delta_dest",
    "balance_mismatch_orig",
    "txn_seq_num_orig",
    "steps_since_last_txn_orig",
    "prior_txn_count_orig",
    "prior_avg_amount_orig",
    "prior_max_amount_orig",
]

train_pd = (
    with_history.filter(F.col("step") <= split_step)
    .select(*FEATURE_COLS, "type", "isFraud")
    .toPandas()
)
test_pd = (
    with_history.filter(F.col("step") > split_step)
    .select(*FEATURE_COLS, "type", "isFraud")
    .toPandas()
)

train_pd["type"] = train_pd["type"].astype("category")
test_pd["type"] = test_pd["type"].astype("category")

print(f"Train rows: {len(train_pd)}  (fraud: {int(train_pd['isFraud'].sum())})")
print(f"Test rows:  {len(test_pd)}  (fraud: {int(test_pd['isFraud'].sum())})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Train
# MAGIC
# MAGIC `scale_pos_weight` handles the class imbalance directly in the loss
# MAGIC function rather than resampling the data, which is simpler and doesn't
# MAGIC risk duplicating minority-class rows into both train and test.

# COMMAND ----------

import lightgbm as lgb

X_train = train_pd[FEATURE_COLS + ["type"]]
y_train = train_pd["isFraud"]
X_test = test_pd[FEATURE_COLS + ["type"]]
y_test = test_pd["isFraud"]

pos = int(y_train.sum())
neg = len(y_train) - pos
scale_pos_weight = (neg / pos) if pos > 0 else 1.0
print(f"scale_pos_weight = {scale_pos_weight:.2f} ({pos} positive / {neg} negative in train)")

model = lgb.LGBMClassifier(
    scale_pos_weight=scale_pos_weight,
    random_state=42,
    n_estimators=200,
)
model.fit(X_train, y_train, categorical_feature=["type"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Evaluate
# MAGIC
# MAGIC Accuracy is meaningless on data this imbalanced — a model that never
# MAGIC predicts fraud could still score >99% accuracy. Precision, recall, and
# MAGIC PR-AUC are what actually matter here.

# COMMAND ----------

from sklearn.metrics import (
    average_precision_score,
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)

y_pred = model.predict(X_test)

metrics = {
    "precision": precision_score(y_test, y_pred, zero_division=0),
    "recall": recall_score(y_test, y_pred, zero_division=0),
    "f1": f1_score(y_test, y_pred, zero_division=0),
}

if y_test.sum() > 0:
    y_proba = model.predict_proba(X_test)[:, 1]
    metrics["pr_auc"] = average_precision_score(y_test, y_proba)
    metrics["roc_auc"] = roc_auc_score(y_test, y_proba)
else:
    print(
        "WARNING: test set has zero fraud cases — PR-AUC/ROC-AUC are undefined "
        "and skipped. This is a real limitation of the current data volume, "
        "not a bug: with this few rows, a time-based split can land on a test "
        "window with no positive examples. Expect this to resolve as more "
        "simulator batches accumulate more data."
    )

for k, v in metrics.items():
    print(f"{k}: {v:.4f}")

print("\nConfusion matrix (rows=actual, cols=predicted, [0,1]):")
print(confusion_matrix(y_test, y_pred, labels=[0, 1]))

# COMMAND ----------

display(
    __import__("pandas").DataFrame(
        {"feature": X_train.columns, "importance": model.feature_importances_}
    ).sort_values("importance", ascending=False)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Log and register with MLflow

# COMMAND ----------

import mlflow

mlflow.set_registry_uri("databricks-uc")

with mlflow.start_run(run_name="lightgbm_fraud_baseline") as run:
    mlflow.log_param("split_step", split_step)
    mlflow.log_param("scale_pos_weight", scale_pos_weight)
    mlflow.log_params(model.get_params())
    for k, v in metrics.items():
        mlflow.log_metric(k, v)
    mlflow.lightgbm.log_model(
        model,
        artifact_path="model",
        registered_model_name=REGISTERED_MODEL_NAME,
    )
    print(f"Run ID: {run.info.run_id}")
    print(f"Registered as: {REGISTERED_MODEL_NAME}")
