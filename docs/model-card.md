# Model card — fraud_classifier

*Fill in after running `jobs/04_train_model.py`. Treat this like the
bikeshare project's model card: report what actually happened, including
if the model underperforms or a metric is undefined — that's information,
not a failure to hide.*

## Model

- **Registered as:** `fraud_project.gold.fraud_classifier`
- **Algorithm:** LightGBM classifier
- **Training run ID:** _fill in from the notebook output_
- **Date trained:** _fill in_

## Data

- **Source:** `fraud_project.gold.transaction_features`, PaySim-derived
- **Train rows / fraud count:** _fill in_
- **Test rows / fraud count:** _fill in_
- **Split method:** time-based (`step <= split_step` train, `step > split_step` test), not random — avoids leaking future account behavior into training
- **Split point (step):** _fill in_

## Features

Row-level: `amount`, `type`, `balance_delta_orig`, `balance_delta_dest`, `balance_mismatch_orig`

Account-history (leakage-safe, strictly prior transactions only):
`txn_seq_num_orig`, `steps_since_last_txn_orig`, `prior_txn_count_orig`,
`prior_avg_amount_orig`, `prior_max_amount_orig`

Not included (computed only in the streaming layer, not joined in here
yet): windowed account velocity (`gold.account_velocity_1h/24h`). Candidate
for a v2 model once orchestration (Week 6) has velocity tables running
continuously with enough history.

## Class imbalance handling

`scale_pos_weight` set to the train-set negative:positive ratio, passed
directly to LightGBM rather than resampling the data.

- **scale_pos_weight used:** _fill in_

## Results

| Metric | Value |
|---|---|
| Precision | _fill in_ |
| Recall | _fill in_ |
| F1 | _fill in_ |
| PR-AUC | _fill in (or "undefined — no positive examples in test set", see limitations)_ |
| ROC-AUC | _fill in_ |

Confusion matrix: _paste from notebook output_

Top features by importance: _paste top 5 from the notebook's display() output_

## Known limitations

- **PaySim balance-zeroing artifact:** PaySim zeroes destination balances
  specifically on fraudulent transactions, which makes balance-based
  features correlate with `isFraud` more strongly than real bank data
  would support. Results here likely look better than a production model
  trained on real transaction data would achieve — noted, not hidden.
- **Small data volume:** current results are based on however much the
  replay simulator has pushed through so far, not the full 6.3M-row
  PaySim dataset. Revisit these numbers once more of the dataset has
  streamed through.
- **Velocity features not yet joined in:** the streaming layer's
  account-velocity aggregates (Week 3) aren't part of this model's
  feature set yet — see "Not included" above.
- _add anything else that comes up when you actually run this_
