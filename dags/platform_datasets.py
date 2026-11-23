"""Airflow Datasets shared by producer and consumer DAGs.

One object, imported on both sides: a URI typed twice with one character off would never trigger, silently.
"""

from airflow.datasets import Dataset

# New five-Channel labels joined with the current Gold fact (dp3_return_labels). ml_feature_materialize reads both
# the Gold fact and these labels, so it waits for the labels, not for Gold alone (diagram: "after each new Gold").
GOLD_LABELS = Dataset("iceberg://lakehouse/gold/fact_return_labels_dp3")
