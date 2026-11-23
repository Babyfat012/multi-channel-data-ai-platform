"""Nightly lakehouse build (diagram: "Build the order lakehouse · Airflow + Spark · every night").

Triggers the existing DAGs one after another and waits for each; a failed child stops the chain. The children keep
`schedule=None`, so each can still be run by hand without dragging the rest along.

Order: dp3_return_labels runs LAST. Its success emits GOLD_LABELS, which starts ml_feature_materialize; earlier in
the chain, that heavy refresh would run next to dp3_features / dp4 and every Spark driver shares the Airflow
container's RAM. dp3_features and dp4 read only the Gold fact, so they do not need the new labels.

Paused on creation: every run still processes the fixed window set by Variables (window start, cutoff), not
"the new day", so enable it deliberately. Runs at 01:00 Vietnam time; catchup=False, so nights when the EC2 machine
is stopped are skipped, not replayed.
"""

import pendulum
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

CHAIN = ["dp1_bronze", "dp2_silver", "dp3_features", "dp4_warehouse", "dp3_return_labels"]

with DAG(
    dag_id="dp_nightly",
    description="Nightly: dp1 Bronze → dp2 Silver/Gold → dp3 features → dp4 warehouse → dp3 labels (→ ML refresh)",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Ho_Chi_Minh"),
    schedule="0 1 * * *",
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
    tags=["platform", "schedule"],
) as dag:
    previous = None
    for child in CHAIN:
        # Synchronous wait (LocalExecutor, no triggerer): the task holds one executor slot while its child runs.
        task = TriggerDagRunOperator(
            task_id=f"run_{child}",
            trigger_dag_id=child,
            trigger_run_id="nightly__{{ ts_nodash }}",
            wait_for_completion=True,
            poke_interval=60,
            allowed_states=["success"],
            failed_states=["failed"],
            # A retry of this task re-runs the child under the same run id instead of failing on "already exists".
            reset_dag_run=True,
        )
        if previous:
            previous >> task
        previous = task
