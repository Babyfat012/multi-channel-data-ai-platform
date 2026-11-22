"""Load the Airflow Connections and Variables the DAGs read, into the Airflow metadata DB.

Stages (each one only adds its own Variables; rerunning converges to the same config):
  base        Spark connection, DP1 Amazon/Walmart, Iceberg maintenance, order lifecycle v2 + CDC v2, feature store
  dp1-files   DP1 for the three newer batch Channels (TikTok Shop, Shopify, Shopee)
  dp1-cdc     DP1 CDC extension (Walmart change events)
  dp2-silver  DP2 Silver + Gold tables for Amazon/Walmart
  dp2-files   DP2 Silver for the three newer Channels
  dp3         DP3 feature tables
  all         every stage above

On AWS (PLATFORM_STORAGE=s3) `base` also loads every dp stage, with S3 packages and smaller Spark resources.

    python3 -m scripts.data.setup_airflow [--stage base|dp1-files|dp1-cdc|dp2-silver|dp2-files|dp3|all]
"""

import argparse
import json
import os

from scripts.common import ROOT, env, run, succeeds

AIRFLOW = ["docker", "compose", "--profile", "airflow", "exec", "-T", "airflow"]
ICEBERG = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.11.0,org.apache.iceberg:iceberg-aws-bundle:1.11.0"
KAFKA = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"
NEW_CHANNELS = ("tiktok_shop", "shopify", "shopee")

STAGES = {
    "base": {
        "platform_spark_packages": f"{ICEBERG},org.postgresql:postgresql:42.7.4",
        "platform_dp1_amazon_window_days": "10",
        "platform_dp1_amazon_window_start": "2026-04-05",
        "platform_dp1_amazon_table": "raw_amazon_orders_dp1",
        "platform_dp1_walmart_table": "raw_walmart_orders_dp1",
        # configs/ is not mounted into Airflow, so the whole maintenance JSON goes into one Variable.
        "platform_maintenance_config": (ROOT / "configs/iceberg_maintenance.json").read_text(),
        # Order lifecycle v2: in-container paths; the planner mounts the fixture before triggering.
        "platform_order_lifecycle_receipt_dir": "/opt/airflow/receipts/order_lifecycle",
        "platform_order_lifecycle_source_root": "/data/order_lifecycle",
        "platform_order_lifecycle_cutoff": "2026-06-01T00:00:00.000000Z",
        "platform_order_lifecycle_driver_memory": "3g",
        "platform_order_lifecycle_executor_memory": "4g",
        "platform_order_lifecycle_executor_cores": "2",
        # Separate CDC v2: the planner creates and mounts the oracle before triggering the paused DAG.
        "platform_order_lifecycle_cdc_control_dir": "/opt/airflow/platform-control/order_lifecycle_cdc",
        "platform_order_lifecycle_cdc_oracle": (
            "/opt/airflow/platform-control/order_lifecycle_cdc/order_lifecycle_cdc_expected.json"
        ),
        "platform_order_lifecycle_cdc_spark_packages": f"{ICEBERG},{KAFKA}",
        # ML Platform feature store; Redis lives in the kind cluster (scripts/ml/setup_kind_feature_store.py).
        "platform_feast_root": "s3://platform/feature-store",
        "platform_feast_redis": "platform-control-plane:30379",
    },
    "dp1-files": {
        "platform_dp1_extra_source_root": "/data/channels-small/source-exports",
        **{f"platform_dp1_{c}_table": f"raw_{c}_orders_dp1" for c in NEW_CHANNELS},
    },
    "dp1-cdc": {
        "platform_dp1_cdc_table": "raw_walmart_cdc_v2",
        "platform_dp1_cdc_topic": "platform_cdc.walmart.orders",
        "platform_cdc_spark_packages": f"{ICEBERG},{KAFKA}",
    },
    "dp2-silver": {
        "platform_dp2_amazon_bronze_table": "raw_amazon_orders_dp1",
        "platform_dp2_walmart_bronze_table": "raw_walmart_orders_dp1",
        "platform_dp2_amazon_silver_table": "stg_amazon_order_lines_dp2",
        "platform_dp2_walmart_silver_table": "stg_walmart_order_lines_dp2",
        "platform_dp2_gold_table": "fact_order_lines_dp2",
        "platform_dp2_amazon_dim_table": "dim_amazon_order_dp2",
    },
    "dp2-files": {
        **{f"platform_dp2_{c}_bronze_table": f"raw_{c}_orders_dp1" for c in NEW_CHANNELS},
        **{f"platform_dp2_{c}_silver_table": f"stg_{c}_order_lines_dp2" for c in NEW_CHANNELS},
    },
    "dp3": {
        "platform_dp3_gold_source_table": "fact_order_lines_dp2",
        "platform_dp3_feature_table": "feat_order_line_return_dp3",
    },
}


def aws_overrides() -> dict:
    """EC2 (t3.xlarge): S3 packages, real maintenance tables, s3a:// sources, a worker with 3g/1 core."""
    packages = env("PLATFORM_SPARK_PACKAGES_AWS")
    values = {
        "platform_cdc_spark_packages": f"{packages},{KAFKA}",  # replaces dp1-cdc's MinIO package set
        "platform_spark_packages_aws": packages,
        # The demo tables do not exist on EC2; the real ones get the AWS policy file.
        "platform_maintenance_config": (ROOT / "configs/iceberg_maintenance_aws.json").read_text(),
        "platform_maintenance_tables": '["lakehouse.bronze.raw_shopee_orders_dp1"]',
        "platform_order_lifecycle_source_root": (
            f"s3a://{env('RAW_BUCKET')}/order_lifecycle/{os.environ.get('PLATFORM_DATASET_NAME') or 'fixture'}"
        ),
        "platform_order_lifecycle_cutoff": os.environ.get("PLATFORM_ORDER_LIFECYCLE_CUTOFF")
        or "2026-06-01T00:00:00.000000Z",
        "platform_order_lifecycle_driver_memory": "2g",
        "platform_order_lifecycle_executor_memory": "2g",
        "platform_order_lifecycle_executor_cores": "1",
    }
    if os.environ.get("PLATFORM_RUNTIME_PROFILE") == "performance":
        values |= {
            "platform_order_lifecycle_driver_memory": "4g",
            "platform_order_lifecycle_executor_memory": "4g",
            "platform_order_lifecycle_executor_cores": "2",
        }
    return values


def variables(stage: str, storage: str) -> dict:
    """Every Variable a stage sets, in order; later values win (AWS overrides last)."""
    names = list(STAGES) if stage == "all" or (stage == "base" and storage == "s3") else [stage]
    values = {}
    for name in names:
        values |= STAGES[name]
    if storage == "s3" and "base" in names:
        values |= aws_overrides()
    return values


def set_variables(values: dict) -> None:
    """Set all Variables in one container exec; values go through stdin, not argv."""
    code = (
        "import json, sys\nfrom airflow.models import Variable\n"
        "for key, value in json.load(sys.stdin).items():\n    Variable.set(key, value)"
    )
    run([*AIRFLOW, "python", "-c", code], stdin=json.dumps(values))


def base_connections() -> None:
    """Credential Connections from the container env (no passwords on argv) + spark_default for standalone Spark."""
    run([*AIRFLOW, "python", "/opt/platform/scripts/data/bootstrap_airflow_config.py"])
    # Airflow may pre-create spark_default pointing at YARN: replace exactly this Connection so reruns converge.
    succeeds([*AIRFLOW, "airflow", "connections", "delete", "spark_default"])
    run(
        [
            *AIRFLOW,
            "airflow",
            "connections",
            "add",
            "spark_default",
            "--conn-type",
            "spark",
            "--conn-host",
            "spark://spark-master",
            "--conn-port",
            "7077",
            "--conn-extra",
            '{"deploy-mode":"client"}',
        ]
    )


def main(argv: list[str] | None = None) -> None:
    """Parse the stage, then load Connections (base only) and Variables."""
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--stage", choices=[*STAGES, "all"], default="base")
    args = parser.parse_args(argv)
    os.chdir(ROOT)
    storage = os.environ.get("PLATFORM_STORAGE") or "minio"
    if args.stage in ("base", "all"):
        base_connections()
    values = variables(args.stage, storage)
    set_variables(values)
    print(f"Airflow stage {args.stage} ({storage}): {len(values)} Variables loaded.")


if __name__ == "__main__":
    main()
