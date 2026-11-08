"""ML Platform: Gold → Parquet offline/tập train → apply → nạp/đối soát bản batch riêng rồi công bố.

`end` là mốc thời gian GIẢ LẬP (mặc định = cutoff Gold), không phải giờ thật: mỗi lần chạy
Publisher chọn snapshot mới nhất mỗi SKU đến end; bản active tiếp tục phục vụ khi bản mới đang nạp.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
from datetime import datetime, timezone

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from platform_spark_operator import PlatformSparkSubmitOperator

FEAST = "/home/airflow/feast-venv/bin/feast"
FEAST_PYTHON = "/home/airflow/feast-venv/bin/python"
ENSURE_24H = (
    "import os; from push_stream_features import ensure_offline_file; "
    "ensure_offline_file(os.environ['FEAST_SKU_RETURN_24H_PATH'])"
)
REPO = "/opt/feature_repo"
# Cutoff của Gold A (hết 180 ngày dữ liệu); local chưa đặt biến thì về cutoff cũ.
FULL_RELOAD_START = "2000-01-01T00:00:00+00:00"  # trước mọi dữ liệu giả lập
CUTOFF = "{{ var.value.get('platform_ml_cutoff', var.value.platform_order_lifecycle_cutoff) }}"
# Spark ghi qua s3a://, Feast đọc cùng chỗ qua s3://.
SPARK_ROOT = "{{ var.value.platform_feast_root | replace('s3://', 's3a://') }}"
SPARK_OUTPUT = SPARK_ROOT + "/sku_daily"
CONF = {
    "spark.driver.host": "airflow",
    "spark.driver.bindAddress": "0.0.0.0",
    "spark.jars.ivy": "/opt/airflow/.ivy2",
}


def run_feast(*args, **context):
    """Chạy CLI Feast trên bản sao repo (mount read-only), credential lấy lúc chạy."""
    # Không khai `params=` trong chữ ký: Airflow cấm tên context trùng vị trí của *args (ValueError reserved).
    params = context.get("params")
    # Redis mất dữ liệu nhưng registry vẫn nhớ mốc cũ ⇒ incremental nạp 0 dòng
    # mà vẫn xanh. full_reload: nạp lại toàn bộ (FULL_RELOAD_START, end], ghi giá trị mới nhất mỗi SKU.
    if args[0] == "materialize-incremental" and (params or {}).get("full_reload"):
        print("full_reload=true: chủ động nạp lại batch, bỏ kiểm tra mốc incremental.")
        args = ("materialize", *args[1:-1], FULL_RELOAD_START, args[-1])
    root = Variable.get("platform_feast_root")
    redis = Variable.get("platform_feast_redis")
    if not redis:
        raise ValueError("platform_feast_redis rỗng: chưa đặt địa chỉ Redis online")
    env = {**os.environ}
    # AWS: S3 thật qua IAM role của EC2, không key, không endpoint riêng. Local: MinIO.
    if os.environ.get("PLATFORM_STORAGE", "minio") != "s3":
        minio = BaseHook.get_connection("platform_minio")
        env.update(
            AWS_ACCESS_KEY_ID=minio.login,
            AWS_SECRET_ACCESS_KEY=minio.password,
            FEAST_S3_ENDPOINT_URL=Variable.get("platform_minio_internal_endpoint"),
        )
    env |= {
        "FEAST_REGISTRY": f"{root}/registry.db",
        "FEAST_SKU_DAILY_PATH": f"{root}/sku_daily",
        "FEAST_SKU_RETURN_24H_PATH": f"{root}/sku_return_24h/",
        "FEAST_REDIS": redis,
        "FEAST_PUBLICATION_ROOT": f"{root}/publications",
    }
    with tempfile.TemporaryDirectory() as tmp:
        repo = shutil.copytree(REPO, f"{tmp}/repo")
        if args[0] == "publish-batch":
            command = [FEAST_PYTHON, "-m", "publish_batch", *args[1:]]
            if (params or {}).get("full_reload"):
                command.append("--full-reload")
            subprocess.run(command, cwd=repo, env=env, check=True, timeout=3600)
            return
        if args[0] == "materialize-incremental":
            subprocess.run(
                [FEAST_PYTHON, "-m", "check_online_store"],
                cwd=repo,
                env=env,
                check=True,
                timeout=120,
            )
        if args[0] == "apply":
            # View stream 24h đọc Parquet do pusher tạo; nơi chưa chạy pusher (AWS) thì tạo file rỗng.
            subprocess.run(
                [FEAST_PYTHON, "-c", ENSURE_24H], cwd=repo, env=env, check=True, timeout=120
            )
        result = subprocess.run(
            [FEAST, *args], cwd=repo, env=env, capture_output=True, text=True, check=False
        )
    print(result.stdout, result.stderr, sep="\n")
    result.check_returncode()


with DAG(
    dag_id="ml_feature_materialize",
    description="Feature SKU theo ngày: Parquet → batch riêng → đối soát → công bố Redis A/B",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    params={
        "end": Param("", type="string", description="Mốc giả lập; trống = cutoff Gold"),
        "full_reload": Param(
            False,
            type="boolean",
            description="Cho phép phục hồi khi bản online đang phục vụ đã mất dữ liệu",
        ),
    },
    tags=["platform", "ml-platform", "feature-store"],
) as dag:
    export = PlatformSparkSubmitOperator(
        task_id="export_sku_daily",
        application="/opt/spark_jobs/gold/export_sku_daily_features.py",
        application_args=["--cutoff", CUTOFF, "--output", SPARK_OUTPUT],
        conn_id="spark_default",
        packages="{{ var.value.platform_spark_packages }},org.apache.hadoop:hadoop-aws:3.3.4",
        driver_memory="1g",
        conf=CONF,
    )
    # Tập train (entity df cho KFP): line có nhãn trước cutoff + thuộc tính lúc đặt, cùng run nhãn với feature.
    export_training = PlatformSparkSubmitOperator(
        task_id="export_training_lines",
        application="/opt/spark_jobs/gold/export_training_lines.py",
        application_args=[
            "--cutoff",
            CUTOFF,
            "--output",
            SPARK_ROOT + "/training_lines",
            # 2‰ ≈ 360k line: Feast PIT join offline chạy pandas trong MỘT pod KFP; 1% (1,8 tr) OOM node 8 GB.
            "--sample-per-mille",
            "2",
        ],
        conn_id="spark_default",
        packages="{{ var.value.platform_spark_packages }},org.apache.hadoop:hadoop-aws:3.3.4",
        driver_memory="1g",
        conf=CONF,
    )
    apply = PythonOperator(task_id="feast_apply", python_callable=run_feast, op_args=["apply"])
    materialize = PythonOperator(
        task_id="publish_batch",
        python_callable=run_feast,
        op_args=[
            "publish-batch",
            "--end",
            # Python 3.10 không đọc được đuôi "Z" của fromisoformat.
            "{{ (params.end or var.value.get('platform_ml_cutoff', var.value.platform_order_lifecycle_cutoff))"
            " | replace('Z', '+00:00') }}",
        ],
    )
    export >> export_training >> apply >> materialize
