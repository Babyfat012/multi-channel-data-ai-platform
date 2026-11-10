# Purpose: offline checks of the AWS provisioning safety guards (Terraform, Ansible, Compose, Spark).
"""Offline checks of AWS Terraform/Ansible/Compose via text and YAML, without calling the cloud."""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

from spark_jobs.lib.iceberg_session import catalog_conf

ROOT = Path(__file__).resolve().parents[4]
TF_DIR = ROOT / "infra/aws/terraform"
ANSIBLE = ROOT / "infra/aws/ansible"
CODE = re.sub(
    r"(?m)^\s*#.*$", "", "\n".join(p.read_text(encoding="utf-8") for p in TF_DIR.glob("*.tf"))
)


class ComposeLoader(yaml.SafeLoader):
    """Load the Compose `!override` tag."""


ComposeLoader.add_constructor(
    "!override",
    lambda loader, node: (
        loader.construct_mapping(node)
        if isinstance(node, yaml.MappingNode)
        else loader.construct_sequence(node)
    ),
)


def block(kind: str, name: str) -> str:
    """Return exactly one top-level block by type and name."""
    found = re.findall(rf'(?ms)^{kind}\s+"{re.escape(name)}"[^\n]*\{{\n.*?^\}}', CODE)
    assert len(found) == 1, (kind, name)
    return found[0]


def test_security_group_has_no_ingress_and_no_access_keys():
    """SSH goes through SSM: no ingress, no access key, no user_data installing a runtime."""
    sg = block("resource", "aws_security_group")
    assert not re.search(r"ingress\s*\{", sg)
    assert "aws_vpc_security_group_ingress_rule" not in CODE
    assert "aws_iam_access_key" not in CODE
    assert "user_data" not in CODE
    assert "AmazonSSMManagedInstanceCore" in CODE


def test_instance_uses_imdsv2_hop_two_and_encrypted_disks():
    """Containers can reach credentials (hop 2); IMDSv1 is disabled; the disk is encrypted."""
    vm = block("resource", "aws_instance")
    assert re.search(r'http_tokens\s*=\s*"required"', vm)
    assert re.search(r"http_put_response_hop_limit\s*=\s*2", vm)
    assert re.search(r"associate_public_ip_address\s*=\s*true", vm)
    assert len(re.findall(r"encrypted\s*=\s*true", CODE)) == 2
    assert not re.search(r'resource\s+"aws_nat_gateway"', CODE)


def test_iam_only_touches_the_two_buckets():
    """The policy only points at the ARN of the bucket Terraform creates, no global `*`."""
    policy = re.search(r'(?ms)^data "aws_iam_policy_document" "buckets".*?^\}', CODE)[0]
    assert 'resources = ["*"]' not in policy
    assert "aws_s3_bucket.data" in policy


def test_buckets_private_versioned_with_bounded_lifecycle():
    """Block public access, disable ACLs, enable versioning, only expire old versions and the tmp/ prefix."""
    assert re.search(r'for_each\s*=\s*toset\(\["raw", "lakehouse"\]\)', CODE)
    pab = block("resource", "aws_s3_bucket_public_access_block")
    for flag in (
        "block_public_acls",
        "block_public_policy",
        "ignore_public_acls",
        "restrict_public_buckets",
    ):
        assert re.search(rf"{flag}\s*=\s*true", pab)
    assert '"BucketOwnerEnforced"' in CODE
    assert re.search(r'status\s*=\s*"Enabled"', block("resource", "aws_s3_bucket_versioning"))
    life = block("resource", "aws_s3_bucket_lifecycle_configuration")
    assert 'prefix = "tmp/"' in life
    assert "noncurrent_days = var.noncurrent_retention_days" in life
    assert "force_destroy = var.force_destroy" in CODE


def test_budget_optional_excludes_credits_and_needs_email():
    """Budget is off by default, does not deduct credits, 3 thresholds, email required."""
    budget = block("resource", "aws_budgets_budget")
    assert re.search(r"count\s*=\s*var.enable_budget\s*\?\s*1\s*:\s*0", budget)
    assert re.search(r"include_credit\s*=\s*false", budget)
    assert "[50, 90, 100]" in budget
    assert "var.budget_email != null" in budget


@pytest.mark.parametrize(
    ("name", "default"),
    [
        ("region", '"ap-southeast-1"'),
        ("instance_type", '"t3.xlarge"'),
        ("cpu_credits", '"standard"'),
        ("enable_budget", "false"),
        ("force_destroy", "false"),
    ],
)
def test_defaults(name, default):
    """Defaults are cheap and safe for free-tier users."""
    assert re.search(rf"(?m)^\s*default\s*=\s*{re.escape(default)}\s*$", block("variable", name))


def test_no_default_ssh_key_and_outputs_for_ansible():
    """No default key; every output Ansible needs is present."""
    assert "default" not in block("variable", "ssh_public_key")
    for name in ("raw_bucket", "lakehouse_bucket", "instance_id", "data_volume_id", "region"):
        block("output", name)
    ports = block("output", "port_forward_command")
    assert "AWS-StartSSHSession" in ports
    assert "-i ~/.ssh/platform_aws" in ports
    for port in (8080, 8081, 8181, 9002, 8089, 8082):
        assert f"-L 127.0.0.1:{port}:127.0.0.1:{port}" in ports


def test_region_and_machine_size_match_the_runtime():
    """Reject an AZ outside the region and an 8 GiB machine that cannot fit the 14 GiB overlay."""
    assert 'startswith(var.availability_zone, "${var.region}")' in CODE
    assert '"t3.large"' not in block("variable", "instance_type")


def test_overlay_fits_c6i_2xlarge_without_keys_or_gcs():
    """Always-on services fit 8 vCPU/16 GiB (2 vCPU spare), loopback ports, no keys, no GCS."""
    text = (ROOT / "docker-compose.aws.yml").read_text()
    services = yaml.load(text, Loader=ComposeLoader)["services"]
    # Exception: MLflow + 2 exporters listen on the VPC private IP for EKS pods (SG opens only that port from the cluster SG).
    ip = "${PLATFORM_PRIVATE_IP:?PLATFORM_PRIVATE_IP_required}"
    private = {
        "mlflow": f"{ip}:${{MLFLOW_PORT:-5050}}:5000",
        "node-exporter": f"{ip}:9100:9100",
        "postgres-exporter": f"{ip}:9187:9187",
    }
    for name, svc in services.items():
        ports = [p for p in svc.get("ports", []) if p != private.get(name)]
        assert all(port.startswith("127.0.0.1:") for port in ports), name
    assert "AWS_ACCESS_KEY_ID" not in text and "gcs" not in text.lower()
    running = [
        "postgres",
        "lakekeeper",
        "spark-master",
        "spark-worker-1",
        "spark-worker-2",
        "airflow",
        "mlflow",
        "node-exporter",
        "postgres-exporter",
    ]

    def gib(value: str) -> float:
        """Convert a Compose mem_limit (g/m) to GiB."""
        return float(value[:-1]) / (1024 if value.endswith("m") else 1)

    # cpus is a ceiling, not a reservation: 6 for the Data Platform + 0.25 MLflow (idle except when KFP registers a model).
    # Since 10/29 the EC2 is r6i.xlarge (4 vCPU/32 GiB): CPU ceilings summing above 4 is intentional (heavy Spark has
    # already finished; the remaining jobs are small), and RAM has 2x headroom.
    # + 0.2 for the 2 exporters (round 2).
    assert sum(float(services[n]["cpus"]) for n in running) <= 6.45
    assert sum(gib(str(services[n]["mem_limit"])) for n in running) <= 15
    for name in ("minio", "kafka", "flink-jobmanager"):
        assert services[name]["profiles"] == ["disabled-aws"]
    assert services["airflow"]["environment"][
        "AIRFLOW_VAR_PLATFORM_ORDER_LIFECYCLE_SOURCE_ROOT"
    ].startswith("s3a://")


def test_ansible_finds_ebs_by_volume_id_and_guards_mkfs():
    """nvme path by volume ID; mkfs only runs when blkid reports the device empty."""
    path = yaml.safe_load((ANSIBLE / "roles/data_disk/vars/main.yml").read_text())["data_disk_path"]
    assert path.startswith("/dev/disk/by-id/nvme-Amazon_Elastic_Block_Store_")
    tasks = yaml.safe_load((ANSIBLE / "roles/data_disk/tasks/main.yml").read_text())
    fmt = next(t for t in tasks if "mkfs.ext4" in str(t.get("ansible.builtin.command", "")))
    assert fmt["when"] == "disk_blkid.rc == 2"
    variables = yaml.safe_load((ANSIBLE / "group_vars/all.example.yml").read_text())
    assert all(len(p.split(":")) == 3 for p in variables["spark_packages_aws"].split(","))
    for path in ANSIBLE.rglob("*"):
        if path.is_file():
            assert not re.search(r"gcloud|gs://|google-", path.read_text()), path


def test_s3_catalog_has_region_and_no_static_keys():
    """s3 branch: S3FileIO + region, no MinIO endpoint, no access key."""
    conf = catalog_conf({"PLATFORM_STORAGE": "s3", "AWS_REGION": "ap-southeast-1"})
    p = "spark.sql.catalog.lakehouse"
    assert conf[p + ".io-impl"] == "org.apache.iceberg.aws.s3.S3FileIO"
    assert conf[p + ".client.region"] == "ap-southeast-1"
    assert conf["spark.hadoop.fs.s3a.endpoint"] == "s3.ap-southeast-1.amazonaws.com"
    assert p + ".s3.endpoint" not in conf
    assert not any("key" in k.lower() for k in conf)
    with pytest.raises(KeyError):
        catalog_conf({"PLATFORM_STORAGE": "s3"})


def test_datahub_is_prepared_without_starting_and_all_ports_are_loopback():
    """SDK/Compose for the operator, secrets created once; phase B does not run during provisioning."""
    site = yaml.safe_load((ANSIBLE / "site.yml").read_text())
    role = next(r for r in site[0]["roles"] if r["role"] == "datahub")
    assert "enable_datahub" in role["when"]
    variables = yaml.safe_load((ANSIBLE / "group_vars/all.example.yml").read_text())
    assert variables["enable_datahub"] is True
    tasks = yaml.safe_load((ANSIBLE / "roles/datahub/tasks/main.yml").read_text())
    assert "acryl-datahub==1.7.0.14" in str(tasks)
    assert not any("docker compose" in str(t.get("ansible.builtin.command", "")) for t in tasks)
    secret = next(t for t in tasks if t.get("no_log"))
    assert secret["ansible.builtin.command"]["creates"].endswith(".local-secrets.env")
    services = yaml.load(
        (ROOT / "docker/datahub-compose.override.yml").read_text(), Loader=ComposeLoader
    )["services"]
    for name in (
        "datahub-gms-quickstart",
        "frontend-quickstart",
        "mysql",
        "kafka-broker",
        "opensearch",
    ):
        assert all(p.startswith("127.0.0.1:") for p in services[name]["ports"])


def test_stream_overlay_only_enables_kafka_flink_on_loopback():
    """The Stream overlay enables exactly Kafka + Flink, loopback ports, output on EBS; MinIO stays off."""
    services = yaml.load(
        (ROOT / "docker-compose.aws-stream.yml").read_text(), Loader=ComposeLoader
    )["services"]
    assert set(services) == {
        "kafka",
        "flink-jobmanager",
        "flink-taskmanager",
        "debezium-connect",
        "feast-pusher",
        "flink-ml-jobmanager",
        "flink-ml-taskmanager",
    }
    for svc in services.values():
        assert all(port.startswith("127.0.0.1:") for port in svc.get("ports", []))
    # The base file binds Kafka and the Flink UI to 0.0.0.0: the overlay must override both.
    assert services["kafka"]["ports"] and services["flink-jobmanager"]["ports"]
    for name in ("flink-jobmanager", "flink-taskmanager"):
        assert "/mnt/platform-data/flink:/opt/flink/output" in services[name]["volumes"]


def test_aws_overlay_points_return_labels_at_s3():
    """dp3_return_labels on AWS: executors have no local /data, so the oracle and keys must be on S3."""
    env = yaml.load((ROOT / "docker-compose.aws.yml").read_text(), Loader=ComposeLoader)[
        "services"
    ]["airflow"]["environment"]
    assert env["AIRFLOW_VAR_PLATFORM_DP3_GROUND_TRUTH"].startswith("s3a://")
    assert env["AIRFLOW_VAR_PLATFORM_DP3_WALMART_KEYS"].startswith("s3a://")


def test_aws_feast_uses_s3_iam_and_eks_redis():
    """Feast on AWS: real S3 store + EKS Redis, no more MinIO/kind; MinIO keys only on the local branch."""
    env = yaml.load((ROOT / "docker-compose.aws.yml").read_text(), Loader=ComposeLoader)[
        "services"
    ]["airflow"]["environment"]
    assert env["AIRFLOW_VAR_PLATFORM_FEAST_ROOT"] == "s3://${LAKEHOUSE_BUCKET}/feature-store"
    assert env["AIRFLOW_VAR_PLATFORM_FEAST_REDIS"] == "${PLATFORM_FEAST_REDIS:-}"
    assert env["AIRFLOW_VAR_PLATFORM_ML_CUTOFF"] == "2026-06-30T00:00:00Z"
    assert env["PLATFORM_STORAGE"] == "s3"
    volumes = yaml.load((ROOT / "docker-compose.aws.yml").read_text(), Loader=ComposeLoader)[
        "services"
    ]["airflow"]["volumes"]
    assert "./feature_repo:/opt/feature_repo:ro" in volumes  # volumes !override dropped it once
    template = (ANSIBLE / "roles/app/templates/env.j2").read_text()
    assert "PLATFORM_FEAST_REDIS={{ feast_redis | default('10.42.0.121:30379')" in template
    dag = (ROOT / "dags/ml_feature_materialize.py").read_text()
    branch = dag.index('if os.environ.get("PLATFORM_STORAGE", "minio") != "s3":')
    assert branch < dag.index('get_connection("platform_minio")') < dag.index("env |= {")


def test_docker_storage_moves_to_ebs_without_silent_empty_store():
    """Docker and containerd storage is bind-mounted to EBS; systemd waits for the mount; copy without --delete, keep the old copy."""
    tasks = yaml.safe_load((ANSIBLE / "roles/data_disk/tasks/main.yml").read_text())
    text = (ANSIBLE / "roles/data_disk/tasks/main.yml").read_text()
    binds = [t for t in tasks if t.get("ansible.posix.mount", {}).get("fstype") == "none"]
    assert len(binds) == 1 and binds[0]["loop"] == ["containerd", "docker"]
    assert (
        "x-systemd.requires-mounts-for=/mnt/platform-data"
        in binds[0]["ansible.posix.mount"]["opts"]
    )
    assert "RequiresMountsFor=/var/lib/containerd /var/lib/docker" in text
    move = next(t for t in tasks if "block" in t)
    rsync = next(
        t["ansible.builtin.command"]["argv"]
        for t in move["block"]
        if t.get("ansible.builtin.command", {}).get("argv", [""])[0] == "rsync"
    )
    assert "--delete" not in rsync and "-aHAXx" in rsync
    commands = [
        t["ansible.builtin.command"] for t in move["block"] if "ansible.builtin.command" in t
    ]
    mv = next(c for c in commands if c["argv"][0] == "mv")
    assert mv["creates"].endswith(".pre-ebs")
    # Order: mount EBS → move storage → bind mount.
    names = [t["name"] for t in tasks]
    assert (
        names.index("Ghi mount UUID và mount nếu chưa có")
        < names.index(move["name"])
        < names.index(binds[0]["name"])
    )


def test_cdc_on_aws_enables_debezium_and_loads_dp1_cdc_variables():
    """dp1_cdc on EC2: Kafka belongs to the cdc profile too, Debezium on loopback, Airflow loads the CDC Variables."""
    services = yaml.load(
        (ROOT / "docker-compose.aws-stream.yml").read_text(), Loader=ComposeLoader
    )["services"]
    assert "cdc" in services["kafka"]["profiles"]
    assert services["debezium-connect"]["profiles"] == ["cdc"]
    assert services["debezium-connect"]["scale"] == 1
    setup = (ROOT / "scripts/setup/setup_airflow.sh").read_text()
    s3_branch = setup[setup.index("= s3 ]; then") :]
    assert "dp1_cdc" in s3_branch.split("done")[0]
    assert "spark-sql-kafka-0-10" in s3_branch and "PLATFORM_SPARK_PACKAGES_AWS" in s3_branch


def _mb(value: str) -> int:
    """'2560m' / '3g' -> MB."""
    return int(value[:-1]) * (1024 if value.endswith("g") else 1)


@pytest.mark.parametrize("name", ["sku_return_feature_a.sql", "sku_return_feature.sql"])
def test_stream_feature_checkpoints_go_to_disk_not_jobmanager_memory(name):
    """The default checkpoint lives in JobManager memory (≤5 MB); stream A state is ~72 MB ⇒ it must go to a mounted directory."""
    sql = (ROOT / "flink_jobs/ml" / name).read_text()
    assert "SET 'state.checkpoints.dir' = 'file:///opt/flink/output/checkpoints';" in sql
    stream = yaml.load((ROOT / "docker-compose.aws-stream.yml").read_text(), Loader=ComposeLoader)
    for svc in ("flink-jobmanager", "flink-taskmanager"):
        assert "/mnt/platform-data/flink:/opt/flink/output" in stream["services"][svc]["volumes"]


def test_aws_flink_ml_cluster_uses_iam_and_disk_checkpoints():
    """Flink ML 2.2 cluster (2 PyFlink jobs) on EC2: S3 via IAM role (no MinIO key), checkpoints to EBS,
    UI on loopback only, offline output is a directory of Parquet parts."""
    stream = yaml.load((ROOT / "docker-compose.aws-stream.yml").read_text(), Loader=ComposeLoader)
    for svc in ("flink-ml-jobmanager", "flink-ml-taskmanager"):
        conf = stream["services"][svc]
        env = conf["environment"]
        assert not {"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "FEAST_S3_ENDPOINT_URL"} & set(
            env
        )
        assert env["FEAST_SKU_RETURN_24H_PATH"].endswith("/feature-store/sku_return_24h/")
        assert (
            "execution.checkpointing.dir: file:///opt/flink/output/checkpoints"
            in env["FLINK_PROPERTIES"]
        )
        assert conf["volumes"] == ["/mnt/platform-data/flink-ml:/opt/flink/output"]
    ports = stream["services"]["flink-ml-jobmanager"]["ports"]
    assert ports == ["127.0.0.1:${FLINK_ML_UI_PORT:-8086}:8081"]


def test_prometheus_reloads_config_when_targets_change():
    """10/30: with reload off, changing the EC2 IP in values left Prometheus scraping the old IP until the pod died."""
    values = yaml.safe_load((ROOT / "infra/k8s/observability/prometheus-values.yaml").read_text())
    assert values["configmapReload"]["prometheus"]["enabled"] is True


def test_aws_flink_taskmanager_has_room_for_stream_a_state():
    """10/30: 1024m made checkpoints OOM (Java heap) with ~401k replayed Returns; Flink promises RAM within the container ceiling."""
    tm = yaml.load((ROOT / "docker-compose.aws-stream.yml").read_text(), Loader=ComposeLoader)[
        "services"
    ]["flink-taskmanager"]
    props = dict(
        line.split(": ", 1) for line in tm["environment"]["FLINK_PROPERTIES"].strip().splitlines()
    )
    process = _mb(props["taskmanager.memory.process.size"])
    assert process >= 4096
    assert process < _mb(tm["mem_limit"])
    assert props["jobmanager.rpc.address"] == "flink-jobmanager"


def test_spark_worker_promise_fits_container_limit():
    """Workers must not promise RAM above the container ceiling: the smallest executor (1536m) packed full + overhead + worker JVM."""
    services = yaml.load((ROOT / "docker-compose.aws.yml").read_text(), Loader=ComposeLoader)[
        "services"
    ]
    for name in ("spark-worker-1", "spark-worker-2"):
        worker = _mb(services[name]["environment"]["SPARK_WORKER_MEMORY"])
        executors = worker // 1536
        assert worker + executors * 384 + 512 <= _mb(services[name]["mem_limit"]), name


def test_aws_maintenance_targets_a_configured_real_table():
    """The S3 branch replaces the demo table: every table in platform_maintenance_tables must have a policy in the AWS file."""
    import json

    from spark_jobs.lib.maintenance_policy import validated_config

    setup = (ROOT / "scripts/setup/setup_airflow.sh").read_text()
    s3_branch = setup[setup.index("= s3 ]; then") :].split("\nfi\n")[0]
    assert "< configs/iceberg_maintenance_aws.json" in s3_branch
    tables = json.loads(re.search(r"platform_maintenance_tables '(\[.*?\])'", s3_branch).group(1))
    policies = json.loads((ROOT / "configs/iceberg_maintenance_aws.json").read_text())["tables"]
    assert tables and set(tables) <= set(policies)
    for name in tables:
        assert not name.startswith("lakehouse.demo.")
        config = validated_config(policies[name])
        assert config["orphans_dry_run"] is True and config["zorder_columns"] == []


def test_docker_log_size_is_capped_before_containers_start():
    """Each container keeps at most 300 MB of logs; applied before the role that creates containers, restart Docker on change."""
    import json

    tasks = yaml.safe_load((ANSIBLE / "roles/base/tasks/main.yml").read_text())
    daemon = next(
        t
        for t in tasks
        if t.get("ansible.builtin.copy", {}).get("dest") == "/etc/docker/daemon.json"
    )
    options = json.loads(daemon["ansible.builtin.copy"]["content"])
    assert options == {"log-driver": "json-file", "log-opts": {"max-size": "100m", "max-file": "3"}}
    restart = next(
        t for t in tasks if t.get("ansible.builtin.service", {}).get("state") == "restarted"
    )
    assert restart["when"] == f"{daemon['register']}.changed"
    roles = [r["role"] for r in yaml.safe_load((ANSIBLE / "site.yml").read_text())[0]["roles"]]
    assert roles.index("base") < roles.index("app") < roles.index("datahub")


def test_mlflow_on_aws_uses_s3_iam_and_private_listener():
    """MLflow on EC2: no MinIO, real S3 artifacts, listens on loopback + private IP; Ansible enables exactly mlflow."""
    services = yaml.load((ROOT / "docker-compose.aws.yml").read_text(), Loader=ComposeLoader)[
        "services"
    ]
    mlflow = services["mlflow"]
    assert "minio" not in mlflow["depends_on"]
    assert not {"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "MLFLOW_S3_ENDPOINT_URL"} & set(
        mlflow["environment"]
    )
    command = mlflow["command"]
    assert (
        command[command.index("--artifacts-destination") + 1] == "s3://${LAKEHOUSE_BUCKET}/mlflow"
    )
    assert mlflow["ports"] == [
        "127.0.0.1:${MLFLOW_PORT:-5050}:5000",
        "${PLATFORM_PRIVATE_IP:?PLATFORM_PRIVATE_IP_required}:${MLFLOW_PORT:-5050}:5000",
    ]
    assert (
        "PLATFORM_PRIVATE_IP={{ ansible_default_ipv4.address"
        in (ANSIBLE / "roles/app/templates/env.j2").read_text()
    )
    tasks = (ANSIBLE / "roles/app/tasks/main.yml").read_text()
    assert "docker compose --profile ml up -d --wait --wait-timeout 180 mlflow" in tasks


def test_kfp_on_eks_reads_s3_by_pod_identity_without_minio():
    """The train image does not hardcode MinIO; pipeline-runner only reads feature-store/*; the aws variant drops the Secret."""
    dockerfile = (ROOT / "docker/Dockerfile.train").read_text()
    assert "minio:9000" not in dockerfile and "s3://platform/" not in dockerfile
    identity = (ROOT / "infra/aws/kubernetes/identity.tf").read_text()
    assert (
        'pipeline_reader = { namespace = "kubeflow", service_account = "pipeline-runner" }'
        in identity
    )
    block = identity[identity.index('resource "aws_iam_role_policy" "pipeline_reader"') :]
    assert "data.aws_iam_policy_document.feature_reader.json" in block.split("}")[0]
    reader = identity[identity.index('data "aws_iam_policy_document" "feature_reader"') :]
    reader = reader[: reader.index('resource "aws_iam_role_policy" "feature_reader"')]
    assert '"s3:GetObject"' in reader and "Put" not in reader and "Delete" not in reader
    pipeline = (ROOT / "ml/training_pipeline.py").read_text()
    assert 'SECRET = "platform-minio" if TARGET == "kind" else None' in pipeline
    script = (ROOT / "scripts/run/run_training_pipeline.sh").read_text()
    assert 'if [[ "$PIPELINE_TARGET" == aws ]]' in script and "REGISTRY must be ECR" in script


def test_kserve_on_eks_reads_registry_by_pod_identity():
    """return-risk: its own SA with a read-only feature-store role; the MinIO Secret is optional; the s3 branch creates no Secret."""
    docs = list(yaml.safe_load_all((ROOT / "infra/k8s/ml/return-risk.yaml").read_text()))
    account = next(d for d in docs if d["kind"] == "ServiceAccount")
    service = next(d for d in docs if d["kind"] == "InferenceService")
    predictor = service["spec"]["predictor"]
    assert predictor["serviceAccountName"] == account["metadata"]["name"] == "return-risk"
    container = predictor["containers"][0]
    assert {"secretRef": {"name": "platform-minio", "optional": True}} in container["envFrom"]
    env = {e["name"]: e["value"] for e in container["env"]}
    assert env["FEAST_REGISTRY"] == "@@FEAST_ROOT@@/registry.db"
    identity = (ROOT / "infra/aws/kubernetes/identity.tf").read_text()
    assert (
        'model_reader    = { namespace = "ml-serving", service_account = "return-risk" }'
        in identity
    )
    block = identity[identity.index('resource "aws_iam_role_policy" "model_reader"') :]
    assert "data.aws_iam_policy_document.feature_reader.json" in block.split("}")[0]
    script = (ROOT / "scripts/run/deploy_return_risk.sh").read_text()
    s3 = script[script.index("== s3 ]]; then") : script.index("\nelse\n")]
    assert "platform-minio" not in s3 and "REGISTRY must be ECR" in s3


def test_training_sample_and_kfp_memory_fit_eks_nodes():
    """Training set at 2‰ (1% once OOMed an 8 GB node); every KFP step declares RAM, load the most."""
    dag = (ROOT / "dags/ml_feature_materialize.py").read_text()
    assert '"--sample-per-mille",\n            "2",' in dag
    pipeline = (ROOT / "ml/training_pipeline.py").read_text()
    assert 'loaded.set_memory_request("2G").set_memory_limit("4G")' in pipeline
    assert 'task.set_memory_request("1G").set_memory_limit("3G")' in pipeline


def test_ec2_allows_memory_optimized_r6i_within_quota():
    """r6i.xlarge (4 vCPU/32 GiB) is allowed: same vCPU quota, double the RAM for DataHub/Trino/Kafka at once."""
    text = (ROOT / "infra/aws/terraform/variables.tf").read_text()
    block = text[text.index('variable "instance_type"') :]
    assert '"r6i.xlarge"' in block[: block.index("}\n}")]


def test_trino_on_aws_uses_iam_catalog_on_loopback_only_when_measuring():
    """Trino on AWS: S3 catalog without MinIO key/endpoint, loopback port, enabled only by the query profile."""
    trino = yaml.load((ROOT / "docker-compose.aws.yml").read_text(), Loader=ComposeLoader)[
        "services"
    ]["trino"]
    assert trino["profiles"] == ["query"]
    assert trino["volumes"] == ["./trino/catalog-aws:/etc/trino/catalog:ro"]
    assert trino["ports"] == ["127.0.0.1:${TRINO_PORT:-8083}:8080"]
    assert "minio" not in trino["depends_on"]
    lake = (ROOT / "trino/catalog-aws/lakehouse.properties").read_text()
    assert "aws-access-key" not in lake and "minio" not in lake and "s3.endpoint" not in lake
    assert "iceberg.rest-catalog.warehouse=${ENV:WAREHOUSE_NAME}" in lake


def test_code_only_sync_tag_covers_bind_mounted_code():
    """--tags code only runs rsync; every code directory it ships is an Airflow bind mount."""
    tasks = yaml.safe_load((ANSIBLE / "roles/app/tasks/main.yml").read_text())
    tagged = [t["name"] for t in tasks if "code" in t.get("tags", [])]
    assert tagged == ["Đồng bộ repo bằng rsync, loại secret và thư mục phát sinh"]
    volumes = yaml.load((ROOT / "docker-compose.aws.yml").read_text(), Loader=ComposeLoader)[
        "services"
    ]["airflow"]["volumes"]
    for code in ("./dags:", "./spark_jobs:", "./scripts:", "./feature_repo:"):
        assert any(v.startswith(code) for v in volumes), code


def test_wave2_vault_ingress_and_letsencrypt_for_eks():
    """Vault on EC2 loopback + EBS; public NLB ingress; Let's Encrypt HTTP-01; every route shares the issuer."""
    vault = yaml.load((ROOT / "docker-compose.aws.yml").read_text(), Loader=ComposeLoader)[
        "services"
    ]["vault"]
    assert vault["ports"] == ["127.0.0.1:${VAULT_PORT:-8200}:8200"]
    assert vault["volumes"] == ["/mnt/platform-data/vault:/vault/file"]
    service = yaml.safe_load(
        (ROOT / "infra/k8s/ingress/nginx-ingress-eks-values.yaml").read_text()
    )["controller"]["service"]
    assert service["type"] == "LoadBalancer"
    assert service["annotations"]["service.beta.kubernetes.io/aws-load-balancer-type"] == "nlb"
    issuer = yaml.safe_load((ROOT / "infra/k8s/ingress/issuer-letsencrypt-http01.yaml").read_text())
    assert issuer["spec"]["acme"]["server"].startswith("https://acme-v02.")
    assert issuer["spec"]["acme"]["solvers"] == [
        {"http01": {"ingress": {"ingressClassName": "nginx"}}}
    ]
    script = (ROOT / "scripts/setup/setup_ingress.sh").read_text()
    assert script.count('-e "s/cluster-issuer: platform-local-ca/cluster-issuer: $issuer/"') == 2
    assert ': "${LE_EMAIL:?' in script and "ROOT=${VAULT_TOKEN:-" in script


def test_wave2_prometheus_pulls_ec2_exporters_with_alerts():
    """EC2 exporters are only open to the cluster SG; Ansible enables them; up/disk/slot rules are loaded only on EKS."""
    variables = (ROOT / "infra/aws/kubernetes/variables.tf").read_text()
    assert "contains([5432, 8181, 5050, 8200, 9100, 9187], p)" in variables
    tasks = (ANSIBLE / "roles/app/tasks/main.yml").read_text()
    assert "spark-worker-2 airflow node-exporter postgres-exporter" in tasks
    values = yaml.safe_load(
        (ROOT / "infra/k8s/observability/prometheus-values-aws.yaml").read_text()
    )
    jobs = yaml.safe_load(values["extraScrapeConfigs"])
    assert {j["job_name"]: j["static_configs"][0]["targets"] for j in jobs} == {
        "ec2-node": ["10.42.0.151:9100"],
        "ec2-postgres": ["10.42.0.151:9187"],
    }
    rules = {
        r["alert"]: r["expr"]
        for g in values["serverFiles"]["alerting_rules.yml"]["groups"]
        for r in g["rules"]
    }
    assert rules["EC2ExporterDown"] == 'up{job=~"ec2-.*"} == 0'
    assert 'mountpoint="/mnt/platform-data"' in rules["EC2DataDiskAlmostFull"]
    assert "> 0.8" in rules["EC2DataDiskAlmostFull"]
    assert "confirmed_flush_lsn > 1073741824" in rules["PostgresSlotWalRetained"]
    script = (ROOT / "scripts/setup/deploy_kind_observability.sh").read_text()
    assert (
        '[[ "${KCTX:-kind-platform}" == kind-* ]] || prom_aws=(-f $V/prometheus-values-aws.yaml)'
        in script
    )
    assert '"${prom_aws[@]}" --wait' in script


def test_letsencrypt_first_issue_opens_then_closes_each_route():
    """HTTP-01 on F5 NGINX: edit-in-place every Ingress; first apply the open version, wait for Ready, then the full one."""
    for f in ("feature-api", "agent-ui", "ops-ui"):
        doc = yaml.safe_load((ROOT / f"infra/k8s/ingress/{f}.yaml").read_text().replace("@@", "x"))
        assert (
            doc["metadata"]["annotations"]["acme.cert-manager.io/http01-edit-in-place"] == "true"
        ), f
    script = (ROOT / "scripts/setup/setup_ingress.sh").read_text()
    fn = script[
        script.index("apply_ing() {") : script.index("\n}\n", script.index("apply_ing() {"))
    ]
    opened, wait, full = (
        fn.index("/nginx.org\\/basic-auth/d"),
        fn.index('wait --for=condition=Ready "certificate/$2"'),
        fn.index('$K apply -f - <<<"$y"'),
    )
    assert opened < wait < full
    assert 'ingress.kubernetes.io/ssl-redirect: "false"' in fn
    # The Secret can exist before a real cert does ⇒ ask whether the Certificate is Ready, not the secret.
    assert '(@.type=="Ready")].status}\'' in fn and "!= True ]]" in fn
    assert "get secret" not in fn
    assert '.yaml" | apply_ing "${f%/*}" "${f#*/}-tls"' in script
    assert 'ops-ui.yaml | apply_ing "$ns" "$name-tls"' in script


def test_wave4_rag_reaches_pgvector_nodeport_with_iam_s3():
    """Airflow on EC2: mounts rag, S3 lakehouse root, pgvector JSON connection from Ansible; SG opens exactly 30432."""
    airflow = yaml.load((ROOT / "docker-compose.aws.yml").read_text(), Loader=ComposeLoader)[
        "services"
    ]["airflow"]
    assert "./rag:/opt/rag/rag:ro" in airflow["volumes"]
    assert airflow["environment"]["AIRFLOW_VAR_PLATFORM_RAG_ROOT"] == "s3://${LAKEHOUSE_BUCKET}/rag"
    assert airflow["environment"]["AIRFLOW_CONN_PLATFORM_RAG_PG"].startswith(
        "${PLATFORM_RAG_PG_CONN:?"
    )
    env = (ANSIBLE / "roles/app/templates/env.j2").read_text()
    assert "(rag_pg | default(':30432')).split(':')" in env
    assert "| to_json | quote }}" in env.split("PLATFORM_RAG_PG_CONN=")[1].splitlines()[0]
    tf = (ROOT / "infra/aws/kubernetes/cluster.tf").read_text()
    rule = tf[tf.index('"data_to_pgvector"') :].split("}")[0]
    assert (
        "from_port                    = 30432" in rule
        and "to_port                      = 30432" in rule
    )
    assert "data_security_group_id" in rule


def test_wave4_stream_feature_a_keeps_kafka_on_ebs_and_pushes_with_iam():
    """Gap #12: Kafka writes logs to a volume (local too) and to EBS on AWS; the pusher has no key, Redis comes from Ansible."""
    base = yaml.load((ROOT / "docker-compose.yml").read_text(), Loader=ComposeLoader)["services"]
    assert base["kafka"]["environment"]["KAFKA_LOG_DIRS"] == "/var/lib/kafka/data"
    assert "kafka-data:/var/lib/kafka/data" in base["kafka"]["volumes"]
    stream = yaml.load((ROOT / "docker-compose.aws-stream.yml").read_text(), Loader=ComposeLoader)[
        "services"
    ]
    assert stream["kafka"]["volumes"] == ["/mnt/platform-data/kafka:/var/lib/kafka/data"]
    env = stream["feast-pusher"]["environment"]
    assert not {"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "FEAST_S3_ENDPOINT_URL"} & set(env)
    assert env["FEAST_REDIS"].startswith("${PLATFORM_FEAST_REDIS:?")
    assert env["FEAST_REGISTRY"] == "s3://${LAKEHOUSE_BUCKET}/feature-store/registry.db"


def test_wave4_flink_a_reads_replayed_topic_without_postgres_lookup():
    """Variant A: reads exactly the topic the replay script publishes, writes the topic the pusher reads, no JDBC; the submit script picks the SQL."""
    sql = (ROOT / "flink_jobs/ml/sku_return_feature_a.sql").read_text()
    replay = (ROOT / "feature_repo/replay_return_events.py").read_text()
    topic = replay.split('TOPIC = "')[1].split('"')[0]
    assert f"'topic' = '{topic}'" in sql and "'topic' = 'ml.sku_return_24h'" in sql
    assert "jdbc" not in sql.lower() and "@@PG_" not in sql
    pusher = (ROOT / "feature_repo/push_stream_features.py").read_text()
    assert '"FEAST_PUSH_TOPIC", "ml.sku_return_24h"' in pusher
    run = (ROOT / "scripts/run/run_sku_return_feature.sh").read_text()
    assert "sql_file=${FEATURE_SQL:-flink_jobs/ml/sku_return_feature.sql}" in run
    assert "if grep -q '@@PG_' \"$sql_file\"; then" in run
    replay_sql = (ROOT / "sql/ml/return_events_replay.sql").read_text()
    assert "l.label = 1" in replay_sql and "ORDER BY l.label_available_at" in replay_sql


def test_wave5_ci_role_is_namespace_scoped_and_ecr_limited():
    """EC2 joins EKS via the platform-ci group (no AWS access policy); full RBAC only inside the CI namespace;
    ECR only pushes/pulls the project repos, never deletes."""
    tf = (ROOT / "infra/aws/kubernetes/cluster.tf").read_text()
    entry = tf[tf.index('resource "aws_eks_access_entry" "ci"') :].split("}")[0]
    assert 'kubernetes_groups = ["platform-ci"]' in entry
    assert (
        "aws_eks_access_policy_association"
        not in tf[tf.index('resource "aws_eks_access_entry" "ci"') :]
    )
    api = tf[tf.index('"data_to_api"') :].split("}")[0]
    assert "from_port                    = 443" in api and "data_security_group_id" in api
    docs = [d for d in yaml.safe_load_all((ROOT / "infra/k8s/ci/rbac.yaml").read_text()) if d]
    bindings = [d for d in docs if d["kind"] in ("RoleBinding", "ClusterRoleBinding")]
    assert all(
        b["subjects"]
        == [{"apiGroup": "rbac.authorization.k8s.io", "kind": "Group", "name": "platform-ci"}]
        for b in bindings
    )
    cluster_roles = [d for d in docs if d["kind"] == "ClusterRole"]
    assert [r["rules"] for r in cluster_roles] == [
        [{"apiGroups": [""], "resources": ["namespaces"], "verbs": ["get", "list"]}]
    ]
    assert {d["metadata"]["namespace"] for d in docs if d["kind"] == "Role"} == {
        "kagent",
        "feature-store",
        "agentgateway",
        "langfuse",
    }
    iam = (ROOT / "infra/aws/terraform/iam.tf").read_text()
    ecr = iam[iam.index('data "aws_iam_policy_document" "ci_ecr"') :]
    assert "resources = [for repo in aws_ecr_repository.application : repo.arn]" in ecr
    assert "Delete" not in ecr.split('resource "aws_iam_role_policy" "ci_ecr"')[0]
    assert 'actions   = ["eks:DescribeCluster"]' in ecr
    assert "cluster/${var.name_prefix}-apps" in ecr


def test_mle_drift_job_reads_s3_with_pipeline_reader_identity():
    """Drift monitoring on EKS: pipeline-runner SA (Pod Identity, read-only feature-store/*), no key, pinned Evidently,
    includes a per-Channel shift comparison; the code mount is exactly the `ml` package."""
    job = yaml.safe_load((ROOT / "infra/k8s/ml/drift-job.yaml").read_text())
    assert job["metadata"]["namespace"] == "kubeflow"
    spec = job["spec"]["template"]["spec"]
    assert spec["serviceAccountName"] == "pipeline-runner"
    container = spec["containers"][0]
    command = container["args"][0]
    assert '"evidently==0.7.23"' in command
    assert '"pandas==2.3.3"' in command  # pandas 3 breaks Evidently 0.7 with ImportError
    assert "s3://@@LAKEHOUSE_BUCKET@@/feature-store/training_lines" in command
    assert "--shift-channel shopee" in command
    assert not {"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"} & {
        e["name"] for e in container["env"]
    }
    assert container["volumeMounts"] == [{"name": "code", "mountPath": "/work/ml"}]
    identity = (ROOT / "infra/aws/kubernetes/identity.tf").read_text()
    assert (
        'pipeline_reader = { namespace = "kubeflow", service_account = "pipeline-runner" }'
        in identity
    )


def test_stopped_ec2_is_not_replaced_by_terraform():
    """A stopped machine has no public IP ⇒ associate_public_ip_address must not force EC2 replacement (lost the root disk on 10/30)."""
    lifecycle = (TF_DIR / "compute.tf").read_text().split("lifecycle {")[1]
    assert "ignore_changes = [ami, associate_public_ip_address]" in lifecycle


def test_fresh_pgvector_volume_enables_vector_extension():
    """The new pgdata PVC (10/30) had no extension ⇒ Feast RAG failed with `type "vector" does not exist`; initdb must enable it."""
    docs = list(yaml.safe_load_all((ROOT / "infra/k8s/rag/pgvector.yaml").read_text()))
    init = next(d for d in docs if d["kind"] == "ConfigMap")
    pod = next(d for d in docs if d["kind"] == "Deployment")["spec"]["template"]["spec"]
    assert "CREATE EXTENSION IF NOT EXISTS vector" in "".join(init["data"].values())
    assert {"name": "init", "mountPath": "/docker-entrypoint-initdb.d"} in pod["containers"][0][
        "volumeMounts"
    ]
    assert {"name": "init", "configMap": {"name": init["metadata"]["name"]}} in pod["volumes"]
