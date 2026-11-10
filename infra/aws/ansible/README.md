<!-- Purpose: how to run Ansible to configure the milestone 1 EC2 after the AWS Terraform is applied. -->
# Ansible AWS Data Platform

Configures the EC2 created by [Terraform](../terraform/README.md): installs Docker, mounts the EBS
at `/mnt/platform-data`, syncs the repo, runs Compose (`docker-compose.yml` +
`docker-compose.aws.yml`), and creates the Lakekeeper warehouse on S3.

SSH does not cross the Internet: `ProxyCommand` calls `aws ssm start-session`, so
the security group does not need port 22 open.

The control machine needs a logged-in AWS CLI, `session-manager-plugin`, Ansible and
the collections below. The private key `~/.ssh/platform_aws` must match Terraform's
`ssh_public_key`; change both the inventory and the tunnel command if you use another path.

```bash
cd infra/aws/ansible
ansible-galaxy collection install -r requirements.yml
cp inventory.example.ini inventory.ini                 # instance_id, region
cp group_vars/all.example.yml group_vars/all.yml       # Terraform outputs + secrets
ansible-vault encrypt group_vars/all.yml
ansible-playbook -i inventory.ini site.yml --ask-vault-pass
# Only dags/, spark_jobs/, scripts/, trino/ changed (bind mounts): sync code, ~1 minute, no restart
ansible-playbook -i inventory.ini site.yml --ask-vault-pass --tags code
# Optional: generate sources and push them to S3, then smoke-test Iceberg write/read
ansible-playbook -i inventory.ini site.yml --ask-vault-pass --tags source_data
ansible-playbook -i inventory.ini site.yml --ask-vault-pass --tags smoke
```

Once the deployment is approved and complete: get the command with
`terraform output -raw port_forward_command` in the Terraform directory, run the
printed SSH command, then open
`http://127.0.0.1:8081` (Airflow), `:8080` (Spark), `:8181` (Lakekeeper).

The overlay enables only PostgreSQL, Lakekeeper, Spark (1 master + 2 workers) and Airflow,
with a total limit of ≈14 GiB to fit a 16 GiB `t3.xlarge`. Kafka, Flink, Trino and Debezium
are off by default; enable them one at a time when needed and turn others off to avoid running out of RAM.

Review the audit results and the outstanding prerequisites
before running the playbook on the EC2.
