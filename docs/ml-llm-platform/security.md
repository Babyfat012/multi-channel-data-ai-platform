# Security and Vault on AWS

Centralized secret management with Vault.

Code and configuration: [Vault setup](../../scripts/setup/setup_vault.sh); secrets are used by CI and the ingress.

The results below come from runs on AWS. They do not mean the service is up while you read this.

## On AWS

Every shared secret (passwords, keys) lives in exactly one place: the KV v2 store `secret/platform/*` in Vault. Code and the repo contain no secret values.

How Vault runs on AWS:

- Vault 2.1.1 runs on EC2 with Docker Compose.
- Data is stored on the EBS volume at `/mnt/platform-data/vault`.
- Vault only listens on loopback (port 8200, reachable only from the machine itself); operators reach the machine through SSM.
- Vault was re-initialized after an incident that required replacing the EC2 instance ([`setup_vault.sh`](../../scripts/setup/setup_vault.sh)).

| Secret | Who reads it |
|---|---|
| `agent-ui`, `ops-ui` | [`setup_ingress.sh`](../../scripts/setup/setup_ingress.sh) generates them once, then reads them to create the htpasswd files for NGINX basic auth |
| `postgres` | [`vault_env.sh`](../../scripts/setup/vault_env.sh), used by jobs and CI (Flink, Jenkins); never written to the repo |
| `minio` | only used in the local setup (AWS uses an IAM role, there is no MinIO) |

The screenshot below only prints the status, secret names and version information, **not the secret values**. It shows:

- `Sealed false`: Vault is unsealed and ready.
- There are 4 keys under `secret/platform`.
- `agent-ui` is at version 1, created exactly when `setup_ingress.sh` first ran after Vault was re-initialized.

![Vault on EC2: status unsealed 2.1.1, kv list secret/platform (agent-ui, minio, ops-ui, postgres), metadata agent-ui version 1](../evidence/aws/vault-status-kv-20261031.png)

<details>
<summary>Reference: design, development guide and records from before the AWS deployment</summary>

Local records keep their original environment and are not a substitute for cloud evidence.

<!-- Purpose: centralized secret management with HashiCorp Vault -->
## Centralized secrets — HashiCorp Vault

Secrets live in **one place** (Vault) so the whole organization shares them, instead of being scattered through the code.

## Design

Secret flow: `setup_vault.sh` loads secrets from `.env` and the agentgateway k8s Secret into Vault. Jenkins only holds a read-only token, and scripts read secrets through `vault_env.sh`.

```
.env + k8s Secret agentgateway ──setup_vault.sh──► Vault KV v2  secret/platform/{minio,postgres,agentgateway}
                                      (setup_jenkins.sh / setup_ingress.sh add jenkins, agent-ui)
                                                    │  policy platform-read (read-only)
                                                    ▼
                               read-only CI token ──► Jenkins (only credential: vault-ci-token)
                                                    ▼
                         scripts/setup/vault_env.sh ──► run_sku_return_feature.sh, feast-pusher, …
```

| Component | File |
|---|---|
| Vault 2.1.1 service, server mode, file storage in volume `vault-data` | `vault` in [`docker-compose.yml`](../../docker-compose.yml) (profile `security`) |
| One-time init + unseal + enable KV v2 + load secrets + policy + CI token | [`scripts/setup/setup_vault.sh`](../../scripts/setup/setup_vault.sh) |
| Read secrets into environment variables | [`scripts/setup/vault_env.sh`](../../scripts/setup/vault_env.sh) |
| Where secrets are used: Flink job submission, stream Jenkinsfile | [`run_sku_return_feature.sh`](../../scripts/run/run_sku_return_feature.sh), [`stream-features.Jenkinsfile`](../../ci/jenkins/stream-features.Jenkinsfile) |

## Decisions

- **Server mode, not dev mode.** Dev mode keeps secrets in RAM and loses them on restart. Server mode writes them to a file.
  In exchange, after every restart Vault is **sealed** (locked), and `setup_vault.sh` must unseal it again.
- **The unseal keys and root token** live in `out/vault/init.json` (gitignored, file mode 600). This is only acceptable
  locally. On GKE: auto-unseal with Cloud KMS, and revoke the root token after setup.
- **Least privilege:** CI uses a token with the `platform-read` policy, which can only read `secret/data/platform/*`; the token has a renewal period of 768 hours.
- Order in which scripts look for credentials: environment variables first, then Vault (if `VAULT_TOKEN` is set), and finally the `.env` file.

## After a machine restart

`scripts/setup/up_ml_base.sh` rebuilds the shared base with one command: MinIO + PostgreSQL + Lakekeeper,
Vault unseal, Jenkins, and reloading (materializing) Redis if Redis is empty. Reason: Redis in kind does not persist to disk; on EKS, Redis has a PVC and AOF, see [Feature Store](feature_store.md#verification-on-eks).
This script does not start the LLM or the agents.

When stopping Compose services, **always name the service explicitly**. `docker compose --profile X stop` without a
name also stops MinIO/PostgreSQL, which Feast, MLflow and Vault depend on.

## Evidence

```text
eval "$(scripts/setup/vault_env.sh postgres)"  → POSTGRES_USER=platform POSTGRES_DB=sources (password 18 characters)
POST secret/data/platform/postgres with the CI token → HTTP 403 (read-only)
vault_env.sh agentgateway                        → AGW_AGENT_KEY 36 characters, matches the k8s Secret
restart vault container                          → sealed=true; setup_vault.sh → sealed=false
```

Reading the results:

1. `vault_env.sh` reads the postgres secret into environment variables (the password is 18 characters long and is not printed).
2. A write to Vault with the CI token is rejected with `403`, as expected for a read-only token.
3. The gateway key read from Vault matches the Secret in k8s.
4. After the container restarts, Vault is sealed (`sealed=true`); running `setup_vault.sh` unseals it (`sealed=false`).

</details>

[Section index](README.md) · [Main index](../README.md)
