# External Routing and Gateway on EKS

Ingress, UIs, auth/rate limiting, domain and HTTPS.

Code and configuration: the config file and screenshot for each route are linked in the EKS results section below.

The results below come from runs on AWS. They do not mean the service is up while you read this.

## On EKS (2026-10-30/31)

Every request from outside the cluster goes through a single entry point (ingress):

- **Entry point:** F5 NGINX Ingress Controller (namespace `ingress`), behind an AWS NLB (network load balancer).
- **HTTPS:** cert-manager obtains real Let's Encrypt certificates (HTTP-01 validation) for each host of the form `<name>.15-134-182-157.sslip.io`.
- **Routes:** every UI sits behind this entry point. Routes are created by [`setup_ingress.sh`](../../scripts/setup/setup_ingress.sh).
- **Passwords:** generated once and stored in Vault.

Route table (the "Host" column is the first part of the domain name):

| Host | Backend service | NGINX setting | Auth |
|---|---|---|---|
| `api.` | Feature API (Web API) | [`feature-api.yaml`](../../infra/k8s/ingress/feature-api.yaml): HTTPS, `limit-req-rate` 50r/s, burst 100 | none (public API, rate limited) |
| `agent.` | kagent UI (agent test UI) | [`agent-ui.yaml`](../../infra/k8s/ingress/agent-ui.yaml): HTTPS, basic auth, 5r/s burst 10, returns 429 when exceeded | `seller` |
| `grafana.` | Grafana (metrics + Loki logs) | [`ops-ui.yaml`](../../infra/k8s/ingress/ops-ui.yaml) | `ops` + Grafana admin |
| `traces.` | Jaeger UI | `ops-ui.yaml` | `ops` |
| `metrics.` | Prometheus UI | `ops-ui.yaml` | `ops` |
| `registry.` | agentregistry UI | `ops-ui.yaml` | `ops` |

**Checking basic auth and rate limiting on the agent UI.** Requests sent from a laptop, without a password:

- One request: returns `401` with the header `WWW-Authenticate: Basic realm="PLATFORM agent UI"`, i.e. a login is required.
- 40 concurrent requests: 12 got `401` (within the limit, so they reached the password check), 28 got `429` (over the limit of 5 requests/second plus burst 10, so NGINX blocked them right away, before the password check).

![agent. without password: 401 Basic realm PLATFORM agent UI; 40 parallel requests: 12 × 401, 28 × 429](../evidence/aws/agent-ui-basic-auth-rate-limit-20261031.png)

UIs behind the gateway (logged in with basic auth, over HTTPS):

![Grafana via https://grafana.…sslip.io: PLATFORM dashboard folder](../evidence/aws/grafana-dashboards-https-20261030.png)

![Jaeger via https://traces.…sslip.io: searching agentgateway traces](../evidence/aws/jaeger-search-https-20261030.png)

![kagent UI via https://agent.…sslip.io: 3 agents coordinator, data-agent, model-agent](../evidence/aws/kagent-ui-agents-20261030.png)

![agentregistry UI via https://registry.…sslip.io: 1 MCP server and 3 published agents](../evidence/aws/agent-registry-ui-20261030.png)

Logs viewed in Grafana (Explore → Loki): [Observability](observability.md#on-eks).
HTTPS Web API: [Swagger through `api.`](web_api_mcp.md#on-eks).

<details>
<summary>Reference: design, development guide and records from before the AWS deployment</summary>

Local records keep their original environment and are not a substitute for cloud evidence.

<!-- Purpose: NGINX Ingress for the LLM Platform services — HTTPS, basic auth, rate limit -->
## Gateway — NGINX Ingress, HTTPS, auth

Goals:

- The services (metrics, logs, traces, agent test UI, registry UI) all sit behind the gateway.
- The agent test UI has basic auth and rate limiting.
- The data-pulling Web API has a domain name and HTTPS.

## Choosing a controller

The community project **ingress-nginx** is no longer maintained: its last chart release is 4.15.1 (Mar 2026), with no more
security patches. So the project uses **F5 NGINX Ingress Controller** (open source, chart `nginx-ingress` 2.7.3 /
app 5.6.3). It is still an "NGINX Ingress Controller" as the design calls for; annotations take the form `nginx.org/*`.

```bash
helm --kube-context kind-platform install nginx-ingress oci://ghcr.io/nginx/charts/nginx-ingress \
  --version 2.7.3 -n ingress --create-namespace --set controller.service.type=NodePort \
  --set controller.service.httpPort.nodePort=30080 --set controller.service.httpsPort.nodePort=30443 \
  --set controller.ingressClass.name=nginx --set controller.ingressClass.setAsDefaultIngress=true
scripts/setup/setup_ingress.sh
```

| Route | Host (local) | Protection | File |
|---|---|---|---|
| Feature API (data-pulling Web API) | `api.<node-ip>.sslip.io` | HTTPS, HTTP→HTTPS redirect, 50 r/s | [`feature-api.yaml`](../../infra/k8s/ingress/feature-api.yaml) |
| kagent UI (agent test chat) | `agent.<node-ip>.sslip.io` | HTTPS + **basic auth** + **5 r/s, burst 10, 429** | [`agent-ui.yaml`](../../infra/k8s/ingress/agent-ui.yaml) |
| Prometheus (metrics) | `metrics.<node-ip>.sslip.io` | HTTPS + basic auth `ops` | [`ops-ui.yaml`](../../infra/k8s/ingress/ops-ui.yaml) |
| Grafana (metrics + logs via Loki), Jaeger (traces) | `grafana.` / `traces.` | HTTPS + basic auth `ops` | same template |
| Agent registry UI (agentregistry 0.4.0, no auth of its own) | `registry.` | HTTPS + basic auth `ops` | same template |

- **Domain**: sslip.io is a DNS service that turns names into IPs, e.g. `api.172-25-0-2.sslip.io` points to `172.25.0.2` (the kind node IP).
  This gives a real domain name without buying one. On GKE, use your own domain pointing at the load balancer's static IP.
- **HTTPS**: cert-manager issues certificates from the internal CA [`issuer.yaml`](../../infra/k8s/ingress/issuer.yaml)
  (named `platform-local-ca`; the CA certificate is exported to `out/ingress/platform-local-ca.crt` for clients to install and trust). On GKE, use ACME/Let's Encrypt.
- **UI passwords** are randomly generated and stored in Vault: `secret/platform/agent-ui` (agent UI) and `secret/platform/ops-ui` (operations UIs).
  `setup_ingress.sh` creates htpasswd secrets in k8s from these passwords; the passwords are not in the repo.

## Evidence (from the host machine)

```text
GET http://api.172-25-0-2.sslip.io:30080/            → 301 → https://…
GET https://api.172-25-0-2.sslip.io:30443/healthz    → 200, ssl_verify_result = 0 (cert valid against the CA)
GET https://agent.172-25-0-2.sslip.io:30443/          → 401 without password, 200 with password
40 concurrent requests with password                 → 11 × 200, 29 × 429 (rate limit)
GET https://metrics.172-25-0-2.sslip.io:30443/        → 401; with the ops password: /api/v1/query?query=up → 12 targets
GET https://grafana.172-25-0-2.sslip.io:30443/        → 401 without password, 200 with password (Loki logs viewed through Grafana Explore)
GET https://traces.172-25-0-2.sslip.io:30443/         → 401 without password, 200 with password (Jaeger UI)
GET https://registry.172-25-0-2.sslip.io:30443/       → 401 without password, 200 with password (agent registry)
```

Reading the results: HTTP is redirected to HTTPS; the certificate is valid; every UI returns `401` without a password and is reachable with one; sending 40 concurrent requests with the password to the agent UI lets 11 requests through (`200`), and 29 are blocked by the rate limit (`429`).

Local limitations:

- The redirect points to port 443 instead of NodePort 30443.
- The node IP changes every time the cluster is recreated; `setup_ingress.sh` must then be rerun.

## Let's Encrypt with DuckDNS (optional, off by default)

```bash
scripts/setup/setup_letsencrypt.sh   # DUCKDNS_DOMAIN not set: prints instructions and exits 0, keeps the internal CA
```

Domain validation here is DNS-01 (proving domain ownership with a DNS record), using the community webhook [ebrianne/cert-manager-webhook-duckdns](https://github.com/ebrianne/cert-manager-webhook-duckdns).
The script takes a **local chart directory** through the `DUCKDNS_WEBHOOK_CHART` variable, and checks that the chart is the pinned version **1.0.0** before installing.

**Not verified:** whether this pin check really exists, and whether the `groupName`, `apiTokenSecretRef` schema
matches the upstream chart; check before enabling. This chart has not been downloaded and checked in the results attached to the repo.
Do not treat this optional path as evidence that a Let's Encrypt cert has been issued.

Before running, the operator needs:

- a verified chart;
- cert-manager running;
- Vault `secret/platform/duckdns` with a `token` field.

The token is only read through `vault_env.sh duckdns` and loaded into the Secret `cert-manager/duckdns-token` via
stdin; it is never written to the repo. Use the bare name `<name>.duckdns.org`, without an `api.` prefix.

```bash
export DUCKDNS_DOMAIN=your-name.duckdns.org
export LE_EMAIL=you@example.com
export DUCKDNS_WEBHOOK_CHART=/path/to/verified-chart
## VAULT_ADDR/VAULT_TOKEN come from the environment; do not copy secrets into commands or docs.
LE_ENV=staging scripts/setup/setup_letsencrypt.sh
kubectl --context kind-platform -n feature-store get certificate
## Only once staging is Ready, request production:
LE_ENV=production scripts/setup/setup_letsencrypt.sh
```

Staging uses ACME staging (the cert is not trusted by browsers); production uses ACME production.
The two environments use different accounts and TLS Secrets.

The script creates a separate Ingress `feature-api-duckdns` to the Feature API, and keeps the sslip.io route using the internal CA unchanged.
For clients to reach it, you need a DNS A record pointing to an address clients can reach, and a path to
NodePort 30443 (or NAT forwarding port 443 to 30443 if you want an HTTPS URL without a port number).
DNS-01 only proves domain ownership. The script does not set up NAT and does not update DuckDNS's A record.

</details>

[Section index](README.md) · [Main index](../README.md)
