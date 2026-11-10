# External Routing và Gateway trên EKS

EDAI-2, dòng 37–43: ingress, UI, auth/rate limit, domain và HTTPS.

Code và cấu hình: file cấu hình và ảnh của từng route được dẫn trong phần kết quả EKS bên dưới.

Các kết quả dưới đây là của những lần đã chạy trên AWS. Chúng không có nghĩa là dịch vụ đang bật lúc bạn đọc.

## Trên EKS (2026-10-30/31)

Mọi request từ bên ngoài vào cụm đều đi qua một cổng vào (ingress) duy nhất:

- **Cổng vào:** F5 NGINX Ingress Controller (namespace `ingress`), đứng sau một NLB (load balancer mạng) của AWS.
- **HTTPS:** cert-manager xin chứng chỉ Let's Encrypt thật (xác minh kiểu HTTP-01) cho mỗi host dạng `<tên>.15-134-182-157.sslip.io`.
- **Route:** mọi UI đều nằm sau cổng này. Route được tạo bởi [`setup_ingress.sh`](../../scripts/setup/setup_ingress.sh).
- **Mật khẩu:** sinh một lần và cất trong Vault.

Bảng route (cột "Host" là phần đầu tên miền):

| Host | Service phía sau | Setting NGINX | Auth |
|---|---|---|---|
| `api.` | Feature API (Web API) | [`feature-api.yaml`](../../infra/k8s/ingress/feature-api.yaml): HTTPS, `limit-req-rate` 50r/s, burst 100 | không (API công khai, có rate limit) |
| `agent.` | kagent UI (UI test agent) | [`agent-ui.yaml`](../../infra/k8s/ingress/agent-ui.yaml): HTTPS, basic auth, 5r/s burst 10, vượt mức thì trả 429 | `seller` |
| `grafana.` | Grafana (metric + log Loki) | [`ops-ui.yaml`](../../infra/k8s/ingress/ops-ui.yaml) | `ops` + admin Grafana |
| `traces.` | Jaeger UI | `ops-ui.yaml` | `ops` |
| `metrics.` | Prometheus UI | `ops-ui.yaml` | `ops` |
| `registry.` | agentregistry UI | `ops-ui.yaml` | `ops` |

**Kiểm tra basic auth và rate limit của UI agent.** Gửi request từ laptop, không kèm mật khẩu:

- Gửi một request: nhận `401` kèm header `WWW-Authenticate: Basic realm="PLATFORM agent UI"`, tức bị yêu cầu đăng nhập.
- Gửi 40 request cùng lúc: 12 request nhận `401` (còn trong hạn mức nên đi tới bước kiểm tra mật khẩu), 28 request nhận `429` (vượt mức 5 request/giây cộng burst 10, nên NGINX chặn ngay, trước cả bước kiểm tra mật khẩu).

![agent. không mật khẩu: 401 Basic realm PLATFORM agent UI; 40 request song song: 12 × 401, 28 × 429](../evidence/aws/agent-ui-basic-auth-rate-limit-20261031.png)

Các UI sau gateway (đã đăng nhập qua basic auth, HTTPS):

![Grafana qua https://grafana.…sslip.io: thư mục dashboard PLATFORM](../evidence/aws/grafana-dashboards-https-20261030.png)

![Jaeger qua https://traces.…sslip.io: tìm trace agentgateway](../evidence/aws/jaeger-search-https-20261030.png)

![kagent UI qua https://agent.…sslip.io: 3 agent coordinator, data-agent, model-agent](../evidence/aws/kagent-ui-agents-20261030.png)

![agentregistry UI qua https://registry.…sslip.io: 1 MCP server và 3 agent đã publish](../evidence/aws/agent-registry-ui-20261030.png)

Log xem trong Grafana (Explore → Loki): [Observability](observability.md#trên-eks).
Web API HTTPS: [Swagger qua `api.`](web_api_mcp.md#trên-eks).

<details>
<summary>Tham khảo: thiết kế, hướng dẫn phát triển và ghi nhận trước triển khai AWS</summary>

Các ghi nhận local giữ nguyên môi trường, không dùng thay bằng chứng cloud.

<!-- Mục đích: NGINX Ingress cho các service LLM Platform — HTTPS, basic auth, rate limit -->
## Gateway — NGINX Ingress, HTTPS, auth

Mục tiêu:

- Các service (metric, log, trace, UI test agent, UI registry) đều đứng sau gateway.
- UI test agent có basic auth và rate limit.
- Web API kéo dữ liệu có tên miền và HTTPS.

## Chọn controller

Dự án cộng đồng **ingress-nginx** đã ngừng bảo trì: bản chart cuối là 4.15.1 (03/2026), không còn bản
vá bảo mật. Vì vậy dự án dùng **F5 NGINX Ingress Controller** (mã nguồn mở, chart `nginx-ingress` 2.7.3 /
app 5.6.3). Đây vẫn là "NGINX Ingress Controller" như thiết kế yêu cầu; annotation có dạng `nginx.org/*`.

```bash
helm --kube-context kind-platform install nginx-ingress oci://ghcr.io/nginx/charts/nginx-ingress \
  --version 2.7.3 -n ingress --create-namespace --set controller.service.type=NodePort \
  --set controller.service.httpPort.nodePort=30080 --set controller.service.httpsPort.nodePort=30443 \
  --set controller.ingressClass.name=nginx --set controller.ingressClass.setAsDefaultIngress=true
scripts/setup/setup_ingress.sh
```

| Route | Host (local) | Bảo vệ | File |
|---|---|---|---|
| Feature API (Web API kéo dữ liệu) | `api.<ip-node>.sslip.io` | HTTPS, redirect HTTP→HTTPS, 50 r/s | [`feature-api.yaml`](../../infra/k8s/ingress/feature-api.yaml) |
| kagent UI (chat test agent) | `agent.<ip-node>.sslip.io` | HTTPS + **basic auth** + **5 r/s, burst 10, 429** | [`agent-ui.yaml`](../../infra/k8s/ingress/agent-ui.yaml) |
| Prometheus (metric) | `metrics.<ip-node>.sslip.io` | HTTPS + basic auth `ops` | [`ops-ui.yaml`](../../infra/k8s/ingress/ops-ui.yaml) |
| Grafana (metric + log qua Loki), Jaeger (trace) | `grafana.` / `traces.` | HTTPS + basic auth `ops` | cùng template |
| Agent registry UI (agentregistry 0.4.0, không có auth riêng) | `registry.` | HTTPS + basic auth `ops` | cùng template |

- **Domain**: sslip.io là dịch vụ DNS tự đổi tên thành IP, ví dụ `api.172-25-0-2.sslip.io` trỏ tới `172.25.0.2` (IP node kind).
  Nhờ vậy có tên miền thật mà không phải mua. Trên GKE thì dùng domain riêng, trỏ tới IP tĩnh của load balancer.
- **HTTPS**: cert-manager cấp chứng chỉ từ CA nội bộ [`issuer.yaml`](../../infra/k8s/ingress/issuer.yaml)
  (tên `platform-local-ca`; chứng chỉ CA được xuất ra `out/ingress/platform-local-ca.crt` để client cài và tin). Trên GKE thì dùng ACME/Let's Encrypt.
- **Mật khẩu UI** được sinh ngẫu nhiên và cất ở Vault: `secret/platform/agent-ui` (UI agent) và `secret/platform/ops-ui` (UI vận hành).
  `setup_ingress.sh` tạo secret htpasswd trong k8s từ các mật khẩu này; mật khẩu không nằm trong repo.

## Bằng chứng (từ máy host)

```text
GET http://api.172-25-0-2.sslip.io:30080/            → 301 → https://…
GET https://api.172-25-0-2.sslip.io:30443/healthz    → 200, ssl_verify_result = 0 (cert hợp lệ với CA)
GET https://agent.172-25-0-2.sslip.io:30443/          → 401 không mật khẩu, 200 có mật khẩu
40 request đồng thời có mật khẩu                     → 11 × 200, 29 × 429 (rate limit)
GET https://metrics.172-25-0-2.sslip.io:30443/        → 401; có mật khẩu ops: /api/v1/query?query=up → 12 target
GET https://grafana.172-25-0-2.sslip.io:30443/        → 401 không mật khẩu, 200 có mật khẩu (log Loki xem qua Grafana Explore)
GET https://traces.172-25-0-2.sslip.io:30443/         → 401 không mật khẩu, 200 có mật khẩu (Jaeger UI)
GET https://registry.172-25-0-2.sslip.io:30443/       → 401 không mật khẩu, 200 có mật khẩu (agent registry)
```

Đọc kết quả: HTTP được chuyển hướng sang HTTPS; chứng chỉ hợp lệ; mọi UI trả `401` khi thiếu mật khẩu và truy cập được khi có mật khẩu; gửi cùng lúc 40 request có mật khẩu vào UI agent thì 11 request qua (`200`), 29 request bị rate limit chặn (`429`).

Giới hạn ở local:

- Lệnh chuyển hướng trả về cổng 443 thay vì NodePort 30443.
- IP node đổi mỗi khi tạo lại cluster; khi đó phải chạy lại `setup_ingress.sh`.

## Let's Encrypt với DuckDNS (tùy chọn, mặc định tắt)

```bash
scripts/setup/setup_letsencrypt.sh   # chưa đặt DUCKDNS_DOMAIN: hướng dẫn rồi exit 0, giữ CA nội bộ
```

Cách xác minh tên miền ở đây là DNS-01 (chứng minh quyền sở hữu domain bằng bản ghi DNS), dùng webhook cộng đồng [ebrianne/cert-manager-webhook-duckdns](https://github.com/ebrianne/cert-manager-webhook-duckdns).
Script nhận **thư mục chart local** qua biến `DUCKDNS_WEBHOOK_CHART`, và kiểm chart có đúng phiên bản ghim (pin) **1.0.0** trước khi cài.

**Chưa kiểm chứng:** bước kiểm pin này có thực sự tồn tại không, và schema `groupName`, `apiTokenSecretRef`
có trùng với chart gốc không; cần kiểm trước khi bật. Chart này chưa được tải về và kiểm trong các kết quả đính kèm repo.
Không coi nhánh tùy chọn này là bằng chứng cert Let's Encrypt đã được cấp.

Trước khi chạy, người vận hành cần chuẩn bị:

- chart đã kiểm;
- cert-manager đang chạy;
- Vault `secret/platform/duckdns` có trường `token`.

Token chỉ được đọc qua `vault_env.sh duckdns` và nạp vào Secret `cert-manager/duckdns-token` qua
stdin; không ghi vào repo. Dùng tên gốc `<tên>.duckdns.org`, không thêm `api.` phía trước.

```bash
export DUCKDNS_DOMAIN=ten-cua-ban.duckdns.org
export LE_EMAIL=ban@example.com
export DUCKDNS_WEBHOOK_CHART=/duong-dan/chart-da-kiem
## VAULT_ADDR/VAULT_TOKEN lấy từ môi trường, không chép secret vào lệnh hay docs.
LE_ENV=staging scripts/setup/setup_letsencrypt.sh
kubectl --context kind-platform -n feature-store get certificate
## Khi staging đã Ready, mới yêu cầu production:
LE_ENV=production scripts/setup/setup_letsencrypt.sh
```

Staging dùng ACME staging (cert chưa được trình duyệt tin); production dùng ACME production.
Hai môi trường dùng account và TLS Secret khác nhau.

Script tạo một Ingress riêng `feature-api-duckdns` tới Feature API, và giữ nguyên route sslip.io dùng CA nội bộ.
Để client vào được, cần bản ghi DNS A trỏ tới địa chỉ client truy cập được, và một đường vào
NodePort 30443 (hoặc NAT chuyển cổng 443 sang 30443 nếu muốn URL HTTPS không ghi số cổng).
DNS-01 chỉ chứng minh quyền sở hữu domain. Script không tự dựng NAT và không cập nhật bản ghi A của DuckDNS.

</details>

[Mục lục phần này](README.md) · [Mục lục chung](../README.md)
