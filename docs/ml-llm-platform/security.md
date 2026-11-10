# Security và Vault trên AWS

EDAI-2, dòng 54: quản lý secret tập trung với Vault.

Code và cấu hình: [Vault setup](../../scripts/setup/setup_vault.sh); secret được dùng cho CI và ingress.

Các kết quả dưới đây là của những lần đã chạy trên AWS. Chúng không có nghĩa là dịch vụ đang bật lúc bạn đọc.

## Trên AWS

Mọi secret dùng chung (mật khẩu, khoá) chỉ nằm ở một nơi: kho KV v2 `secret/platform/*` trong Vault. Code và repo không chứa giá trị secret.

Cách Vault chạy trên AWS:

- Vault 2.1.1 chạy trên EC2 bằng Docker Compose.
- Dữ liệu lưu trên ổ EBS tại `/mnt/platform-data/vault`.
- Vault chỉ nghe trên loopback (cổng 8200, chỉ truy cập từ chính máy đó); người vận hành vào máy qua SSM.
- Vault đã được khởi tạo lại sau sự cố phải thay EC2 ([`setup_vault.sh`](../../scripts/setup/setup_vault.sh)).

| Secret | Ai đọc |
|---|---|
| `agent-ui`, `ops-ui` | [`setup_ingress.sh`](../../scripts/setup/setup_ingress.sh) sinh một lần, rồi đọc để tạo file htpasswd cho basic auth của NGINX |
| `postgres` | [`vault_env.sh`](../../scripts/setup/vault_env.sh), dùng cho job và CI (Flink, Jenkins); không ghi vào repo |
| `minio` | chỉ dùng ở bản local (trên AWS dùng IAM role, không có MinIO) |

Ảnh dưới chỉ in trạng thái, tên secret và thông tin phiên bản, **không in giá trị** secret. Ảnh cho thấy:

- `Sealed false`: Vault đã mở khoá và sẵn sàng.
- Có 4 key dưới `secret/platform`.
- `agent-ui` ở version 1, được tạo đúng lúc `setup_ingress.sh` chạy lần đầu sau khi khởi tạo lại Vault.

![Vault trên EC2: status unsealed 2.1.1, kv list secret/platform (agent-ui, minio, ops-ui, postgres), metadata agent-ui version 1](../evidence/aws/vault-status-kv-20261031.png)

<details>
<summary>Tham khảo: thiết kế, hướng dẫn phát triển và ghi nhận trước triển khai AWS</summary>

Các ghi nhận local giữ nguyên môi trường, không dùng thay bằng chứng cloud.

<!-- Mục đích: quản lý secret tập trung bằng HashiCorp Vault -->
## Secret tập trung — HashiCorp Vault

Secret nằm ở **một chỗ** (Vault) để cả tổ chức dùng chung, không rải trong code.

## Thiết kế

Luồng secret: `setup_vault.sh` nạp secret từ `.env` và Secret k8s của agentgateway vào Vault. Jenkins chỉ giữ một token chỉ-đọc, và các script đọc secret qua `vault_env.sh`.

```
.env + Secret k8s agentgateway ──setup_vault.sh──► Vault KV v2  secret/platform/{minio,postgres,agentgateway}
                                      (setup_jenkins.sh / setup_ingress.sh thêm jenkins, agent-ui)
                                                    │  policy platform-read (chỉ đọc)
                                                    ▼
                               token CI chỉ-đọc ──► Jenkins (credential duy nhất: vault-ci-token)
                                                    ▼
                         scripts/setup/vault_env.sh ──► run_sku_return_feature.sh, feast-pusher, …
```

| Thành phần | File |
|---|---|
| Service Vault 2.1.1, chế độ server, lưu file trong volume `vault-data` | `vault` trong [`docker-compose.yml`](../../docker-compose.yml) (profile `security`) |
| Init 1 lần + unseal + bật KV v2 + nạp secret + policy + token CI | [`scripts/setup/setup_vault.sh`](../../scripts/setup/setup_vault.sh) |
| Đọc secret thành biến môi trường | [`scripts/setup/vault_env.sh`](../../scripts/setup/vault_env.sh) |
| Nơi dùng secret: submit job Flink, Jenkinsfile stream | [`run_sku_return_feature.sh`](../../scripts/run/run_sku_return_feature.sh), [`stream-features.Jenkinsfile`](../../ci/jenkins/stream-features.Jenkinsfile) |

## Quyết định

- **Chạy server mode, không dùng dev mode.** Dev mode giữ secret trong RAM, khởi động lại là mất. Server mode lưu ra file.
  Đổi lại, mỗi lần khởi động lại, Vault ở trạng thái **sealed** (bị khoá), và `setup_vault.sh` phải unseal (mở khoá) lại.
- **Khoá unseal và root token** nằm ở `out/vault/init.json` (có trong gitignore, quyền file 600). Cách này chỉ chấp nhận được ở
  local. Trên GKE: tự unseal bằng Cloud KMS, và thu hồi root token sau khi setup.
- **Quyền tối thiểu (least privilege):** CI dùng token có policy `platform-read`, chỉ được đọc `secret/data/platform/*`; token có hạn gia hạn (period) 768 giờ.
- Thứ tự script tìm credential: biến môi trường trước, rồi Vault (nếu có `VAULT_TOKEN`), cuối cùng là file `.env`.

## Sau khi máy restart

`scripts/setup/up_ml_base.sh` dựng lại phần nền dùng chung bằng một lệnh: MinIO + PostgreSQL + Lakekeeper,
unseal Vault, Jenkins, và nạp lại (materialize) Redis nếu Redis trống. Lý do: Redis trong kind không lưu ra đĩa; trên EKS thì Redis có PVC và AOF, xem [Feature Store](feature_store.md#kiểm-chứng-trên-eks).
Script này không bật LLM hay agent.

Khi dừng service Compose, **luôn ghi rõ tên service**. Lệnh `docker compose --profile X stop` không kèm
tên sẽ dừng luôn cả MinIO/PostgreSQL, là những thứ Feast, MLflow và Vault phụ thuộc vào.

## Bằng chứng

```text
eval "$(scripts/setup/vault_env.sh postgres)"  → POSTGRES_USER=platform POSTGRES_DB=sources (password 18 ký tự)
POST secret/data/platform/postgres bằng token CI     → HTTP 403 (chỉ đọc)
vault_env.sh agentgateway                        → AGW_AGENT_KEY 36 ký tự, khớp Secret k8s
restart container vault                          → sealed=true; setup_vault.sh → sealed=false
```

Đọc kết quả:

1. `vault_env.sh` đọc được secret postgres thành biến môi trường (mật khẩu dài 18 ký tự, không in ra).
2. Token CI ghi vào Vault bị từ chối `403`, đúng với quyền chỉ-đọc.
3. Khoá gateway đọc từ Vault khớp với Secret trong k8s.
4. Sau khi khởi động lại container, Vault bị khoá (`sealed=true`); chạy `setup_vault.sh` thì mở lại (`sealed=false`).

</details>

[Mục lục phần này](README.md) · [Mục lục chung](../README.md)
