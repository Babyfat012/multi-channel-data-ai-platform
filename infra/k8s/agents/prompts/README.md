<!-- Mục đích: bản gốc system prompt của Agent, chỉ dùng để nạp Langfuse lần đầu. -->
# System prompt của Agent

Nơi quản lý chính là **Langfuse** (prompt `platform-<agent>`, label `production`). Các file `.txt` ở đây chỉ để nạp lần đầu
(`scripts/setup/deploy_kind_langfuse.sh`, chỉ tạo prompt khi Langfuse chưa có). Sửa prompt trên Langfuse rồi chạy
`scripts/setup/sync_agent_prompts.sh` (hoặc job Jenkins) để đẩy bản `production` vào ConfigMap `agent-prompts` mà
Agent đọc qua `systemMessageFrom`. Xem [`docs/llm-platform-agents.md`](../../../../docs/ml-llm-platform/reference/llm_platform_agents.md).
