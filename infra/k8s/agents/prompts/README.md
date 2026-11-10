<!-- Purpose: seed copies of the Agent system prompts, used only for the initial Langfuse load. -->
# Agent system prompts

The source of truth is **Langfuse** (prompt `platform-<agent>`, label `production`). The `.txt` files here are only for the initial load
(`scripts/setup/deploy_kind_langfuse.sh`, which creates a prompt only when Langfuse does not have it yet). Edit prompts in Langfuse, then run
`scripts/setup/sync_agent_prompts.py` (or the Jenkins job) to push the `production` version into ConfigMap `agent-prompts`, which
Agents read via `systemMessageFrom`. See [`docs/ml-llm-platform/reference/llm_platform_agents.md`](../../../../docs/ml-llm-platform/reference/llm_platform_agents.md).
