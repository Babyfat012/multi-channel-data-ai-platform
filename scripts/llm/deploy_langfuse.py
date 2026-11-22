"""Deploy Langfuse v2 (ns langfuse) on the pgvector database `langfuse`, then seed + sync the agent prompts.

API keys, auth secrets and the admin password are generated once and kept in Secret langfuse/langfuse-env; the same
API keys go to kagent/langfuse-api for the prompt sync script and Jenkins.

    python3 -m scripts.llm.deploy_langfuse          (KCTX, default kind-platform)
"""

import os
import secrets
import sys

from scripts.common import ROOT, apply, kubectl, run, secret_manifest, secret_value
from scripts.llm.deploy_agentregistry import pgvector_database_url

GENERATED = {
    "LANGFUSE_INIT_PROJECT_PUBLIC_KEY": lambda: f"pk-lf-{secrets.token_hex(12)}",
    "LANGFUSE_INIT_PROJECT_SECRET_KEY": lambda: f"sk-lf-{secrets.token_hex(16)}",
    "NEXTAUTH_SECRET": lambda: secrets.token_hex(32),
    "SALT": lambda: secrets.token_hex(16),
    "LANGFUSE_INIT_USER_PASSWORD": lambda: secrets.token_hex(12),
}


def main() -> None:
    """DB, app, secrets (kept across reruns), restart, then prompts."""
    os.chdir(ROOT)
    kc = kubectl()
    url = f"postgresql://{pgvector_database_url(kc, 'langfuse')}"
    run([*kc, "apply", "-f", "infra/k8s/agents/langfuse.yaml"])
    values = {
        key: secret_value(kc, "langfuse", "langfuse-env", key) or make()
        for key, make in GENERATED.items()
    }
    apply(
        kc,
        secret_manifest(
            "langfuse",
            "langfuse-env",
            {"DATABASE_URL": url, "LANGFUSE_INIT_USER_EMAIL": "admin@platform.local", **values},
        ),
    )
    apply(
        kc,
        secret_manifest(
            "kagent",
            "langfuse-api",
            {
                "LANGFUSE_PUBLIC_KEY": values["LANGFUSE_INIT_PROJECT_PUBLIC_KEY"],
                "LANGFUSE_SECRET_KEY": values["LANGFUSE_INIT_PROJECT_SECRET_KEY"],
            },
        ),
    )
    run([*kc, "-n", "langfuse", "rollout", "restart", "deploy/langfuse"], capture=True)
    run([*kc, "-n", "langfuse", "rollout", "status", "deploy/langfuse", "--timeout=600s"])
    run([sys.executable, "scripts/llm/sync_agent_prompts.py", "--seed"])


if __name__ == "__main__":
    main()
