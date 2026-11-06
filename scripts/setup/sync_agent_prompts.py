"""Đồng bộ system prompt Agent từ Langfuse (label `production`) sang ConfigMap `kagent/agent-prompts`.

Agent đọc prompt qua `systemMessageFrom` ⇒ đổi prompt = sửa trên Langfuse rồi chạy script này (job Jenkins cũng gọi nó).
`--seed`: tạo prompt `platform-<agent>` từ infra/k8s/agents/prompts/<agent>.txt nếu Langfuse chưa có (lần đầu).
Khoá API đọc từ Secret `kagent/langfuse-api`; Langfuse gọi qua port-forward tạm.

`--push AGENT`: tạo version MỚI từ file .txt (label production) khi nội dung khác bản production hiện tại; rồi đồng bộ.

python scripts/setup/sync_agent_prompts.py [--seed] [--push coordinator]
"""

import argparse
import base64
import json
import os
import subprocess
import time
import urllib.error
import urllib.request
from pathlib import Path

AGENTS = ("data-agent", "model-agent", "coordinator")
SEEDS = Path(__file__).resolve().parents[2] / "infra/k8s/agents/prompts"
KC = ["kubectl", "--context", os.environ.get("KCTX", "kind-platform")]
URL = "http://localhost:13000"


def secret(name, key, namespace="kagent"):
    """Đọc 1 khoá trong Secret k8s."""
    out = subprocess.run(
        [*KC, "-n", namespace, "get", "secret", name, "-o", f"jsonpath={{.data.{key}}}"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    return base64.b64decode(out).decode()


def call(method, path, auth, body=None):
    """Gọi Langfuse public API (basic auth public:secret); trả (status, json|None)."""
    request = urllib.request.Request(
        URL + path,
        method=method,
        data=json.dumps(body).encode() if body is not None else None,
        headers={"Authorization": f"Basic {auth}", "Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return response.status, json.load(response)
    except urllib.error.HTTPError as exc:
        return exc.code, None


def main():
    """Port-forward Langfuse, seed (tuỳ chọn), lấy bản production, ghi ConfigMap."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--seed", action="store_true", help="tạo prompt từ file .txt nếu Langfuse chưa có"
    )
    parser.add_argument(
        "--push", choices=AGENTS, help="đẩy file .txt của agent này thành version mới"
    )
    args = parser.parse_args()
    auth = base64.b64encode(
        f"{secret('langfuse-api', 'LANGFUSE_PUBLIC_KEY')}:{secret('langfuse-api', 'LANGFUSE_SECRET_KEY')}".encode()
    ).decode()
    forward = subprocess.Popen(
        [*KC, "-n", "langfuse", "port-forward", "svc/langfuse", "13000:3000"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        for _ in range(30):
            try:
                urllib.request.urlopen(URL + "/api/public/health", timeout=2)
                break
            except OSError:
                time.sleep(1)
        prompts, versions = {}, {}
        for agent in AGENTS:
            name = f"platform-{agent}"
            status, data = call("GET", f"/api/public/v2/prompts/{name}?label=production", auth)
            if status == 404 and args.seed:
                text = (SEEDS / f"{agent}.txt").read_text()
                status, data = call(
                    "POST",
                    "/api/public/v2/prompts",
                    auth,
                    {"name": name, "type": "text", "prompt": text, "labels": ["production"]},
                )
                print(f"seeded {name}: HTTP {status}")
            if args.push == agent and data:
                text = (SEEDS / f"{agent}.txt").read_text()
                if text != data["prompt"]:
                    status, data = call(
                        "POST",
                        "/api/public/v2/prompts",
                        auth,
                        {"name": name, "type": "text", "prompt": text, "labels": ["production"]},
                    )
                    print(f"pushed {name}: HTTP {status}")
            if not data:
                raise SystemExit(f"không lấy được prompt {name} (HTTP {status})")
            prompts[agent], versions[agent] = data["prompt"], str(data["version"])
            print(f"{name}: version {data['version']}, labels {data.get('labels')}")
    finally:
        forward.terminate()
    manifest = subprocess.run(
        [
            *KC,
            "-n",
            "kagent",
            "create",
            "configmap",
            "agent-prompts",
            "--dry-run=client",
            "-o",
            "json",
            *[f"--from-literal={a}={p}" for a, p in prompts.items()],
        ],
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    doc = json.loads(manifest)
    doc["metadata"]["annotations"] = {
        f"langfuse.platform/{a}-version": v for a, v in versions.items()
    }
    subprocess.run([*KC, "apply", "-f", "-"], input=json.dumps(doc), text=True, check=True)


if __name__ == "__main__":
    main()
