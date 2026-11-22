"""Helpers shared by the Python scripts: run commands, read .env, apply Kubernetes objects, talk to Vault.

Stdlib only: some scripts run inside the Jenkins container, which has a bare python3.
Run any script from the repo root as a module, e.g. `python3 -m scripts.shared.push_images`.
"""

import json
import os
import subprocess
import sys
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def run(cmd: list[str], stdin: str | None = None, capture: bool = False, check: bool = True) -> str:
    """Run a command without a shell; stop the script on failure (like `set -e`).

    capture=True hides the output and returns stdout (also the way to silence a command).
    """
    result = subprocess.run(
        cmd,
        input=stdin,
        text=True,
        stdout=subprocess.PIPE if capture else None,
        stderr=subprocess.PIPE if capture else None,
    )
    if check and result.returncode != 0:
        detail = f":\n{result.stderr.strip()}" if capture and result.stderr else ""
        sys.exit(f"command failed ({result.returncode}): {' '.join(cmd[:4])}…{detail}")
    return (result.stdout or "").strip()


def succeeds(cmd: list[str]) -> bool:
    """True when the command exits 0; its output is discarded (bash `cmd >/dev/null 2>&1`)."""
    return subprocess.run(cmd, capture_output=True).returncode == 0


def read_env_file(path: Path = ROOT / ".env") -> dict[str, str]:
    """Parse KEY=VALUE lines of a .env file (comments, `export ` and surrounding quotes allowed); {} if missing."""
    values = {}
    if not path.exists():
        return values
    for line in path.read_text().splitlines():
        line = line.strip().removeprefix("export ")
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "'\"":
            value = value[1:-1]
        values[key.strip()] = value
    return values


def load_env(path: Path = ROOT / ".env", required: bool = True) -> None:
    """Load .env into os.environ, overriding existing values (same as bash `set -a; . ./.env`)."""
    if required and not path.exists():
        sys.exit(f"{path} not found")
    os.environ.update(read_env_file(path))


def env_defaults(keys: list[str], path: Path = ROOT / ".env") -> None:
    """Take only these keys from .env, and only when the environment does not set them (environment wins)."""
    values = read_env_file(path)
    for key in keys:
        if key not in os.environ:
            os.environ[key] = values.get(key, "")


def env(key: str, default: str | None = None) -> str:
    """Read an env var; stop with a clear message when it is required and empty."""
    value = os.environ.get(key) or default
    if not value:
        sys.exit(f"{key} is required")
    return value


def tee(cmd: list[str], log: Path, stdin: str | None = None) -> None:
    """Run a command, print its output live and also save it to a log file (bash `cmd 2>&1 | tee log`)."""
    log.parent.mkdir(parents=True, exist_ok=True)
    with log.open("w") as out:
        proc = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE if stdin is not None else None,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        if stdin is not None:
            proc.stdin.write(stdin)
            proc.stdin.close()
        for line in proc.stdout:
            sys.stdout.write(line)
            out.write(line)
    if proc.wait() != 0:
        sys.exit(f"command failed ({proc.returncode}): {' '.join(cmd[:4])}… (log: {log})")


def kubectl(context: str | None = None) -> list[str]:
    """Base kubectl command for the selected context (default KCTX or kind-platform)."""
    return ["kubectl", "--context", context or os.environ.get("KCTX", "kind-platform")]


def apply(kc: list[str], manifest: str | dict) -> None:
    """kubectl apply through stdin: secrets never show up in process arguments."""
    text = manifest if isinstance(manifest, str) else json.dumps(manifest)
    run([*kc, "apply", "-f", "-"], stdin=text, capture=True)


def vault_addr() -> str:
    """Vault address: VAULT_ADDR, else localhost on VAULT_PORT."""
    return (
        os.environ.get("VAULT_ADDR") or f"http://localhost:{os.environ.get('VAULT_PORT', '8200')}"
    )


def vault_root_token() -> str:
    """Root token written by setup_vault on the first init (local use only, out/ is gitignored)."""
    return json.loads((ROOT / "out/vault/init.json").read_text())["root_token"]


def vault(method: str, path: str, token: str, body: dict | None = None) -> dict | None:
    """Call the Vault HTTP API; returns the JSON body, or None when the path does not exist (404)."""
    request = urllib.request.Request(
        f"{vault_addr()}/v1/{path}",
        data=json.dumps(body).encode() if body is not None else None,
        headers={"X-Vault-Token": token},
        method=method,
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            raw = response.read()
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return None
        raise
    return json.loads(raw) if raw else {}


def vault_secret(name: str, token: str) -> dict | None:
    """Read secret/platform/<name> (KV v2); None when it does not exist yet."""
    found = vault("GET", f"secret/data/platform/{name}", token)
    return found["data"]["data"] if found else None


def vault_put_secret(name: str, data: dict, token: str) -> None:
    """Write secret/platform/<name> (KV v2)."""
    vault("POST", f"secret/data/platform/{name}", token, {"data": data})
