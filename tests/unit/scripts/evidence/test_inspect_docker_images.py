"""Check the Docker inventory (no Docker needed) and that the naive image installs exactly the locked library set."""

import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
spec = importlib.util.spec_from_file_location(
    "scripts.evidence.inspect_docker_images", ROOT / "scripts/evidence/inspect_docker_images.py"
)
inventory = importlib.util.module_from_spec(spec)
spec.loader.exec_module(inventory)


def test_services_keep_profiles_ports_and_limits():
    """One row per service, ports keep host_ip; a missing host_ip means listening on every interface."""
    config = {
        "services": {
            "spark-master": {
                "image": "platform-spark",
                "build": {"dockerfile": "docker/Dockerfile.airflow"},
                "profiles": ["spark"],
                "ports": [{"host_ip": "127.0.0.1", "published": "8080", "target": 8080}],
                "mem_limit": "2g",
                "cpus": 1.5,
            },
            "postgres": {"image": "postgres:15", "ports": [{"published": "5432", "target": 5432}]},
        }
    }
    rows = inventory.services(config)
    assert [r["service"] for r in rows] == ["postgres", "spark-master"]
    assert rows[0]["ports"][0]["host_ip"] == "0.0.0.0" and rows[0]["profiles"] == []
    assert rows[1]["dockerfile"] == "docker/Dockerfile.airflow" and rows[1]["mem_limit"] == "2g"


def test_naive_dockerfile_installs_locked_dependencies_only():
    """The naive image installs libraries from requirements.lock, the project with --no-deps ⇒ same library set as uv.lock."""
    text = (ROOT / "docker/Dockerfile.naive").read_text()
    assert "pip install -r requirements.lock" in text and "--no-deps ." in text
    lock = (ROOT / "requirements.lock").read_text()
    assert "uv export --frozen --no-dev" in lock and "--hash=sha256:" in lock


def test_dockerignore_excludes_secrets_and_agent_notes():
    """The build context does not carry .env, agent notes or generated data into the naive image (COPY . .)."""
    ignored = set((ROOT / ".dockerignore").read_text().split())
    assert {".env", ".git", "out", ".briefs", ".vibe-wise"} <= ignored
