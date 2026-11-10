"""List the images/services/profiles actually shipped (local and the GCP overlay) from `docker compose config`.

Per service: image, build Dockerfile, profiles, ports (with host_ip), RAM/CPU limits; for images present
on the machine, also digest, size and build date via `docker image inspect`. No pull, no network.

    .venv/bin/python scripts/evidence/inspect_docker_images.py --output docs/evidence/docker-inventory-local.json
    RAW_BUCKET=x LAKEHOUSE_BUCKET=y PLATFORM_SPARK_PACKAGES_GCP=z .venv/bin/python scripts/evidence/inspect_docker_images.py \\
        -f docker-compose.yml -f docker-compose.gcp.yml --profile catalog --profile spark --profile airflow \\
        --output docs/evidence/docker-inventory-gcp.json
"""

from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path


def services(config: dict) -> list[dict]:
    """Reduce the `docker compose config` JSON to one row per service, sorted by name."""
    rows = []
    for name, service in sorted(config.get("services", {}).items()):
        build = service.get("build") or {}
        rows.append(
            {
                "service": name,
                "image": service.get("image"),
                "dockerfile": build.get("dockerfile") if build else None,
                "profiles": service.get("profiles", []),
                "ports": [
                    {
                        "host_ip": p.get("host_ip", "0.0.0.0"),
                        "published": p.get("published"),
                        "target": p.get("target"),
                    }
                    for p in service.get("ports", [])
                ],
                "mem_limit": service.get("mem_limit"),
                "cpus": service.get("cpus"),
            }
        )
    return rows


def inspect_image(image: str | None) -> dict:
    """Digest/size of an image already on the machine; if absent, record present=false instead of failing."""
    if not image:
        return {"present": False}
    result = subprocess.run(
        ["docker", "image", "inspect", image, "--format", "{{json .}}"],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        return {"present": False}
    data = json.loads(result.stdout)
    return {
        "present": True,
        "id": data["Id"],
        "size_bytes": data["Size"],
        "created": data["Created"],
    }


def main() -> int:
    """Call compose config with the given files/profiles and write the inventory JSON."""
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "-f", "--file", action="append", default=[], help="Compose file (repeatable)"
    )
    parser.add_argument("--profile", action="append", default=[])
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    command = ["docker", "compose"]
    for path in args.file or ["docker-compose.yml"]:
        command += ["-f", path]
    for profile in args.profile:
        command += ["--profile", profile]
    config = json.loads(
        subprocess.run(
            command + ["config", "--format", "json"], check=True, capture_output=True, text=True
        ).stdout
    )
    rows = services(config)
    for row in rows:
        row["image_inspect"] = inspect_image(row["image"])
    document = {
        "compose_files": args.file or ["docker-compose.yml"],
        "profiles": args.profile,
        "services": rows,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(document, ensure_ascii=False, indent=2) + "\n")
    print(f"{len(rows)} service → {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
