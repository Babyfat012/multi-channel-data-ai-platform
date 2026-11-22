"""Print a Compose override so deployed services pull exactly the TAG released in the Push stage.

TAG=<sha> python3 -m scripts.shared.compose_release_images airflow=platform-airflow > out/ci-compose-images.yaml
"""

import os
import re
import sys

from scripts.common import env


def main() -> None:
    """One `image:` line per service=image argument, all on the same registry and TAG."""
    tag = env("TAG")
    if tag == "latest":
        sys.exit("Refusing the latest tag")
    registry = os.environ.get("REGISTRY", "localhost:5001").rstrip("/")
    lines = ["services:"]
    for mapping in sys.argv[1:]:
        service, _, image = mapping.partition("=")
        if not (re.fullmatch(r"[a-z0-9-]+", service) and re.fullmatch(r"[a-z0-9-]+", image)):
            sys.exit(f"Expected service=image, got {mapping!r}")
        lines += [f"  {service}:", f'    image: "{registry}/{image}:{tag}"']
    print("\n".join(lines))


if __name__ == "__main__":
    main()
