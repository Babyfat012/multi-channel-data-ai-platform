"""Release local images to a registry: kind (localhost:5001), ECR or Artifact Registry.

    REGISTRY=localhost:5001 TAG=<sha> python3 -m scripts.shared.push_images platform-feature-api:<source-tag> [...]

With TAG: release each source image under that TAG; without TAG: keep the immutable source tag.
ECR registries are logged into first with the available AWS credentials (EC2 IAM role for Jenkins, SSO on a laptop).
"""

import os
import re
import sys

from scripts.common import run


def ecr_login(registry: str) -> None:
    """`aws ecr get-login-password | docker login` when the registry is an ECR address; no-op otherwise."""
    match = re.match(r"^([0-9]+\.dkr\.ecr\.([a-z0-9-]+)\.amazonaws\.com)", registry)
    if match:
        password = run(["aws", "ecr", "get-login-password", "--region", match[2]], capture=True)
        run(
            ["docker", "login", "--username", "AWS", "--password-stdin", match[1]],
            stdin=password,
            capture=True,
        )


def target_for(image: str, registry: str, tag: str | None) -> str:
    """platform-x:src + registry + TAG → <registry>/platform-x:<TAG or src>; refuses missing or `latest` tags."""
    name, sep, source_tag = image.rpartition(":")
    if not sep or "/" in source_tag:
        sys.exit(f"Missing tag: {image}")
    tag = tag or source_tag
    if tag == "latest":
        sys.exit("Refusing to release the latest tag")
    return f"{registry.rstrip('/')}/{name.rsplit('/', 1)[-1]}:{tag}"


def main() -> None:
    """Tag each image for the registry and push it."""
    images = sys.argv[1:]
    if not images:
        sys.exit("At least one image:tag is required")
    registry = os.environ.get("REGISTRY", "localhost:5001")
    tag = os.environ.get("TAG")
    targets = [
        target_for(image, registry, tag) for image in images
    ]  # validate everything before pushing
    ecr_login(registry)
    for image, target in zip(images, targets, strict=True):
        run(["docker", "image", "inspect", image], capture=True)
        run(["docker", "tag", image, target])
        run(["docker", "push", target])


if __name__ == "__main__":
    main()
