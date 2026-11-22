"""Add this machine's public IP to the EKS API allow list (publicAccessCidrs), then wait until the cluster is ACTIVE.

Use when `kubectl` says "dial tcp …:443: i/o timeout" because the ISP changed your IP. Rerunnable: no-op when the IP
is already allowed; the current list is read from AWS. This drifts from Terraform (terraform.tfvars).

    python3 -m scripts.shared.eks_allow_my_ip          preview only: DRY_RUN=1 python3 -m scripts.shared.eks_allow_my_ip
"""

import os
import re
import sys
import urllib.request

from scripts.common import run


def main() -> None:
    """Read the allow list, append <ip>/32 if missing, and apply it."""
    cluster = os.environ.get("EKS_CLUSTER", "platform-apps")
    region = os.environ.get("AWS_REGION", "ap-southeast-2")
    with urllib.request.urlopen("https://checkip.amazonaws.com", timeout=10) as response:
        my_ip = response.read().decode().strip()
    if not re.fullmatch(r"\d+\.\d+\.\d+\.\d+", my_ip):
        sys.exit(f"Could not get the public IP: {my_ip!r}")
    aws = ["aws", "eks", "--region", region]
    run([*aws, "wait", "cluster-active", "--name", cluster])
    current = run(
        [
            *aws,
            "describe-cluster",
            "--name",
            cluster,
            "--output",
            "text",
            "--query",
            "cluster.resourcesVpcConfig.publicAccessCidrs",
        ],
        capture=True,
    ).split()
    print(f"Current IP: {my_ip}")
    if f"{my_ip}/32" in current:
        print("IP is already in the allow list; no change needed.")
        return
    cidrs = ",".join([*current, f"{my_ip}/32"])
    print(f"New list: {cidrs}")
    if os.environ.get("DRY_RUN"):
        print("(DRY_RUN: not sent to AWS)")
        return
    run(
        [
            *aws,
            "update-cluster-config",
            "--name",
            cluster,
            "--resources-vpc-config",
            f"publicAccessCidrs={cidrs}",
            "--query",
            "update.status",
            "--output",
            "text",
        ]
    )
    run([*aws, "wait", "cluster-active", "--name", cluster])
    print(f"Done. Try: kubectl --context {cluster} get nodes")


if __name__ == "__main__":
    main()
