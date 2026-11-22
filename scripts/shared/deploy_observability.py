"""Deploy the observability stack (ns observability): Prometheus (metrics), Loki + Fluent Bit (logs), Jaeger (traces),
Grafana (views all three, with the PLATFORM dashboard). Safe to rerun.

    python3 -m scripts.shared.deploy_observability          (KCTX, default kind-platform)
"""

import os
import secrets

from scripts.common import ROOT, apply, kubectl, run, succeeds

VALUES = "infra/k8s/observability"
REPOS = {
    "prometheus-community": "https://prometheus-community.github.io/helm-charts",
    "grafana": "https://grafana.github.io/helm-charts",
    "fluent": "https://fluent.github.io/helm-charts",
    "jaegertracing": "https://jaegertracing.github.io/helm-charts",
}


def prometheus_values(kctx: str) -> list[str]:
    """EKS adds the EC2 exporter scrape + alert rules; kind has no EC2, so no false alarms."""
    files = [f"{VALUES}/prometheus-values.yaml"]
    if not kctx.startswith("kind-"):
        files.append(f"{VALUES}/prometheus-values-aws.yaml")
    return [arg for f in files for arg in ("-f", f)]


def main() -> None:
    """Install or upgrade every chart, pinned to a version."""
    os.chdir(ROOT)
    kctx = os.environ.get("KCTX", "kind-platform")
    kc = kubectl(kctx)
    ns = [*kc, "-n", "observability"]
    for name, url in REPOS.items():
        run(["helm", "repo", "add", name, url], capture=True)
    run(["helm", "repo", "update"], capture=True)
    apply(kc, {"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "observability"}})
    # Grafana admin password: generated once (read it: kubectl -n observability get secret grafana-admin).
    if not succeeds([*ns, "get", "secret", "grafana-admin"]):
        apply(
            kc,
            {
                "apiVersion": "v1",
                "kind": "Secret",
                "metadata": {"name": "grafana-admin", "namespace": "observability"},
                "stringData": {"admin-user": "admin", "admin-password": secrets.token_hex(12)},
            },
        )
    dashboards = run(
        [
            *ns,
            "create",
            "configmap",
            "grafana-dashboards-platform",
            f"--from-file={VALUES}/dashboards",
            "--dry-run=client",
            "-o",
            "yaml",
        ],
        capture=True,
    )
    apply(ns, dashboards)

    helm = ["helm", "--kube-context", kctx, "-n", "observability", "upgrade", "--install"]
    run(
        [
            *helm,
            "prometheus",
            "prometheus-community/prometheus",
            "--version",
            "29.36.2",
            *prometheus_values(kctx),
            "--wait",
        ]
    )
    run(
        [
            *helm,
            "loki",
            "grafana/loki",
            "--version",
            "7.3.0",
            "-f",
            f"{VALUES}/loki-values.yaml",
            "--wait",
            "--timeout",
            "10m",
        ]
    )
    run(
        [
            *helm,
            "fluent-bit",
            "fluent/fluent-bit",
            "--version",
            "0.58.3",
            "-f",
            f"{VALUES}/fluent-bit-values.yaml",
            "--wait",
        ]
    )
    run(
        [
            *helm,
            "jaeger",
            "jaegertracing/jaeger",
            "--version",
            "4.16.1",
            "-f",
            f"{VALUES}/jaeger-values.yaml",
            "--wait",
        ]
    )
    run(
        [
            *helm,
            "grafana",
            "grafana/grafana",
            "--version",
            "10.5.15",
            "-f",
            f"{VALUES}/grafana-values.yaml",
            "--wait",
        ]
    )
    run([*ns, "get", "pods"])


if __name__ == "__main__":
    main()
