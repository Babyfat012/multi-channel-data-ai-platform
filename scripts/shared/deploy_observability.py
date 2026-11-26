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


def values(kctx: str, chart: str) -> list[str]:
    """EKS adds `<chart>-values-aws.yaml` (EC2 scrape + alert rules, EC2 Postgres datasource); kind has no EC2."""
    files = [f"{VALUES}/{chart}-values.yaml"]
    if not kctx.startswith("kind-"):
        files.append(f"{VALUES}/{chart}-values-aws.yaml")
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
    # Read-only Postgres password for the Live orders dashboard (EKS only); same value the EC2 role was created with.
    if not kctx.startswith("kind-") and os.environ.get("GRAFANA_RO_PASSWORD"):
        apply(
            kc,
            {
                "apiVersion": "v1",
                "kind": "Secret",
                "metadata": {"name": "grafana-postgres-ro", "namespace": "observability"},
                "stringData": {"password": os.environ["GRAFANA_RO_PASSWORD"]},
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
            *values(kctx, "prometheus"),
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
            *values(kctx, "grafana"),
            "--wait",
        ]
    )
    run([*ns, "get", "pods"])


if __name__ == "__main__":
    main()
