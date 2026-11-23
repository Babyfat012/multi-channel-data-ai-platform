"""Human approval of the return-risk model: challenger → production (diagram D11).

Training only sets alias `challenger` (beats the baseline). This script, run by the return-risk Jenkins
pipeline around an `input` step, is the only writer of alias `production`:

    show                                 comparison on stderr, challenger version on stdout
    promote --version N --approved-by X  after approval: N must STILL be the challenger, then move the alias

Between `show` and the click a new training run may move `challenger`; `promote` re-checks so the human
never approves one version and ships another. Needs the MLflow client:

    uv run --no-project --with mlflow-skinny==3.17.0 python -m scripts.ml.promote_return_risk show
"""

import argparse
import sys
from datetime import UTC, datetime

from scripts.common import load_env
from scripts.ml.deploy_return_risk import mlflow_uri

MODEL = "return-risk"
SHOWN = ["test_rows", "prevalence", "pr_auc_model", "pr_auc_baseline", "pr_auc_champion"]


def aliases(client) -> dict[str, str]:
    """{alias: version} of the model; versions as str (MLflow returns int or str by store)."""
    found = client.search_registered_models(filter_string=f"name='{MODEL}'")
    return {k: str(v) for k, v in found[0].aliases.items()} if found else {}


def show(client) -> str:
    """Print the challenger next to the champion; return the challenger version."""
    current = aliases(client)
    challenger, production = current.get("challenger"), current.get("production")
    if challenger is None:
        sys.exit("no challenger: no training run beat the baseline since the last approval")
    metrics = client.get_run(client.get_model_version(MODEL, challenger).run_id).data.metrics
    print(f"challenger v{challenger}, production v{production or '-'}", file=sys.stderr)
    for key in SHOWN:
        print(f"  {key:16} {metrics.get(key, '-')}", file=sys.stderr)
    # The comparison only means something against the model that is in production NOW, on the same test set.
    scored = metrics.get("champion_version")
    if production and (scored is None or str(int(scored)) != production):
        print(
            f"  WARNING: not scored against the current production v{production} "
            f"(scored against: {'none' if scored is None else f'v{int(scored)}'}); retrain to compare",
            file=sys.stderr,
        )
    return challenger


def promote(client, version: str, approver: str, now: datetime) -> None:
    """Move alias production → version, record who/when/what it replaced, clear the challenger."""
    current = aliases(client)
    if current.get("challenger") != version:
        sys.exit(
            f"v{version} is no longer the challenger (now v{current.get('challenger')}): "
            "rerun the pipeline and review again"
        )
    if client.get_model_version(MODEL, version).tags.get("beats_baseline") != "true":
        sys.exit(f"v{version} does not beat the baseline")
    if current.get("production") == version:
        # An earlier promote died between the two alias writes: finish it, keep its approval tags
        # (rewriting previous_production now would lose the version to roll back to).
        client.delete_registered_model_alias(MODEL, "challenger")
        return
    for key, value in {
        "approved_by": approver,
        "approved_at": now.isoformat(),
        "previous_production": current.get("production", ""),
    }.items():
        client.set_model_version_tag(MODEL, version, key, value)
    # Production first: a crash in between leaves challenger == production, finished by rerunning promote.
    client.set_registered_model_alias(MODEL, "production", version)
    client.delete_registered_model_alias(MODEL, "challenger")
    print(f"{MODEL} v{version} → production (approved by {approver})", file=sys.stderr)


def main(argv=None) -> None:
    """CLI used by infra/jenkins/return-risk.Jenkinsfile."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("show")
    promote_cmd = sub.add_parser("promote")
    promote_cmd.add_argument("--version", required=True)
    promote_cmd.add_argument("--approved-by", required=True)
    args = parser.parse_args(argv)

    from mlflow import MlflowClient  # imported here: tests pass a fake client

    load_env(required=False, override=False)  # only MLFLOW_*; the Jenkins workspace has no .env
    client = MlflowClient(mlflow_uri())
    if args.command == "show":
        print(show(client))
    else:
        promote(client, args.version, args.approved_by, datetime.now(UTC))


if __name__ == "__main__":
    main()
