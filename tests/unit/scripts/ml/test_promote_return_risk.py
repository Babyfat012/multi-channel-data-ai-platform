"""Approval gate challenger → production, against an in-memory stand-in for MlflowClient."""

from datetime import UTC, datetime
from types import SimpleNamespace as NS

import pytest

from scripts.ml.promote_return_risk import promote, show

NOW = datetime(2026, 11, 22, tzinfo=UTC)


class Registry:
    """Only the MlflowClient calls the script makes; versions keyed by str, aliases may hold int like MLflow."""

    def __init__(self, aliases, tags=None, metrics=None):
        self.aliases = dict(aliases)
        self.tags = {v: dict(t) for v, t in (tags or {}).items()}
        self.metrics = metrics or {}

    def search_registered_models(self, filter_string):
        return [NS(aliases=dict(self.aliases))]

    def get_model_version(self, name, version):
        return NS(run_id=f"run-{version}", tags=self.tags.get(str(version), {}))

    def get_run(self, run_id):
        return NS(data=NS(metrics=self.metrics))

    def set_model_version_tag(self, name, version, key, value):
        self.tags.setdefault(str(version), {})[key] = value

    def set_registered_model_alias(self, name, alias, version):
        self.aliases[alias] = version

    def delete_registered_model_alias(self, name, alias):
        del self.aliases[alias]


def test_promote_moves_production_and_records_the_approval():
    """Approved challenger becomes production; who/when/what it replaced is kept for rollback."""
    reg = Registry({"production": 3, "challenger": 5}, tags={"5": {"beats_baseline": "true"}})
    promote(reg, "5", "alice", NOW)
    assert reg.aliases == {"production": "5"}
    assert reg.tags["5"] == {
        "beats_baseline": "true",
        "approved_by": "alice",
        "approved_at": NOW.isoformat(),
        "previous_production": "3",
    }


def test_promote_refuses_a_version_that_is_no_longer_the_challenger():
    """Training moved the challenger while the human was reviewing ⇒ refuse, change nothing."""
    reg = Registry({"production": 3, "challenger": 6}, tags={"5": {"beats_baseline": "true"}})
    with pytest.raises(SystemExit, match="no longer the challenger"):
        promote(reg, "5", "alice", NOW)
    assert reg.aliases == {"production": 3, "challenger": 6} and "approved_by" not in reg.tags["5"]


def test_promote_refuses_a_challenger_below_the_baseline():
    """Defence in depth: an alias set by hand on a losing version is not promotable."""
    reg = Registry({"challenger": 5}, tags={"5": {"beats_baseline": "false"}})
    with pytest.raises(SystemExit, match="baseline"):
        promote(reg, "5", "alice", NOW)
    assert "production" not in reg.aliases


def test_rerun_after_a_crash_keeps_the_rollback_target():
    """Crash between the two alias writes ⇒ rerun only clears challenger; previous_production stays v3."""
    tags = {"5": {"beats_baseline": "true", "previous_production": "3", "approved_by": "alice"}}
    reg = Registry({"production": "5", "challenger": "5"}, tags=tags)
    promote(reg, "5", "bob", NOW)
    assert reg.aliases == {"production": "5"}
    assert reg.tags["5"]["previous_production"] == "3" and reg.tags["5"]["approved_by"] == "alice"


def test_show_returns_the_challenger_and_warns_on_a_stale_comparison(capsys):
    """Scored against v2 but production is now v3 ⇒ the comparison is stale, say so."""
    metrics = {"pr_auc_model": 0.36, "pr_auc_champion": 0.34, "champion_version": 2.0}
    assert show(Registry({"production": 3, "challenger": 5}, metrics=metrics)) == "5"
    assert "not scored against the current production v3" in capsys.readouterr().err
    metrics["champion_version"] = 3.0
    show(Registry({"production": 3, "challenger": 5}, metrics=metrics))
    assert "WARNING" not in capsys.readouterr().err


def test_show_without_challenger_stops():
    """Nothing beat the baseline ⇒ the pipeline stops before asking anyone to approve."""
    with pytest.raises(SystemExit, match="no challenger"):
        show(Registry({"production": 3}))
