"""LLM A/B gate: the decision rule, where it is recorded, how the gateway drops B, and the order of the steps.

Offline: no cluster, no registry, no eval run (commands are recorded, not executed).
"""

import contextlib
import json
from pathlib import Path

import pytest
import yaml

from scripts.llm import ab_gate, check_gateway_allowlist
from scripts.llm.check_gateway_allowlist import DECISION_LABEL, GATEWAY, b_kept, without_b
from scripts.llm.publish_agentregistry import REGISTRY, tagged


@pytest.mark.parametrize(
    ("a", "b", "keep", "why"),
    [
        (1.0, 4 / 12, False, "< threshold"),  # the AWS result: B 4/12
        (0.8, 0.94, False, "< threshold"),  # better than a weak A is not enough
        (1.0, 0.96, False, "< A accuracy"),  # above the bar but less accurate than A
        (0.96, 0.96, True, "≥ A accuracy"),  # same quality (and faster): keep
        (0.95, 1.0, True, "≥ A accuracy"),
        (None, 1.0, False, "no eval result for A"),  # nothing to compare with
        (1.0, None, False, "no eval result for B"),  # B down or eval crashed
    ],
)
def test_b_wins_only_at_the_threshold_and_not_below_a(a, b, keep, why):
    """B must clear the eval threshold AND match A; a missing result never keeps B."""
    assert ab_gate.decide(a, b, minimum=0.95)[0] is keep
    assert why in ab_gate.decide(a, b, minimum=0.95)[1]


def test_accuracy_reads_the_eval_summary_and_treats_garbage_as_missing(tmp_path, monkeypatch):
    """eval_agent_ci deletes its summary before running, so a crash leaves nothing ⇒ None, never a stale score."""
    monkeypatch.setattr(ab_gate, "ROOT", tmp_path)
    (tmp_path / "out").mkdir()
    assert ab_gate.accuracy("data-agent-b") is None
    summary = tmp_path / "out/agent-eval-data-agent-b-features-summary.json"
    summary.write_text(json.dumps({"agent": "data-agent-b", "accuracy": 0.333}))
    assert ab_gate.accuracy("data-agent-b") == 0.333
    summary.write_text("not json")
    assert ab_gate.accuracy("data-agent-b") is None


def test_decision_is_a_label_on_the_stable_registry_entry():
    """Published manifest: data-agent-b, tag stable, decision label; a manifest with its own labels is refused."""
    text = (REGISTRY / "data-agent-b.yaml").read_text()
    published = yaml.safe_load(tagged(ab_gate.with_decision(text, "drop"), "stable"))
    assert published["metadata"] == {
        "name": "data-agent-b",
        "tag": "stable",
        "labels": {DECISION_LABEL: "drop"},
    }
    with pytest.raises(ValueError, match="owns them"):
        ab_gate.with_decision(text.replace("metadata:\n", "metadata:\n  labels: {}\n"), "keep")


def gateway_config(text: str) -> dict:
    """The agentgateway config embedded in the ConfigMap of the gateway manifests."""
    configmap = next(d for d in yaml.safe_load_all(text) if d and d["kind"] == "ConfigMap")
    return yaml.safe_load(configmap["data"]["config.yaml"])


def test_dropping_b_removes_exactly_its_two_split_entries():
    """platform-chat → 1.5B only, /a2a/data-agent-ab/ → A only; B's model and direct eval route stay; nothing else moves."""
    before = gateway_config(GATEWAY.read_text())
    after = gateway_config(without_b(GATEWAY.read_text()))
    targets = after["llm"]["virtualModels"][0]["routing"]["weighted"]["targets"]
    assert targets == [{"model": "qwen2.5-1.5b-instruct", "weight": 1}]
    routes = {r["name"]: r for r in after["binds" if "binds" in after else "routes"]}
    assert [b["host"] for b in routes["a2a-data-agent-ab"]["backends"]] == [
        "data-agent.kagent.svc.cluster.local:8080"
    ]
    assert routes["a2a-data-agent-b-path"]["backends"] == [
        {"host": "data-agent-b.kagent.svc.cluster.local:8080"}
    ]
    # Everything except those two lists is identical.
    before["llm"]["virtualModels"][0]["routing"]["weighted"]["targets"] = targets
    for route in before["routes"]:
        if route["name"] == "a2a-data-agent-ab":
            route["backends"] = routes["a2a-data-agent-ab"]["backends"]
    assert before == after
    other_docs = [
        d
        for d in yaml.safe_load_all(without_b(GATEWAY.read_text()))
        if d and d["kind"] != "ConfigMap"
    ]
    assert other_docs == [
        d for d in yaml.safe_load_all(GATEWAY.read_text()) if d and d["kind"] != "ConfigMap"
    ]


def test_reshaped_gateway_fails_instead_of_keeping_b():
    """If the file no longer has each B block exactly once, refuse: guessing could leave B in the split."""
    text = GATEWAY.read_text()
    with pytest.raises(ValueError, match="exactly one"):
        without_b(text.replace("              weight: 1\n", "              weight: 2\n"))
    with pytest.raises(ValueError, match="exactly one"):
        without_b(without_b(text))


@pytest.mark.parametrize(
    ("agents", "kept"),
    [
        ([{"metadata": {"name": "data-agent-b", "labels": {DECISION_LABEL: "keep"}}}], True),
        ([{"metadata": {"name": "data-agent-b", "labels": {DECISION_LABEL: "drop"}}}], False),
        ([{"metadata": {"name": "data-agent-b"}}], False),  # published without a decision
        ([{"metadata": {"name": "data-agent", "labels": {DECISION_LABEL: "keep"}}}], False),
        ([], False),  # never evaluated
    ],
)
def test_gateway_keeps_b_only_on_an_explicit_keep(monkeypatch, agents, kept):
    """Default is drop: a deploy can only put B in the split after the gate said keep."""
    monkeypatch.setattr(check_gateway_allowlist, "run", lambda cmd, **kw: json.dumps(agents))
    assert b_kept() is kept


@pytest.mark.parametrize(("a", "b", "dropped"), [(1.0, 0.333, True), (0.96, 1.0, False)])
def test_gate_records_then_applies_then_scales_down(monkeypatch, a, b, dropped):
    """Decision → gateway apply → scale B to 0 (only when dropped): never half the traffic to a branch without pods."""
    calls = []

    def fake_run(cmd, check=True, **kw):
        calls.append(("eval" if "scripts.llm.eval_agent_ci" in cmd else " ".join(cmd[-3:]), check))
        return ""

    monkeypatch.setattr(ab_gate, "run", fake_run)
    monkeypatch.setattr(ab_gate, "kubectl", lambda: ["kubectl"])
    monkeypatch.setattr(ab_gate, "registry", contextlib.nullcontext)
    monkeypatch.setattr(ab_gate, "accuracy", {"data-agent": a, "data-agent-b": b}.get)
    ab_gate.main()
    steps = [c for c, _ in calls]
    assert steps[:2] == [
        "scale deploy/qwen-small --replicas=1",
        "scale deploy/data-agent-b --replicas=1",
    ]
    assert [check for c, check in calls if c == "eval"] == [
        False,
        False,
    ]  # a low score is not a crash
    apply_registry = next(i for i, c in enumerate(steps) if c.startswith("apply -f"))
    apply_gateway = steps.index("--tag stable --apply")
    assert apply_registry < apply_gateway
    down = [i for i, c in enumerate(steps) if c.endswith("--replicas=0")]
    assert (down and min(down) > apply_gateway) if dropped else not down


@pytest.mark.parametrize("kept", [True, False])
def test_every_gateway_apply_follows_the_recorded_decision(monkeypatch, kept):
    """The deploy pipelines' `--apply` (not only the gate) applies the config with B in or out per the decision."""
    applied = []

    def fake_run(cmd, **kw):
        if cmd[-3:-1] == ["apply", "-f"]:
            applied.append(Path(cmd[-1]).read_text())
        return ""

    monkeypatch.setattr(check_gateway_allowlist, "run", fake_run)
    monkeypatch.setattr(check_gateway_allowlist, "kubectl", lambda: ["kubectl"])
    monkeypatch.setattr(check_gateway_allowlist, "registry", contextlib.nullcontext)
    monkeypatch.setattr(
        check_gateway_allowlist,
        "registry_state",
        lambda tag: (
            {"data-agent", "model-agent", "coordinator"},
            {
                "get_sku_features",
                "get_policy_chunks",
                "search_policy_chunks",
                "predict_return_risk",
            },
        ),
    )
    monkeypatch.setattr(check_gateway_allowlist, "b_kept", lambda: kept)
    monkeypatch.setattr("sys.argv", ["check_gateway_allowlist", "--tag", "abc123", "--apply"])
    check_gateway_allowlist.main()
    config = GATEWAY.read_text()
    assert applied == [config if kept else without_b(config)]
