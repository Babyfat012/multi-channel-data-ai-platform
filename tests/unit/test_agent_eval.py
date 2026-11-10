"""Check the grading in ml/agent_eval: number normalization, rate rounding boundary, ungradable SKUs."""

import json

import pytest

from ml.agent_eval import numbers, score, score_policy, score_predict, texts

FEATURES = {"labeled_count_90d": 30, "positive_count_90d": 10, "return_or_refund_rate_90d": 1 / 3}


def test_numbers_normalise_percent_and_comma():
    """33,3% and 0.333 are the same number; integers stay as they are."""
    assert numbers("rate 33,3% from 30 lines (0.333)") == {"0.333", "30"}


@pytest.mark.parametrize(
    ("answer", "ok"),
    [
        ("30 labeled, 10 returned, rate 0.33", True),
        ("30 labeled, 10 returned, rate 33.3%", True),
        ("30 labeled, 10 returned, rate 0.34", False),  # off by 0.0067 > 0.005
        ("30 labeled, 11 returned, rate 0.33", False),
        ("about a third of orders come back", False),  # no real number ⇒ fail
        ("labeled: 30, returned: 10, rate: 33.3%", True),
        ("30 labeled lines, of which 10 were returned; rate 0.333", True),
        # Codex bug 2026-10-22: swapped fields or blind guesses used to PASS
        ("labeled_count_90d=10, positive_count_90d=30, rate=33.3%", False),
        ("The values may be 10, 30, 0.333, but I cannot verify them.", False),
        ("labeled 30, positive 10, rate 0.333; actually labeled 10", False),  # self-contradictory
    ],
)
def test_score_boundaries(answer, ok):
    """Pass only when all three numbers are present; the rate accepts rounding up to 0.005."""
    assert score(answer, FEATURES)["pass"] is ok


@pytest.mark.parametrize(
    ("answer", "ok"),
    [
        (
            '{"labeled_count_90d": 30, "positive_count_90d": 10, "return_or_refund_rate_90d": 0.333}',
            True,
        ),
        (
            'Here: {"labeled_count_90d": "30", "positive_count_90d": 10, "return_or_refund_rate_90d": 0.33}',
            True,
        ),
        (
            '{"labeled_count_90d": 10, "positive_count_90d": 30, "return_or_refund_rate_90d": 0.333}',
            False,
        ),
    ],
)
def test_score_json_checks_each_field(answer, ok):
    """JSON with all three fields ⇒ grade per field; swapped fields fail."""
    result = score(answer, FEATURES)
    assert (result["pass"], result["mode"]) == (ok, "json")


def test_broken_json_falls_back_to_text():
    """Broken JSON/missing fields ⇒ fall back to prose (an agent that forgets the format is not unfairly penalized)."""
    result = score('{"labeled_count_90d": 30} labeled 30, returned 10, rate 0.333', FEATURES)
    assert (result["pass"], result["mode"]) == (True, "text")


@pytest.mark.parametrize(
    "features",
    [
        {"labeled_count_90d": 0, "positive_count_90d": 0, "return_or_refund_rate_90d": None},
        {"labeled_count_90d": None},
    ],
)
def test_sku_without_history_is_not_graded(features):
    """No history yet ⇒ not graded by numbers (so an agent saying '0' does not pass)."""
    assert score("0 0 0", features) == {"gradable": False, "pass": None}


def test_texts_collects_nested_a2a_parts():
    """Collect text from every level of an A2A result."""
    result = {
        "status": {"message": {"parts": [{"kind": "text", "text": "a"}]}},
        "artifacts": [{"parts": [{"text": "b"}]}],
    }
    assert texts(result) == ["a", "b"]


def test_sku_id_digits_are_not_counted_as_answer_numbers():
    """'sku-00010' must not count as the number 10 ⇒ just repeating the SKU id does not pass."""
    assert numbers("sku-00010 sku-00030") == set()
    # Pipeline A (AWS): ids APP-00042 / ELE-01103 must not be read as 42 / 1103 either.
    assert numbers("APP-00042 has 3 returns, ELE-01103 has 0.25") == {"3", "0.25"}
    assert score("For sku-00010 and sku-00030: rate 0.33", FEATURES)["pass"] is False


@pytest.mark.parametrize(
    ("answer", "exit_code"),
    [
        (
            '{"labeled_count_90d": 30, "positive_count_90d": 10, "return_or_refund_rate_90d": 0.333}',
            0,
        ),
        (
            '{"labeled_count_90d": 30, "positive_count_90d": 11, "return_or_refund_rate_90d": 0.333}',
            1,
        ),
    ],
)
def test_ci_http_truth_and_accuracy_gate(monkeypatch, tmp_path, capsys, answer, exit_code):
    """Port-forward HTTP needs no CA; below threshold returns 1 and still writes JSONL/summary."""
    import json

    from ml import agent_eval

    def fake_post(url, body, headers, context=None, timeout=180):
        assert context is None
        if url.endswith("/features"):
            return {"features": [{"seller_sku": "sku-00001", **FEATURES}]}
        assert headers["Authorization"] == "Bearer fake-key"
        return {"result": {"parts": [{"text": answer}]}}

    monkeypatch.setattr(agent_eval, "post", fake_post)
    monkeypatch.setenv("AGW_AGENT_KEY", "fake-key")
    output = tmp_path / "eval.jsonl"
    assert (
        agent_eval.main(
            [
                "--agent",
                "data-agent",
                "--skus",
                "sku-00001",
                "--feature-api",
                "http://localhost:18000",
                "--min-accuracy",
                "0.95",
                "--ca",
                "/does-not-exist",
                "--out",
                str(output),
            ]
        )
        == exit_code
    )
    assert json.loads(output.read_text())["pass"] is (exit_code == 0)
    assert json.loads(capsys.readouterr().out)["accuracy"] == (1 if exit_code == 0 else 0)


def test_timeout_flag_reaches_agent_call_only(monkeypatch, tmp_path):
    """--timeout applies only to agent questions, not to the ground-truth calls."""
    from ml import agent_eval

    seen = {}

    def fake_post(url, body, headers, context=None, timeout=180):
        seen[url.rsplit("/", 2)[-1] or "a2a"] = timeout
        if url.endswith("/features"):
            return {"features": [{"seller_sku": "sku-00001", **FEATURES}]}
        return {"result": {"parts": [{"text": "x"}]}}

    monkeypatch.setattr(agent_eval, "post", fake_post)
    monkeypatch.setenv("AGW_AGENT_KEY", "k")
    args = ["--agent", "coordinator", "--skus", "sku-00001", "--feature-api", "http://f"]
    agent_eval.main([*args, "--timeout", "600", "--out", str(tmp_path / "o.jsonl")])
    assert seen == {"features": 180, "a2a": 600}


def test_texts_of_artifacts_ignore_history_echo():
    """Grade the final answer: the echoed question in history must not leak in."""
    result = {
        "history": [{"parts": [{"text": "Reply {labeled_count_90d: int} for sku-00002"}]}],
        "artifacts": [{"parts": [{"text": "labeled 44"}]}],
    }
    assert " ".join(texts(result["artifacts"])) == "labeled 44"


def test_time_window_is_not_a_value():
    """'last 90 days: 30' → the value is 30, not 90; but the real numbers must still be complete and correct."""
    ok = "Total labeled returns in the last 90 days: 30\nPositive returns in the last 90 days: 10\nRate in the last 90 days: 33.3%"
    assert score(ok, FEATURES)["pass"] is True
    assert score(ok.replace(": 10", ": 11"), FEATURES)["pass"] is False


@pytest.mark.parametrize(
    ("answer", "ok"),
    [
        ("Shopee buyers can request a return within 15 days after delivery.", True),
        ("Within 30 days after delivery.", False),
        ("The window is 150 days.", False),  # 150 is not 15
    ],
)
def test_score_policy(answer, ok):
    """Pass on the exact number from the document; a different number or one containing it fails."""
    assert score_policy(answer, "15")["pass"] is ok


@pytest.mark.parametrize(
    ("truth", "answer", "ok"),
    [
        ({"score": 0.3047, "abstain": False}, "The risk score is 0.30 (low).", True),
        ({"score": 0.3047, "abstain": False}, "The risk score is 0.45.", False),
        ({"score": None, "abstain": True}, "The model abstains: too little history.", True),
        ({"score": None, "abstain": True}, "The risk score is 0.30.", False),
    ],
)
def test_score_predict(truth, answer, ok):
    """Score within ≤ 0.005 of the Feature API; for a SKU without enough history the agent must abstain."""
    assert score_predict(answer, truth)["pass"] is ok


def test_main_stops_asking_after_two_timeouts(monkeypatch, tmp_path):
    """Two consecutive timeouts ⇒ stop asking (no orphan sessions piling up); skipped questions count as failures."""
    import ml.agent_eval as ae

    calls = []

    def fake_post(url, body, headers, context=None, timeout=180):
        if url.endswith("/features"):
            return {"features": [{"seller_sku": f"S-{i}", **FEATURES} for i in range(5)]}
        calls.append(url)
        raise TimeoutError

    monkeypatch.setattr(ae, "post", fake_post)
    monkeypatch.setenv("AGW_AGENT_KEY", "k")
    out = tmp_path / "o.jsonl"
    rc = ae.main(
        ["--agent", "coordinator", "--skus", "a", "--feature-api", "http://x", "--out", str(out)]
    )
    rows = [json.loads(line) for line in out.read_text().splitlines()]
    assert (rc, len(calls), len(rows)) == (1, 2, 5)
    assert [r["mode"] for r in rows[2:]] == ["skipped"] * 3
    assert not any(r["pass"] for r in rows)


def test_score_route():
    """Coordinator: calling the right sub-agent + giving a final answer passes, even if numbers are miscopied; a wrong call or no answer fails."""
    from ml.agent_eval import score_route

    call = {"history": [{"parts": [{"data": {"name": "kagent__kagent__data_agent"}}]}]}
    final = {"artifacts": [{"parts": [{"text": "0 returns"}]}]}
    assert score_route({**call, **final}, "data_agent")["pass"] is True
    assert score_route({**call, **final}, "model_agent")["pass"] is False
    assert score_route(call, "data_agent")["pass"] is False
    assert score_route({}, "data_agent")["pass"] is False
