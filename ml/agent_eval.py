"""Agent eval harness and regression gate.

Ask the agent (through agentgateway, A2A protocol) the same set of SKU questions; the ground truth comes
from the Feature API (the very source the agent must call as a tool). The agent is asked to reply with a
three-field JSON; without JSON the prose is parsed and a number must sit next to the field name. The
coordinator agent is graded on routing only (calls the right sub-agent tool, gives a final answer): the 1.5B model
often drops digits when copying, and numeric correctness is covered by each sub-agent's eval. PASS when the labeled
line count, returned line count and 90-day return rate are all CORRECT. Accuracy below threshold ⇒ exit code 1 ⇒ CI marks the build red after an agent deploy.
Run: `python -m ml.agent_eval --agent data-agent --skus sku-00001,sku-00002 --min-accuracy 0.6`.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import ssl
import sys
import time
import urllib.error
import urllib.request
import uuid

NUMBER = re.compile(r"(?<![\w.])-?\d+(?:[.,]\d+)?%?")
# SKU ids are letters-digits: local `sku-00001`, pipeline A on AWS `APP-00042`; digits in an id are not answers.
SKU_ID = re.compile(r"\b[A-Za-z]+-\d+\b")
# Time windows ("last 90 days", "24 hours") are context, not values to grade.
WINDOW = re.compile(r"\b\d+[- ]?(?:days?|hours?)\b", re.IGNORECASE)
CLAUSE = re.compile(r"[;\n]|[.,](?=\s|$)")  # "0.333", "33,3%" are not split
FIELDS = {  # field name → how the agent usually refers to it in prose
    "labeled_count_90d": re.compile(r"\w*label\w*", re.IGNORECASE),
    "positive_count_90d": re.compile(r"\w*(?:positive|returned|refunded)\w*", re.IGNORECASE),
    "return_or_refund_rate_90d": re.compile(r"\w*rate\w*", re.IGNORECASE),
}
ASK_JSON = (
    " Reply with one JSON object with exactly these keys: "
    '{"labeled_count_90d": int, "positive_count_90d": int, "return_or_refund_rate_90d": float}.'
)


def value(raw: str) -> str:
    """Normalize a number: '33.3%' → '0.333', '0,25' → '0.25', '7' → '7'."""
    number = float(raw.rstrip("%").replace(",", "."))
    if raw.endswith("%"):
        number /= 100
    return f"{number:.3f}".rstrip("0").rstrip(".")


def numbers(text: str) -> set[str]:
    """Every number in the text, normalized."""
    return {value(raw) for raw in NUMBER.findall(SKU_ID.sub(" ", text))}  # "sku-00002" is not 2


def expected(features: dict) -> dict[str, str] | None:
    """The three numbers the agent must state. None when the SKU has no history (not gradable by numbers)."""
    labeled, positive = features.get("labeled_count_90d"), features.get("positive_count_90d")
    rate = features.get("return_or_refund_rate_90d")
    if not labeled or rate is None:
        return None
    return {
        "labeled_count_90d": str(labeled),
        "positive_count_90d": str(positive),
        "return_or_refund_rate_90d": f"{rate:.3f}".rstrip("0").rstrip("."),
    }


def rate_close(found: set[str], target: str) -> bool:
    """The rate passes when some number is within ≤ 0.005 (agents often round to 2 digits or say a percentage)."""
    return any(abs(float(x) - float(target)) <= 0.005 for x in found if float(x) <= 1)


def from_json(answer: str) -> dict[str, set[str]] | None:
    """Read the 'form with fields': the first JSON object with all three fields. None when the agent returns no JSON."""
    for chunk in re.findall(r"\{[^{}]*\}", answer):
        try:
            data = json.loads(chunk)
            if isinstance(data, dict) and set(FIELDS) <= data.keys():
                return {field: {value(str(data[field]))} for field in FIELDS}
        except ValueError:  # bad JSON or non-numeric field ⇒ try next chunk, then prose
            continue
    return None


def from_text(answer: str) -> dict[str, set[str]]:
    """Parse prose: each mention of a field name takes the number CLOSEST to it in the same clause (tie ⇒ the later number).

    Numbers not next to any field name do not count ⇒ a blind guess "10, 30, 0.333" fails; a swapped
    "labeled 10, positive 30" fails; mentioning one field twice with two different numbers also fails.
    """
    # Limitation: a character-distance heuristic, roundabout sentences can be misgraded; JSON is the main path,
    # the mode column shows how many answers take this path.
    out: dict[str, set[str]] = {field: set() for field in FIELDS}
    for clause in CLAUSE.split(WINDOW.sub(" ", SKU_ID.sub(" ", answer))):
        nums = [(m.start(), m.end(), m.group()) for m in NUMBER.finditer(clause)]
        for field, label in FIELDS.items():
            for hit in label.finditer(clause):
                gaps = [
                    (
                        start - hit.end() if start >= hit.end() else hit.start() - end,
                        start < hit.start(),
                        raw,
                    )
                    for start, end, raw in nums
                ]
                if gaps:
                    out[field].add(value(min(gaps)[2]))
    return out


def score(answer: str, features: dict) -> dict:
    """Grade one answer: each field must have exactly ONE value and that value must be correct."""
    want = expected(features)
    if want is None:
        return {"gradable": False, "pass": None}
    found = from_json(answer)
    mode = "json" if found else "text"
    found = found or from_text(answer)
    checks = {
        field: len(found[field]) == 1
        and (
            rate_close(found[field], want[field])
            if field == "return_or_refund_rate_90d"
            else want[field] in found[field]
        )
        for field in FIELDS
    }
    return {
        "gradable": True,
        "pass": all(checks.values()),
        "mode": mode,
        "checks": checks,
        "expected": want,
    }


# Agent 2 (model-agent), RAG tool: question → the number that must appear in the answer (checked against rag/corpus/*.md).
POLICY_CASES = [
    ("How many days does a Shopee buyer have to request a return after delivery?", "15"),
    ("What is the TikTok Shop return window for most categories, in days?", "15"),
    ("How many days does an Amazon seller have to inspect returned items?", "3"),
    ("How many days does a Shopee seller have to answer a return dispute?", "3"),
    ("How many days do Walmart buyers have to return an item after delivery?", "30"),
]
ABSTAIN = re.compile(r"abstain|too little|insufficient|not enough", re.IGNORECASE)


def score_policy(answer: str, want: str) -> dict:
    """A policy question passes when the correct number appears in the answer (digits in SKU ids excluded)."""
    found = numbers(WINDOW.sub(lambda m: m.group().split()[0], answer))
    return {"gradable": True, "pass": want in found, "mode": "policy", "expected": want}


def score_predict(answer: str, truth: dict) -> dict:
    """A prediction passes when score is within ≤ 0.005 of Feature API /predict (or both abstain)."""
    if truth.get("abstain") or truth.get("score") is None:
        return {"gradable": True, "pass": bool(ABSTAIN.search(answer)), "mode": "predict"}
    want = str(truth["score"])  # not rounded: agents often round to 2 digits
    return {
        "gradable": True,
        "pass": rate_close(numbers(answer), want),
        "mode": "predict",
        "expected": want,
    }


def score_route(result: dict, tool: str) -> dict:
    """The coordinator passes when history has a call to tool `tool` and there is a final answer; numbers are graded by the sub-agent evals."""
    routed = tool in json.dumps(result.get("history") or [])
    answered = bool(" ".join(texts(result.get("artifacts") or [])).strip())
    return {"gradable": True, "pass": routed and answered, "mode": "route", "expected": tool}


def texts(node) -> list[str]:
    """Collect every "text" field in an A2A result (artifacts/parts/status.message), in order."""
    if isinstance(node, dict):
        own = [node["text"]] if isinstance(node.get("text"), str) else []
        return own + [t for v in node.values() for t in texts(v)]
    if isinstance(node, list):
        return [t for v in node for t in texts(v)]
    return []


def post(url: str, body: dict, headers: dict, context=None, timeout=180) -> dict:
    """POST JSON, return JSON."""
    req = urllib.request.Request(
        url, json.dumps(body).encode(), {"Content-Type": "application/json", **headers}
    )
    return json.load(urllib.request.urlopen(req, timeout=timeout, context=context))


def main(argv=None) -> int:
    """Ask the agent about each SKU, grade, write JSONL, return code 1 if accuracy < threshold."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--agent", required=True, help="A2A route name: data-agent, data-agent-b, data-agent-ab"
    )
    parser.add_argument("--skus", default="", help="required with --task features/predict")
    parser.add_argument(
        "--task",
        choices=("features", "predict", "policy"),
        default="features",
        help="features: Agent 1; predict + policy: the two tools of Agent 2 (model-agent)",
    )
    parser.add_argument("--gateway", default="http://localhost:13000")
    parser.add_argument("--feature-api", default="https://api.172-25-0-2.sslip.io:30443")
    parser.add_argument("--ca", default="out/ingress/platform-local-ca.crt")
    parser.add_argument("--min-accuracy", type=float, default=0.6)
    parser.add_argument("--out", default="out/agent-eval.jsonl")
    parser.add_argument(
        "--timeout",
        type=int,
        default=180,
        help="seconds to wait per agent question (slow CPU: raise it)",
    )
    args = parser.parse_args(argv)
    key = {"Authorization": f"Bearer {os.environ['AGW_AGENT_KEY']}"}
    context = (
        ssl.create_default_context(cafile=args.ca)
        if args.feature_api.startswith("https://")
        else None
    )
    skus = [x for x in args.skus.split(",") if x]
    jobs = []  # (label, question, grading function)
    if args.task == "policy":
        jobs = [(q, q, lambda a, w=w: score_policy(a, w)) for q, w in POLICY_CASES]
    else:
        if not skus:
            parser.error("--skus is required with --task features/predict")
        if args.task == "features":
            truth = post(f"{args.feature_api}/features", {"seller_skus": skus}, {}, context)[
                "features"
            ]
            jobs = [
                (
                    t["seller_sku"],
                    f"What are the return features of {t['seller_sku']}?" + ASK_JSON,
                    lambda a, t=t: score(a, t),
                )
                for t in truth
            ]
        else:
            for sku in skus:
                order = {
                    "seller_sku": sku,
                    "channel": "amazon",
                    "country": "US",
                    "ordered_basis_qty": 2,
                    "ordered_product_net_minor": 4500,
                }
                truth = post(f"{args.feature_api}/predict", order, {}, context)
                question = (
                    f"What is the return risk of a new order: 2 units of {sku} on amazon, "
                    "shipped to US, price 4500 cents?"
                )
                jobs.append((sku, question, lambda a, t=truth: score_predict(a, t)))
    tool = "data_agent" if args.task == "features" else "model_agent"
    rows = []
    timeouts = 0  # consecutive timeouts
    for label, question, grade in jobs:
        if timeouts >= 2:
            # The agent keeps processing timed-out questions in the background: asking more only piles orphan sessions onto the LLM (spirals on 04/11 and 06/11).
            # Skipped questions count as failures (accuracy is still divided by the full set).
            rows.append(
                {
                    "agent": args.agent,
                    "sku": label,
                    "seconds": 0,
                    "answer": "SKIPPED after 2 timeouts",
                    "gradable": True,
                    "pass": False,
                    "mode": "skipped",
                }
            )
            continue
        started = time.perf_counter()
        result = {}
        try:
            reply = post(
                f"{args.gateway}/a2a/{args.agent}/",
                {
                    "jsonrpc": "2.0",
                    "id": str(uuid.uuid4()),
                    "method": "message/send",
                    "params": {
                        "message": {
                            "role": "user",
                            "messageId": str(uuid.uuid4()),
                            "parts": [{"kind": "text", "text": question}],
                        }
                    },
                },
                key,
                timeout=args.timeout,
            )
            result = reply.get("result", {})
            # Only the final answer (artifacts): history holds the echoed question and tool calls, which would skew grading.
            answer = " ".join(texts(result.get("artifacts") or result))
        except (
            urllib.error.URLError,
            TimeoutError,
        ) as error:  # a broken agent = a failed question, the harness keeps going
            answer = f"ERROR {error}"
        timeouts = timeouts + 1 if answer.startswith("ERROR") else 0
        rows.append(
            {
                "agent": args.agent,
                "sku": label,
                "seconds": round(time.perf_counter() - started, 2),
                "answer": answer[-600:],
                **(score_route(result, tool) if args.agent == "coordinator" else grade(answer)),
            }
        )
    graded = [r for r in rows if r["gradable"]]
    accuracy = sum(r["pass"] for r in graded) / len(graded) if graded else 0.0
    with open(args.out, "a") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    print(
        json.dumps(
            {
                "agent": args.agent,
                "graded": len(graded),
                "accuracy": round(accuracy, 3),
                "min": args.min_accuracy,
            }
        )
    )
    return 0 if accuracy >= args.min_accuracy else 1


if __name__ == "__main__":
    sys.exit(main())
