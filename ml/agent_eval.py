"""ML-N1: eval harness cho agent + cổng hồi quy (regression gate).

Hỏi agent (qua agentgateway, giao thức A2A) cùng một bộ câu về SKU; đáp án chuẩn lấy từ Feature API
(chính nguồn agent phải gọi tool). Agent được yêu cầu trả JSON ba trường; không có JSON thì đọc câu
văn, số phải đứng cạnh tên trường. ĐẠT khi số line có nhãn, số line bị trả và tỷ lệ trả 90 ngày đều đúng
TRƯỜNG. Độ chính xác dưới ngưỡng ⇒ thoát mã 1 ⇒ CI báo build đỏ sau deploy agent.
Chạy: `python -m ml.agent_eval --agent data-agent --skus sku-00001,sku-00002 --min-accuracy 0.6`.
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
# Mã SKU dạng chữ-số: local `sku-00001`, pipeline A trên AWS `APP-00042`; số trong mã không phải đáp án.
SKU_ID = re.compile(r"\b[A-Za-z]+-\d+\b")
# Cửa sổ thời gian ("last 90 days", "24 hours") là ngữ cảnh, không phải giá trị cần chấm.
WINDOW = re.compile(r"\b\d+[- ]?(?:days?|hours?)\b", re.IGNORECASE)
CLAUSE = re.compile(r"[;\n]|[.,](?=\s|$)")  # "0.333", "33,3%" không bị cắt
FIELDS = {  # tên trường → cách agent hay gọi nó trong câu văn
    "labeled_count_90d": re.compile(r"\w*label\w*", re.IGNORECASE),
    "positive_count_90d": re.compile(r"\w*(?:positive|returned|refunded)\w*", re.IGNORECASE),
    "return_or_refund_rate_90d": re.compile(r"\w*rate\w*", re.IGNORECASE),
}
ASK_JSON = (
    " Reply with one JSON object with exactly these keys: "
    '{"labeled_count_90d": int, "positive_count_90d": int, "return_or_refund_rate_90d": float}.'
)


def value(raw: str) -> str:
    """Chuẩn hoá một số: '33.3%' → '0.333', '0,25' → '0.25', '7' → '7'."""
    number = float(raw.rstrip("%").replace(",", "."))
    if raw.endswith("%"):
        number /= 100
    return f"{number:.3f}".rstrip("0").rstrip(".")


def numbers(text: str) -> set[str]:
    """Mọi số trong câu, đã chuẩn hoá."""
    return {value(raw) for raw in NUMBER.findall(SKU_ID.sub(" ", text))}  # "sku-00002" không phải 2


def expected(features: dict) -> dict[str, str] | None:
    """Ba con số agent phải nói. None khi SKU chưa có lịch sử (không chấm được bằng số)."""
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
    """Tỷ lệ đạt khi có một số lệch ≤ 0,005 (agent hay làm tròn 2 chữ số hoặc nói phần trăm)."""
    return any(abs(float(x) - float(target)) <= 0.005 for x in found if float(x) <= 1)


def from_json(answer: str) -> dict[str, set[str]] | None:
    """Đọc 'phiếu có ô': object JSON đầu tiên có đủ ba trường. None khi agent không trả JSON."""
    for chunk in re.findall(r"\{[^{}]*\}", answer):
        try:
            data = json.loads(chunk)
            if isinstance(data, dict) and set(FIELDS) <= data.keys():
                return {field: {value(str(data[field]))} for field in FIELDS}
        except ValueError:  # JSON hỏng hoặc ô không phải số ⇒ thử chunk sau, rồi về câu văn
            continue
    return None


def from_text(answer: str) -> dict[str, set[str]]:
    """Đọc câu văn: mỗi lần nhắc tên trường lấy số GẦN nó nhất trong cùng mệnh đề (hoà ⇒ số đứng sau).

    Số không đứng cạnh tên trường nào thì không tính ⇒ câu đoán mò "10, 30, 0.333" trượt; câu tráo
    "labeled 10, positive 30" trượt; nhắc một trường hai lần với hai số khác nhau cũng trượt.
    """
    # Giới hạn: heuristic khoảng cách ký tự, câu lòng vòng có thể chấm sai; JSON là đường chính,
    # cột mode cho biết bao nhiêu câu đi đường này.
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
    """Chấm một câu trả lời: mỗi trường phải có đúng MỘT giá trị và giá trị đó đúng."""
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


# Agent 2 (model-agent), tool RAG: câu hỏi → con số phải có trong câu trả lời (đối chiếu rag/corpus/*.md).
POLICY_CASES = [
    ("How many days does a Shopee buyer have to request a return after delivery?", "15"),
    ("What is the TikTok Shop return window for most categories, in days?", "15"),
    ("How many days does an Amazon seller have to inspect returned items?", "3"),
    ("How many days does a Shopee seller have to answer a return dispute?", "3"),
    ("How many days do Walmart buyers have to return an item after delivery?", "30"),
]
ABSTAIN = re.compile(r"abstain|too little|insufficient|not enough", re.IGNORECASE)


def score_policy(answer: str, want: str) -> dict:
    """Câu hỏi chính sách đạt khi con số đúng có trong câu trả lời (số trong mã SKU đã loại)."""
    found = numbers(WINDOW.sub(lambda m: m.group().split()[0], answer))
    return {"gradable": True, "pass": want in found, "mode": "policy", "expected": want}


def score_predict(answer: str, truth: dict) -> dict:
    """Dự đoán đạt khi score lệch ≤ 0,005 so với Feature API /predict (hoặc cùng abstain)."""
    if truth.get("abstain") or truth.get("score") is None:
        return {"gradable": True, "pass": bool(ABSTAIN.search(answer)), "mode": "predict"}
    want = str(truth["score"])  # chưa làm tròn: agent hay làm tròn 2 chữ số
    return {
        "gradable": True,
        "pass": rate_close(numbers(answer), want),
        "mode": "predict",
        "expected": want,
    }


def texts(node) -> list[str]:
    """Gom mọi trường "text" trong kết quả A2A (artifacts/parts/status.message), theo thứ tự."""
    if isinstance(node, dict):
        own = [node["text"]] if isinstance(node.get("text"), str) else []
        return own + [t for v in node.values() for t in texts(v)]
    if isinstance(node, list):
        return [t for v in node for t in texts(v)]
    return []


def post(url: str, body: dict, headers: dict, context=None, timeout=180) -> dict:
    """POST JSON, trả JSON."""
    req = urllib.request.Request(
        url, json.dumps(body).encode(), {"Content-Type": "application/json", **headers}
    )
    return json.load(urllib.request.urlopen(req, timeout=timeout, context=context))


def main(argv=None) -> int:
    """Hỏi agent từng SKU, chấm, ghi JSONL, trả mã 1 nếu độ chính xác < ngưỡng."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--agent", required=True, help="tên route A2A: data-agent, data-agent-b, data-agent-ab"
    )
    parser.add_argument("--skus", default="", help="bắt buộc với --task features/predict")
    parser.add_argument(
        "--task",
        choices=("features", "predict", "policy"),
        default="features",
        help="features: Agent 1; predict + policy: hai tool của Agent 2 (model-agent)",
    )
    parser.add_argument("--gateway", default="http://localhost:13000")
    parser.add_argument("--feature-api", default="https://api.172-25-0-2.sslip.io:30443")
    parser.add_argument("--ca", default="out/ingress/platform-local-ca.crt")
    parser.add_argument("--min-accuracy", type=float, default=0.6)
    parser.add_argument("--out", default="out/agent-eval.jsonl")
    parser.add_argument(
        "--timeout", type=int, default=180, help="giây chờ mỗi câu hỏi agent (CPU chậm: tăng)"
    )
    args = parser.parse_args(argv)
    key = {"Authorization": f"Bearer {os.environ['AGW_AGENT_KEY']}"}
    context = (
        ssl.create_default_context(cafile=args.ca)
        if args.feature_api.startswith("https://")
        else None
    )
    skus = [x for x in args.skus.split(",") if x]
    jobs = []  # (nhãn, câu hỏi, hàm chấm)
    if args.task == "policy":
        jobs = [(q, q, lambda a, w=w: score_policy(a, w)) for q, w in POLICY_CASES]
    else:
        if not skus:
            parser.error("--skus bắt buộc với --task features/predict")
        if args.task == "features":
            truth = post(f"{args.feature_api}/features", {"seller_skus": skus}, {}, context)["features"]
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
    rows = []
    timeouts = 0  # quá hạn liên tiếp
    for label, question, grade in jobs:
        if timeouts >= 2:
            # Agent vẫn xử lý câu quá hạn ở nền: hỏi tiếp chỉ chất thêm phiên mồ côi lên LLM (vòng xoáy 04/11 và 06/11).
            # Câu bỏ dở tính là trượt (accuracy vẫn chia cho cả bộ).
            rows.append({"agent": args.agent, "sku": label, "seconds": 0, "answer": "SKIPPED after 2 timeouts", "gradable": True, "pass": False, "mode": "skipped"})
            continue
        started = time.perf_counter()
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
            # Chỉ câu trả lời cuối (artifacts): history chứa câu hỏi echo và tool-call, làm nhiễu phép chấm.
            answer = " ".join(texts(result.get("artifacts") or result))
        except (
            urllib.error.URLError,
            TimeoutError,
        ) as error:  # agent hỏng = câu trượt, không dừng harness
            answer = f"ERROR {error}"
        timeouts = timeouts + 1 if answer.startswith("ERROR") else 0
        rows.append(
            {
                "agent": args.agent,
                "sku": label,
                "seconds": round(time.perf_counter() - started, 2),
                "answer": answer[-600:],
                **grade(answer),
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
