"""Print a markdown table comparing llm-d-benchmark (inference-perf) runs, one directory per configuration.

python3 scripts/evidence/compare_llmd_runs.py out/llmd-benchmark/baseline out/llmd-benchmark/optimized
"""

import json
import sys
from pathlib import Path


def stages(run_dir):
    """Read the report of each load level (stage_N_lifecycle_metrics.json) in one run, ordered by N."""
    files = sorted(
        Path(run_dir).rglob("stage_*_lifecycle_metrics.json"),
        key=lambda p: int(p.name.split("_")[1]),
    )
    return [json.loads(f.read_text()) for f in files]


def row(label, s):
    """One markdown table row for one load level: OK/failed, TTFT, TPOT, request latency, throughput."""
    ok, lat = s["successes"], s["successes"]["latency"]
    ttft, tpot, req = (
        lat["time_to_first_token"],
        lat["time_per_output_token"],
        lat["request_latency"],
    )
    return (
        f"| {label} | {s['load_summary']['requested_rate']:g} | {ok['count']} / {s['failures']['count']} "
        f"| {ttft['median']:.2f} / {ttft['p90']:.2f} | {tpot['median'] * 1000:.0f} "
        f"| {req['median']:.2f} / {req['p90']:.2f} | {ok['throughput']['requests_per_sec']:.3f} "
        f"| {ok['throughput']['output_tokens_per_sec']:.2f} |"
    )


if __name__ == "__main__":
    print(
        "| Configuration | Load (req/s) | OK / failed | TTFT p50 / p90 (s) | TPOT p50 (ms) | Request p50 / p90 (s) | Achieved (req/s) | Output tokens/s |"
    )
    print("|---|---:|---:|---:|---:|---:|---:|---:|")
    for d in sys.argv[1:]:
        for s in stages(d):
            print(row(Path(d).name, s))
