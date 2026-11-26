"""Smoke test parsing of the llm-d EPP metrics (lines as the EPP v1.0.0 printed them in the local POC)."""

from scripts.llm.smoke_llm_serving import epp_endpoints

METRICS = """# HELP llm_d_epp_per_endpoint_queue_size Per-endpoint queue size
llm_d_epp_per_endpoint_queue_size{model_server_endpoint="qwen-7b68957bc9-7zbw8-rank-0",name="epp"} 0
llm_d_epp_per_endpoint_queue_size{model_server_endpoint="qwen-7b68957bc9-q2xk4-rank-0",name="epp"} 3
llm_d_epp_average_queue_size{name="epp"} 1.5
"""


def test_counts_each_replica_once():
    """Two replicas tracked ⇒ two endpoints; other series are ignored."""
    assert epp_endpoints(METRICS) == {
        "qwen-7b68957bc9-7zbw8-rank-0",
        "qwen-7b68957bc9-q2xk4-rank-0",
    }


def test_no_series_means_no_replica():
    """An EPP that has not found any pod prints no per-endpoint series."""
    assert epp_endpoints('llm_d_epp_average_queue_size{name="epp"} 0\n') == set()
