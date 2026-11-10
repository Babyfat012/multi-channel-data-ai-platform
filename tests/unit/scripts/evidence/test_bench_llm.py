"""bench_llm through agentgateway: with AGW_AGENT_KEY it sends a Bearer token, without it calls llama.cpp directly as before."""

import io
import json

from scripts.evidence import bench_llm


def _capture(monkeypatch):
    """Fake urlopen: keeps the request, returns minimal usage."""
    seen = []

    def fake_urlopen(request, timeout):
        """Record the headers, then return a body with usage."""
        seen.append(dict(request.header_items()))
        return io.BytesIO(
            json.dumps({"usage": {"completion_tokens": 3, "prompt_tokens": 9}}).encode()
        )

    monkeypatch.setattr(bench_llm.urllib.request, "urlopen", fake_urlopen)
    return seen


def test_gateway_key_is_sent_only_when_set(monkeypatch):
    """The key comes from the environment, not a command-line argument (not exposed in ps)."""
    seen = _capture(monkeypatch)
    monkeypatch.delenv("AGW_AGENT_KEY", raising=False)
    bench_llm.one("http://llm/v1/chat/completions", "platform-chat", 0, 8)
    monkeypatch.setenv("AGW_AGENT_KEY", "k-123")
    bench_llm.one("http://gw/v1/chat/completions", "platform-chat", 1, 8)
    assert "Authorization" not in seen[0]
    assert seen[1]["Authorization"] == "Bearer k-123"
