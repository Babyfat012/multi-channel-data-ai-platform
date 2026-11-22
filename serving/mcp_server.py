"""MCP server: the tools agents call, as a thin client over the API services.

It owns no data: features come from the Feature API, policy chunks from the Policy API, risk scores from KServe.
Inputs are checked with the same schemas the APIs use, so a bad call fails fast with a message the LLM can act on.

URLs (env): FEATURE_API_URL, POLICY_API_URL (default http://localhost:8000, i.e. the same process in `all` mode),
KSERVE_URL (KServe protocol v1 predict endpoint).
"""

import functools
import os

import httpx
from mcp.server.mcpserver import MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from prometheus_client import Counter
from pydantic import BaseModel, ValidationError

from serving.schemas import (
    ChunkIdRequest,
    ChunkRequest,
    ChunkResponse,
    FeatureRequest,
    FeatureResponse,
    Prediction,
)

# Agent telemetry: every MCP tool call, split into success / error. MCP errors are still HTTP 200
# (isError in JSON-RPC), so http_requests_total cannot count them.
TOOL_CALLS = Counter("mcp_tool_calls_total", "Number of MCP tool calls", ["tool", "outcome"])
TIMEOUT = 10  # seconds per upstream call


def counted(tool):
    """Wrap an MCP tool to count into TOOL_CALLS; keeps the signature (MCP builds the schema from it)."""

    @functools.wraps(tool)
    async def wrapper(*args, **kwargs):
        """Count the MCP tool call as success or error, then return the tool's result."""
        try:
            result = await tool(*args, **kwargs)
        except Exception:
            TOOL_CALLS.labels(tool.__name__, "error").inc()
            raise
        TOOL_CALLS.labels(tool.__name__, "ok").inc()
        return result

    return wrapper


def service_url(name: str) -> str:
    """Base URL of an API service; `all` mode serves every route in one process on :8000."""
    return os.environ.get(name, "http://localhost:8000").rstrip("/")


async def call(http: httpx.AsyncClient, service: str, url: str | None, body: dict) -> dict:
    """POST to an upstream service; any failure becomes a ToolError the LLM can read.

    4xx: the input was wrong; 5xx: the upstream is down. Both keep the upstream message so the agent can tell why.
    Timeouts, connection errors and non-JSON bodies become "<service> unavailable".
    """
    if not url:
        raise ToolError(f"{service} not configured")
    try:
        response = await http.post(url, json=body, timeout=TIMEOUT)
    except httpx.HTTPError as exc:
        raise ToolError(f"{service} unavailable") from exc
    if 400 <= response.status_code < 500:
        raise ToolError(f"{service} rejected the input: {error_message(response)}")
    if response.status_code >= 300:
        raise ToolError(f"{service} unavailable: {error_message(response)}")
    try:
        return response.json()
    except ValueError as exc:
        raise ToolError(f"{service} unavailable") from exc


def error_message(response: httpx.Response) -> str:
    """FastAPI puts errors in `detail`, KServe in `error`; fall back to the raw text."""
    try:
        body = response.json()
    except ValueError:
        return response.text[:200]
    detail = body.get("detail") or body.get("error") or body
    return detail if isinstance(detail, str) else str(detail)[:200]


def checked(model: type[BaseModel], label: str, **fields) -> BaseModel:
    """Validate tool arguments with the API's own schema; the first error goes back to the LLM."""
    try:
        return model(**fields)
    except ValidationError as exc:
        raise ToolError(f"invalid {label}: {exc.errors()[0]['msg']}") from exc


def build_mcp(state) -> MCPServer:
    """MCP server whose tools use `state.http` (an httpx.AsyncClient set by the app lifespan)."""
    # stateless: any MCP request can reach any replica, no sticky session needed.
    mcp = MCPServer(
        "platform-features",
        instructions=(
            "Look up return rate features by SKU, score Return risk for new orders"
            " and search return policy chunks."
        ),
    )

    @mcp.tool()
    @counted
    async def get_sku_features(seller_skus: list[str]) -> FeatureResponse:
        # Docstring = the tool description the LLM sees ⇒ English: a Vietnamese description made Qwen 1.5B not call the tool (0/3 vs 3/3).
        """Return features and per-group availability for 1-100 SKUs (e.g. APP-00042).

        Use only ready data. Missing values are null, never assumed zero; unavailable groups
        must be reported as unavailable. insufficient_history means confirmed zero labeled samples.
        """
        body = checked(FeatureRequest, "seller_skus", seller_skus=seller_skus)
        url = f"{service_url('FEATURE_API_URL')}/features"
        return FeatureResponse(**await call(state.http, "feature api", url, body.model_dump()))

    @mcp.tool()
    @counted
    async def search_policy_chunks(query: str, top_k: int = 3) -> ChunkResponse:
        """Search return/refund policy text of Amazon, Walmart, Shopify, TikTok Shop, Shopee and the
        seller playbook. Use for ANY policy, return window or refund question. Returns top_k (1-10) chunks."""
        body = checked(ChunkRequest, "input", query=query, top_k=top_k)
        url = f"{service_url('POLICY_API_URL')}/chunks"
        return ChunkResponse(**await call(state.http, "policy api", url, body.model_dump()))

    @mcp.tool()
    @counted
    async def get_policy_chunks(chunk_ids: list[str]) -> ChunkResponse:
        """Get the text of 1-20 policy chunks by chunk_id (16 hex characters)."""
        body = checked(ChunkIdRequest, "chunk_ids", chunk_ids=chunk_ids)
        url = f"{service_url('POLICY_API_URL')}/chunks/by-id"
        return ChunkResponse(**await call(state.http, "policy api", url, body.model_dump()))

    @mcp.tool()
    @counted
    async def predict_return_risk(
        seller_sku: str,
        channel: str,
        ordered_basis_qty: int,
        ordered_product_net_minor: int,
        country: str,
    ) -> Prediction:
        """Return risk score (0-1, higher = riskier, for ranking) of one new order line.

        channel: amazon|walmart|shopify|tiktok_shop|shopee; country: ISO-2 ship-to country (e.g. US);
        ordered_product_net_minor: price in cents. abstain=true when the SKU has too little labeled history.
        """
        # KServe validates the order line itself (ml/serve.py): it is the trust boundary for /predict.
        instance = {
            "seller_sku": seller_sku,
            "channel": channel,
            "country": country,
            "ordered_basis_qty": ordered_basis_qty,
            "ordered_product_net_minor": ordered_product_net_minor,
        }
        payload = await call(
            state.http, "model", os.environ.get("KSERVE_URL"), {"instances": [instance]}
        )
        try:
            return Prediction(**payload["predictions"][0])
        except (KeyError, IndexError, TypeError, ValidationError) as exc:
            raise ToolError("model unavailable") from exc

    return mcp
