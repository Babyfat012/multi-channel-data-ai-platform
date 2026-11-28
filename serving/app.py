"""Entry point for the serving pods: one image, the API_SURFACE env picks which service a pod runs.

  features  Feature API  (serving/features.py)   SKU features from Feast/Redis
  policy    Policy API   (serving/policy.py)     policy chunks from Feast RAG/pgvector
  mcp       MCP server   (serving/mcp_server.py) agent tools, a thin client over the two APIs + KServe
  all       every route in one process (local runs and tests)

Run: `uvicorn serving.app:app` from the repo root. Features need `FEAST_REPO_PATH` (feature_store/) plus the variables
`feature_store.yaml` reads (FEAST_REGISTRY, FEAST_REDIS); policy needs `FEAST_RAG_REGISTRY` (+ RAG_PG_*).
"""

import os
import time
from contextlib import AsyncExitStack, asynccontextmanager

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import Response
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest

from serving import features, policy
from serving.mcp_server import build_mcp

SURFACES = {
    "features": {"features"},
    "policy": {"policy"},
    "mcp": {"mcp"},
    "all": {"features", "policy", "mcp"},
}
TITLES = {
    "features": "Feature API",
    "policy": "Policy API",
    "mcp": "MCP server",
    "all": "Serving API",
}

# Web API metrics: Prometheus scrapes /metrics; `handler` is the path template to avoid label explosion.
REQUESTS = Counter("http_requests_total", "Number of requests", ["method", "handler", "status"])
LATENCY = Histogram("http_request_duration_seconds", "Processing time", ["handler"])


def create_app(
    store_factory=features.load_store,
    rag_factory=policy.load_rag,
    http_factory=httpx.AsyncClient,
    surface=None,
):
    """`store_factory`, `rag_factory`, `http_factory` let tests swap in fakes; each pod loads only what it serves."""
    surface = surface or os.environ.get("API_SURFACE", "all")
    if surface not in SURFACES:
        raise ValueError(f"API_SURFACE must be {'|'.join(SURFACES)}, got {surface!r}")
    parts = SURFACES[surface]
    mcp = None

    @asynccontextmanager
    async def lifespan(app):
        """Open only the dependencies of the services this pod runs."""
        app.state.store = store_factory() if "features" in parts else None
        app.state.rag = rag_factory() if "policy" in parts else None
        async with AsyncExitStack() as stack:
            if mcp is not None:
                app.state.http = await stack.enter_async_context(http_factory())
                await stack.enter_async_context(mcp.session_manager.run())
            yield

    app = FastAPI(title=TITLES[surface], lifespan=lifespan)
    if "features" in parts:
        app.include_router(features.router)
    if "policy" in parts:
        app.include_router(policy.router)
    if "mcp" in parts:
        mcp = build_mcp(app.state)
        app.mount(
            "/mcp",
            mcp.streamable_http_app(
                streamable_http_path="/", stateless_http=True, json_response=True, host="0.0.0.0"
            ),
        )

    @app.middleware("http")
    async def metrics(request: Request, call_next):
        """Record request count, status and latency per handler path."""
        start = time.perf_counter()
        status = 500
        try:
            response = await call_next(request)
            status = response.status_code
            return response
        finally:
            # The MCP app is mounted ⇒ its inner route is "/"; group under "/mcp" for readability.
            route = request.scope.get("route")
            handler = (
                "/mcp" if request.url.path.startswith("/mcp") else getattr(route, "path", "other")
            )
            REQUESTS.labels(request.method, handler, str(status)).inc()
            LATENCY.labels(handler).observe(time.perf_counter() - start)

    @app.get("/metrics", include_in_schema=False)
    async def metrics_endpoint():
        """Expose metrics in Prometheus format."""
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    @app.get("/healthz")
    async def healthz():
        """Liveness for k8s: the process is alive."""
        return {"status": "ok"}

    @app.get("/readyz")
    async def readyz(request: Request):
        """Readiness for k8s: the stores this pod serves are loaded (the MCP server only needs to be up)."""
        if "features" in parts and request.app.state.store is None:
            raise HTTPException(503, "feature store not loaded")
        if surface == "policy" and request.app.state.rag is None:
            raise HTTPException(503, "rag not configured")
        return {"status": "ready"}

    return app


app = create_app()
