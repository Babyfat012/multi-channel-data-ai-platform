"""Policy API: return-policy chunks from the Feast RAG store (pgvector), by question or by chunk id.

Owns the RAG data: the only service that reads pgvector. Enabled only when `FEAST_RAG_REGISTRY` is set
(+ RAG_PG_*, see `rag/feature_store/feature_store.yaml`); otherwise every route answers 503.
"""

import asyncio
import os

from fastapi import APIRouter, HTTPException, Request

from serving.schemas import ChunkIdRequest, ChunkRequest, ChunkResponse, PolicyChunk

router = APIRouter()


def load_rag():
    """Return an object with .search(query, top_k) and .get(chunk_ids) over Feast RAG; None if not configured."""
    if not os.environ.get("FEAST_RAG_REGISTRY"):
        return None
    from types import SimpleNamespace

    from rag.pipeline import get_chunks, open_store, search

    store = open_store()
    cache = os.environ.get("RAG_MODEL_CACHE")
    return SimpleNamespace(
        search=lambda query, top_k: search(store, query, top_k=top_k, cache_dir=cache),
        get=lambda chunk_ids: get_chunks(store, chunk_ids),
    )


async def fetch_chunks(rag, method, *args):
    """Feast/embedding code is synchronous ⇒ run it in a thread so it does not block the event loop."""
    if rag is None:
        raise HTTPException(503, "rag not configured")
    try:
        rows = await asyncio.to_thread(getattr(rag, method), *args)
    except Exception as exc:
        raise HTTPException(503, "vector store unavailable") from exc
    return ChunkResponse(chunks=[PolicyChunk(**r) for r in rows])


@router.post("/chunks", response_model=ChunkResponse)
async def chunks(body: ChunkRequest, request: Request):
    """Find the policy chunks closest to the question in a REST request."""
    return await fetch_chunks(request.app.state.rag, "search", body.query, body.top_k)


@router.post("/chunks/by-id", response_model=ChunkResponse)
async def chunks_by_id(body: ChunkIdRequest, request: Request):
    """Look up policy chunks by id in a REST request."""
    return await fetch_chunks(request.app.state.rag, "get", body.chunk_ids)
