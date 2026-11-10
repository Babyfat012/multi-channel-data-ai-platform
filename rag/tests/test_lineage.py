"""Tests for the RAG pipeline's DataHub MCPs: correct lineage chain and assertions from the receipt."""

import json

from rag.pipeline import lineage_mcps

RECEIPT = {
    "run_id": "r1",
    "completed_at": "2026-10-21T00:00:00+00:00",
    "checks": {"duplicate_chunk_ids": 0, "probe_wrong_top_doc": 1},
}


def by_aspect(mcps, aspect):
    return [
        (m["entityUrn"], json.loads(m["aspect"]["value"]))
        for m in mcps
        if m["aspectName"] == aspect
    ]


def test_lineage_chain_raw_to_online():
    mcps = lineage_mcps("platform/rag", RECEIPT)
    edges = {
        urn: [u["dataset"] for u in v["upstreams"]] for urn, v in by_aspect(mcps, "upstreamLineage")
    }
    online = (
        "urn:li:dataset:(urn:li:dataPlatform:postgres,rag.public.platform_rag_policy_chunks,PROD)"
    )
    vectors = "urn:li:dataset:(urn:li:dataPlatform:s3,platform/rag/chunks.parquet,PROD)"
    assert edges[online] == [vectors]
    assert len(edges) == 4  # raw, chunks_text, chunks, online


def test_jobs_follow_task_order():
    jobs = by_aspect(lineage_mcps("platform/rag", RECEIPT), "dataJobInputOutput")
    names = [urn.rsplit(",", 1)[1].rstrip(")") for urn, _ in jobs]
    assert names == ["upload", "chunk", "embed", "load", "validate"]
    assert jobs[0][1]["inputDatajobs"] == []
    assert jobs[1][1]["inputDatajobs"] == [jobs[0][0]]


def test_assertion_result_follows_check_value():
    runs = by_aspect(lineage_mcps("platform/rag", RECEIPT), "assertionRunEvent")
    results = sorted(v["result"]["type"] for _, v in runs)
    assert results == ["FAILURE", "SUCCESS"]


def test_aspect_values_are_latin1_bytes_for_restli():
    """DataHub GMS rejects values with characters > U+00FF ('is not a valid string representation of bytes')."""
    for mcp in lineage_mcps("platform/rag", RECEIPT):
        mcp["aspect"]["value"].encode("latin-1")


def test_datajob_type_is_a_restli_union_member():
    """DataJobInfo.type is a union ⇒ a plain string gets 422 from GMS 'union type is not backed by a DataMap'."""
    infos = by_aspect(lineage_mcps("platform/rag", RECEIPT), "dataJobInfo")
    assert infos and all(value["type"] == {"string": "COMMAND"} for _, value in infos)
