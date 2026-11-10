"""RAG pipeline steps; the `ml_rag_ingest` DAG calls each step inside feast-venv.

upload   : text documents (rag/corpus) → s3://…/rag/raw/*.md               ("pull data from text" source)
chunk    : raw → s3://…/rag/chunks_text.parquet                            (rag.chunking.chunk_markdown)
embed    : chunks_text → s3://…/rag/chunks.parquet (+ 384-dim embedding)   (Feast offline source)
load     : feast apply + write_to_online_store → Postgres/pgvector          ("store embeddings via Feast")
validate : count, check vectors, try sample questions → receipt JSON (each check = violation count, 0 passes)
publish  : dataset + job + lineage + assertion to DataHub over REST (no SDK needed)

Configured through environment variables (the DAG reads them from Airflow Connection/Variable): RAG_ROOT (s3://bucket/rag),
FEAST_S3_ENDPOINT_URL, AWS_*, FEAST_RAG_REGISTRY, RAG_PG_*.
"""

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

from rag.chunking import MODEL, chunk_markdown, embed

FEATURE_VIEW = "policy_chunks"
REPO = Path(__file__).resolve().parent / "feature_repo"
DAG_ID = "ml_rag_ingest"
# Sample question → the document that must rank first in search; catches wrong embeddings/indexes.
PROBES = {
    "How many days does a buyer have to return an item on Walmart?": "walmart-returns",
    "Can a TikTok Shop buyer get a refund without sending the item back?": "tiktok-shop-returns",
    "What should I do when a SKU return rate is above 30 percent?": "seller-return-playbook",
}


def _fs():
    """Create an S3 client using the configured endpoint for RAG data."""
    import s3fs

    return s3fs.S3FileSystem(endpoint_url=os.environ.get("FEAST_S3_ENDPOINT_URL"))


def _root():
    """Get the RAG root path on S3, without the protocol prefix and trailing slash."""
    return os.environ["RAG_ROOT"].removeprefix("s3://").rstrip("/")


def upload(corpus):
    """Upload the Markdown documents in the corpus to the raw area on S3."""
    fs = _fs()
    files = sorted(Path(corpus).glob("*.md"))
    for path in files:
        fs.put_file(str(path), f"{_root()}/raw/{path.name}")
    print(f"uploaded {len(files)} documents to s3://{_root()}/raw/")


def chunk():
    """Split raw documents into chunks and write a Parquet with the text to S3."""
    import pandas as pd

    fs = _fs()
    rows = []
    for key in sorted(fs.glob(f"{_root()}/raw/*.md")):
        with fs.open(key, "r") as f:
            rows += chunk_markdown(Path(key).stem, f.read())
    frame = pd.DataFrame(rows)
    with fs.open(f"{_root()}/chunks_text.parquet", "wb") as f:
        frame.to_parquet(f, index=False)
    print(f"{len(frame)} chunks from {frame['doc_id'].nunique()} documents")


def embed_step(cache_dir):
    """Create embeddings for the chunks and write a Parquet with the event timestamp to S3."""
    import pandas as pd

    fs = _fs()
    with fs.open(f"{_root()}/chunks_text.parquet", "rb") as f:
        frame = pd.read_parquet(f)
    frame["embedding"] = embed(frame["text"], cache_dir=cache_dir)
    frame["event_timestamp"] = pd.Timestamp.now(tz="UTC")
    with fs.open(f"{_root()}/chunks.parquet", "wb") as f:
        frame.to_parquet(f, index=False)
    print(f"embedded {len(frame)} chunks with {MODEL}")


def _store():
    """FeatureStore on a copy of the repo (the mounted directory is read-only, Feast needs to write temp files)."""
    from feast import FeatureStore

    tmp = tempfile.mkdtemp()
    return FeatureStore(shutil.copytree(REPO, f"{tmp}/repo"))


def load():
    """Apply the Feast definitions and write the chunks to the online store."""
    import pandas as pd

    fs = _fs()
    with fs.open(f"{_root()}/chunks.parquet", "rb") as f:
        frame = pd.read_parquet(f)
    store = _store()
    # CLI from the same venv: create the pgvector table + write definitions to the registry.
    feast = Path(sys.executable).parent / "feast"
    subprocess.run([str(feast), "apply"], cwd=store.repo_path, check=True)
    store = _store()
    store.write_to_online_store(FEATURE_VIEW, frame)
    print(f"wrote {len(frame)} chunks to online store ({FEATURE_VIEW})")


def search(store, question, top_k=3, cache_dir=None):
    """Find the top_k chunks closest to the question; shared by validate and the Feature API."""
    vector = embed([question], cache_dir=cache_dir)[0]
    response = store.retrieve_online_documents_v2(
        features=[f"{FEATURE_VIEW}:{n}" for n in ("text", "doc_id", "heading", "embedding")],
        query=vector,
        top_k=top_k,
        distance_metric="cosine",
    ).to_dict()
    # Feast's SQL picks the right top_k but returns them unordered ⇒ re-sort here (closest first).
    rows = [
        {"chunk_id": c, "doc_id": d, "heading": h, "text": t, "distance": dist}
        for c, d, h, t, dist in zip(
            response["chunk_id"],
            response["doc_id"],
            response["heading"],
            response["text"],
            response["distance"],
            strict=True,
        )
    ]
    return sorted(rows, key=lambda r: r["distance"])


def get_chunks(store, chunk_ids):
    """Look up chunks by id in the online store (no embedding needed); unknown id ⇒ text None."""
    response = store.get_online_features(
        features=[f"{FEATURE_VIEW}:{n}" for n in ("text", "doc_id", "heading")],
        entity_rows=[{"chunk_id": c} for c in chunk_ids],
    ).to_dict()
    return [
        {"chunk_id": c, "doc_id": d, "heading": h, "text": t}
        for c, d, h, t in zip(
            response["chunk_id"],
            response["doc_id"],
            response["heading"],
            response["text"],
            strict=True,
        )
    ]


def validate(receipt_dir, run_id, cache_dir):
    """Check chunk count, vectors and sample queries, then write a pass or fail receipt."""
    import pandas as pd
    import psycopg

    fs = _fs()
    with fs.open(f"{_root()}/chunks.parquet", "rb") as f:
        frame = pd.read_parquet(f)
    store = _store()
    table = f"{store.project}_{FEATURE_VIEW}"
    with psycopg.connect(
        host=os.environ["RAG_PG_HOST"],
        port=os.environ["RAG_PG_PORT"],
        dbname="rag",
        user=os.environ["RAG_PG_USER"],
        password=os.environ["RAG_PG_PASSWORD"],
    ) as conn:
        online = conn.execute(f'SELECT count(DISTINCT entity_key) FROM "{table}"').fetchone()[0]
    norms = frame["embedding"].map(lambda v: sum(x * x for x in v) ** 0.5)
    top = {q: search(store, q, top_k=1, cache_dir=cache_dir)[0]["doc_id"] for q in PROBES}
    checks = {
        "online_chunk_count_mismatch": abs(int(online) - len(frame)),
        "duplicate_chunk_ids": int(frame["chunk_id"].duplicated().sum()),
        "empty_text": int((frame["text"].str.len() == 0).sum()),
        "wrong_dimension": int((frame["embedding"].map(len) != 384).sum()),
        "not_unit_norm": int(((norms - 1).abs() > 1e-3).sum()),
        "probe_wrong_top_doc": sum(top[q] != doc for q, doc in PROBES.items()),
    }
    receipt = {
        "run_id": run_id,
        "completed_at": datetime.now(timezone.utc).isoformat(),
        "chunks": len(frame),
        "online_rows": int(online),
        "probe_top_doc": top,
        "checks": checks,
        "status": "PASS" if not any(checks.values()) else "FAIL",
    }
    Path(receipt_dir).mkdir(parents=True, exist_ok=True)
    path = Path(receipt_dir) / "validate.json"
    path.write_text(json.dumps(receipt, indent=2, ensure_ascii=False))
    print(json.dumps(receipt, indent=2, ensure_ascii=False))
    if receipt["status"] != "PASS":
        raise SystemExit(f"validate FAIL: {checks}")


def _mcp(entity_type, urn, aspect, value):
    """Build a proposal that updates one aspect of a DataHub entity."""
    return {
        "entityType": entity_type,
        "entityUrn": urn,
        "changeType": "UPSERT",
        "aspectName": aspect,
        "aspect": {
            # Rest.li reads `value` as bytes (each character ≤ U+00FF): raw "→"/Vietnamese text ⇒ HTTP 400 ⇒ escape \uXXXX.
            "value": json.dumps(value),
            "contentType": "application/json",
        },
    }


def lineage_mcps(bucket_root, receipt):
    """Datasets (S3 raw, Parquet, Postgres online table), jobs per DAG task, lineage edges, assertions."""

    def ds(platform, name):
        """Build a DataHub dataset URN from the platform and dataset name."""
        return f"urn:li:dataset:(urn:li:dataPlatform:{platform},{name},PROD)"

    flow = f"urn:li:dataFlow:(airflow,{DAG_ID},prod)"
    corpus = ds("file", "platform/rag/corpus")
    raw = ds("s3", f"{bucket_root}/raw")
    text = ds("s3", f"{bucket_root}/chunks_text.parquet")
    vectors = ds("s3", f"{bucket_root}/chunks.parquet")
    online = ds("postgres", f"rag.public.platform_rag_{FEATURE_VIEW}")
    jobs = {
        "upload": ([corpus], [raw]),
        "chunk": ([raw], [text]),
        "embed": ([text], [vectors]),
        "load": ([vectors], [online]),
        "validate": ([vectors, online], []),
    }
    out = [
        _mcp(
            "dataFlow",
            flow,
            "dataFlowInfo",
            {"name": DAG_ID, "description": "RAG: text → chunk → embedding → Feast (pgvector)"},
        )
    ]
    descriptions = {
        corpus: "Return policy documents (simulated) in the repo at rag/corpus",
        raw: "Source documents on MinIO",
        text: "Chunk text (no embedding yet)",
        vectors: f"Chunk + embedding {MODEL}; offline source of Feast platform_rag",
        online: "Feast online store (pgvector), feature view policy_chunks",
    }
    for urn, description in descriptions.items():
        out.append(
            _mcp(
                "dataset",
                urn,
                "datasetProperties",
                {"description": description, "customProperties": {"pipeline": DAG_ID}},
            )
        )
    previous = None
    for task, (inputs, outputs) in jobs.items():
        urn = f"urn:li:dataJob:({flow},{task})"
        out.append(
            _mcp(
                "dataJob",
                urn,
                "dataJobInfo",
                # `type` is a union (AzkabanJobType | string) ⇒ Rest.li needs {"string": ...}, not a plain string (422).
                {"name": task, "type": {"string": "COMMAND"}, "flowUrn": flow},
            )
        )
        out.append(
            _mcp(
                "dataJob",
                urn,
                "dataJobInputOutput",
                {
                    "inputDatasets": inputs,
                    "outputDatasets": outputs,
                    "inputDatajobs": [previous] if previous else [],
                },
            )
        )
        previous = urn
        for target in outputs:
            out.append(
                _mcp(
                    "dataset",
                    target,
                    "upstreamLineage",
                    {"upstreams": [{"dataset": s, "type": "TRANSFORMED"} for s in inputs]},
                )
            )
    ts = int(datetime.fromisoformat(receipt["completed_at"]).timestamp() * 1000)
    for check, value in sorted(receipt["checks"].items()):
        urn = (
            f"urn:li:assertion:platform-{DAG_ID}-{hashlib.sha256(check.encode()).hexdigest()[:12]}"
        )
        out.append(
            _mcp(
                "assertion",
                urn,
                "assertionInfo",
                {
                    "type": "CUSTOM",
                    "description": f"validate: {check} = 0",
                    "customAssertion": {
                        "type": "PLATFORM RAG validate",
                        "entity": online,
                        "logic": f"{check} == 0",
                    },
                },
            )
        )
        out.append(
            _mcp(
                "assertion",
                urn,
                "assertionRunEvent",
                {
                    "timestampMillis": ts,
                    "runId": receipt["run_id"],
                    "asserteeUrn": online,
                    "assertionUrn": urn,
                    "status": "COMPLETE",
                    "result": {
                        "type": "SUCCESS" if value == 0 else "FAILURE",
                        "nativeResults": {"value": str(value)},
                    },
                },
            )
        )
    return out


def publish(receipt_dir, server):
    """Send RAG datasets, jobs, lineage and assertions to DataHub from the validation receipt."""
    receipt = json.loads((Path(receipt_dir) / "validate.json").read_text())
    proposals = lineage_mcps(_root(), receipt)
    for proposal in proposals:
        request = urllib.request.Request(
            f"{server.rstrip('/')}/aspects?action=ingestProposal",
            data=json.dumps({"proposal": proposal}).encode(),
            headers={"Content-Type": "application/json", "X-RestLi-Protocol-Version": "2.0.0"},
        )
        urllib.request.urlopen(request, timeout=30).read()
    print(f"sent {len(proposals)} MCP to {server}")


def main():
    """Parse command-line arguments and run the selected RAG pipeline step."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("step", choices=["upload", "chunk", "embed", "load", "validate", "publish"])
    parser.add_argument("--corpus", default=str(Path(__file__).resolve().parent / "corpus"))
    parser.add_argument("--receipt-dir", default="/tmp/rag-receipts")
    parser.add_argument("--run-id", default="manual")
    parser.add_argument("--model-cache", default=None, help="fastembed model cache directory")
    parser.add_argument("--datahub", default=os.environ.get("DATAHUB_GMS_URL", ""))
    args = parser.parse_args()
    {
        "upload": lambda: upload(args.corpus),
        "chunk": chunk,
        "embed": lambda: embed_step(args.model_cache),
        "load": load,
        "validate": lambda: validate(args.receipt_dir, args.run_id, args.model_cache),
        "publish": lambda: publish(args.receipt_dir, args.datahub),
    }[args.step]()


if __name__ == "__main__":
    main()
