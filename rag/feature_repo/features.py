"""RAG feature view: each return policy document chunk is one entity, with a 384-dim embedding.

The `dags/ml_rag_ingest.py` pipeline writes offline Parquet, then pushes it to the online store with
`write_to_online_store`; the Agent searches with `retrieve_online_documents_v2` (cosine).
"""

import os
from datetime import timedelta

from feast import Entity, FeatureView, Field, FileSource, ValueType
from feast.types import Array, Float32, String

chunk = Entity(
    name="chunk",
    join_keys=["chunk_id"],
    value_type=ValueType.STRING,
    description="One document chunk",
)

policy_chunks = FeatureView(
    name="policy_chunks",
    entities=[chunk],
    ttl=timedelta(0),  # documents do not expire; a new version overwrites by chunk_id
    schema=[
        Field(
            name="embedding",
            dtype=Array(Float32),
            vector_index=True,
            vector_length=384,
            vector_search_metric="cosine",
        ),
        Field(name="text", dtype=String),
        Field(name="doc_id", dtype=String),
        Field(name="heading", dtype=String),
    ],
    source=FileSource(
        name="policy_chunks_source",
        path=os.environ.get("FEAST_RAG_CHUNKS_PATH", "s3://platform/rag/chunks.parquet"),
        timestamp_field="event_timestamp",
        s3_endpoint_override=os.environ.get("FEAST_S3_ENDPOINT_URL"),
    ),
)
