"""`search` must refuse an index built with another embedding model: same dims, meaningless distances."""

from types import SimpleNamespace

import pytest

from rag import pipeline


class FakeStore:
    """Online store answering one chunk embedded with `model`."""

    def __init__(self, model):
        """Model name the fake index was built with."""
        self.model = model

    def retrieve_online_documents_v2(self, **kwargs):
        """Same shape as Feast: an object with to_dict()."""
        row = {
            "chunk_id": ["c1"],
            "doc_id": ["walmart-returns"],
            "heading": ["Window"],
            "text": ["30 days"],
            "distance": [0.1],
            "embedding_model": [self.model],
        }
        return SimpleNamespace(to_dict=lambda: row)


@pytest.fixture(autouse=True)
def no_model(monkeypatch):
    """Skip the real embedding model."""
    monkeypatch.setattr(pipeline, "embed", lambda texts, cache_dir=None: [[0.0] * 384])


def test_search_returns_chunks_from_the_same_model():
    """Index and query share the model ⇒ normal result."""
    rows = pipeline.search(FakeStore(pipeline.MODEL), "return window?")
    assert [r["doc_id"] for r in rows] == ["walmart-returns"]


@pytest.mark.parametrize("model", ["sentence-transformers/all-MiniLM-L6-v2", None])
def test_search_rejects_another_or_unknown_model(model):
    """Another model, or an old index without the column, must fail loudly."""
    with pytest.raises(RuntimeError, match="re-run the ml_rag_ingest DAG"):
        pipeline.search(FakeStore(model), "return window?")
