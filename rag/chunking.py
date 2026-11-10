"""Split Markdown documents into chunks for RAG; pure Python so tests need no model or Feast.

Each `## ` section is one chunk (with the document title); a section longer than `max_chars` is split by
sentence with an `overlap`-character overlap. `chunk_id` is a hash of the content ⇒ reruns give the same id (overwrite, no duplicates).
"""

import functools
import hashlib
import re

MODEL = "BAAI/bge-small-en-v1.5"  # 384 dims, ONNX on CPU via fastembed


def _split_long(text, max_chars, overlap):
    """Group sentences into parts ≤ max_chars; each part repeats the tail of the previous one to keep context."""
    sentences = re.split(r"(?<=[.!?])\s+|\n(?=- )", text)
    parts, current = [], ""
    for sentence in sentences:
        if current and len(current) + len(sentence) + 1 > max_chars:
            parts.append(current)
            current = current[-overlap:] + " " + sentence if overlap else sentence
        else:
            current = f"{current} {sentence}".strip()
    if current:
        parts.append(current)
    return parts


def chunk_markdown(doc_id, markdown, max_chars=700, overlap=100):
    """Return a list of dicts {chunk_id, doc_id, heading, text}; drop HTML comments and empty sections."""
    markdown = re.sub(r"<!--.*?-->", "", markdown, flags=re.S)
    title_match = re.search(r"^# (.+)$", markdown, flags=re.M)
    title = title_match.group(1).strip() if title_match else doc_id
    chunks = []
    for section in re.split(r"^## ", markdown, flags=re.M)[1:]:
        heading, _, body = section.partition("\n")
        body = " ".join(body.split())
        if not body:
            continue
        for part in _split_long(body, max_chars, overlap):
            text = f"{title} — {heading.strip()}: {part}"
            chunk_id = hashlib.sha256(f"{doc_id}\n{text}".encode()).hexdigest()[:16]
            chunks.append(
                {"chunk_id": chunk_id, "doc_id": doc_id, "heading": heading.strip(), "text": text}
            )
    return chunks


@functools.lru_cache(maxsize=2)
def _model(model_name, cache_dir):
    """Load and cache the embedding model by name and model directory."""
    from fastembed import TextEmbedding

    return TextEmbedding(model_name, cache_dir=cache_dir)


def embed(texts, model_name=MODEL, cache_dir=None):
    """Normalized embeddings (for cosine); the model loads once per process, fastembed is imported only when needed."""
    return [vector.tolist() for vector in _model(model_name, cache_dir).embed(list(texts))]
