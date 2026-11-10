"""Chunking tests: pure Python, run with the repo's pytest."""

from pathlib import Path

from rag.chunking import chunk_markdown

CORPUS = Path(__file__).resolve().parents[1] / "corpus"

DOC = """<!-- note -->
# Shop — Returns

## Window
Buyers have 30 days.

## Empty

## Long
""" + " ".join(f"Sentence number {i} is here." for i in range(60))


def test_one_chunk_per_section_with_title():
    chunks = chunk_markdown("shop", DOC)
    assert chunks[0]["heading"] == "Window"
    assert chunks[0]["text"] == "Shop — Returns — Window: Buyers have 30 days."
    assert all(c["heading"] != "Empty" for c in chunks)
    assert "note" not in " ".join(c["text"] for c in chunks)


def test_long_section_split_with_overlap():
    long = [
        c for c in chunk_markdown("shop", DOC, max_chars=300, overlap=50) if c["heading"] == "Long"
    ]
    assert len(long) > 1
    assert all(len(c["text"]) <= 300 + 50 + len("Shop — Returns — Long: ") + 40 for c in long)
    # the tail of the previous part reappears at the start of the next one
    tail = long[0]["text"][-30:]
    assert tail in long[1]["text"]


def test_ids_stable_and_unique():
    first, second = chunk_markdown("shop", DOC), chunk_markdown("shop", DOC)
    assert [c["chunk_id"] for c in first] == [c["chunk_id"] for c in second]
    assert len({c["chunk_id"] for c in first}) == len(first)
    assert chunk_markdown("other", DOC)[0]["chunk_id"] != first[0]["chunk_id"]


def test_real_corpus():
    chunks = [c for f in sorted(CORPUS.glob("*.md")) for c in chunk_markdown(f.stem, f.read_text())]
    assert len(chunks) >= 25
    assert {c["doc_id"] for c in chunks} >= {"amazon-returns", "seller-return-playbook"}
    assert len({c["chunk_id"] for c in chunks}) == len(chunks)
