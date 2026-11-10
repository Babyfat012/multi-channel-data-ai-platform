"""HTML rendering part of render_ab_comparison (no Chrome needed)."""

from scripts.evidence.render_ab_comparison import mark, page


def test_mark_and_totals_per_branch():
    """Pass/fail/ungraded cells and the total row match the data."""
    a = {"S1": {"sku": "S1", "seconds": 10.0, "gradable": True, "pass": True}}
    b = {
        "S1": {"sku": "S1", "seconds": 6.0, "gradable": True, "pass": False},
        "S2": {"sku": "S2", "seconds": 4.0, "gradable": False, "pass": None},
    }
    assert mark(a["S1"]) == "✓ (10.0 s)" and mark(b["S1"]) == "✗ (6.0 s)"
    assert mark(b["S2"]).startswith("–") and mark(None) == "–"
    doc = page(a, b, "A", "B")
    assert "1/1 passed, avg 10.0 s/question" in doc and "0/1 passed, avg 5.0 s/question" in doc
