"""Phần dựng HTML của render_ab_comparison (không cần Chrome)."""

from scripts.evidence.render_ab_comparison import mark, page


def test_mark_and_totals_per_branch():
    """Ô đạt/trượt/không chấm và dòng tổng đúng với số liệu."""
    a = {"S1": {"sku": "S1", "seconds": 10.0, "gradable": True, "pass": True}}
    b = {
        "S1": {"sku": "S1", "seconds": 6.0, "gradable": True, "pass": False},
        "S2": {"sku": "S2", "seconds": 4.0, "gradable": False, "pass": None},
    }
    assert mark(a["S1"]) == "✓ (10.0 s)" and mark(b["S1"]) == "✗ (6.0 s)"
    assert mark(b["S2"]).startswith("–") and mark(None) == "–"
    doc = page(a, b, "A", "B")
    assert "1/1 đạt, TB 10.0 s/câu" in doc and "0/1 đạt, TB 5.0 s/câu" in doc
