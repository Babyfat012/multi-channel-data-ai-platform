"""Feature names are typed in three places: Feast (source of truth), training and the Feature API.

Renaming a field in Feast must fail here, not at runtime in training or serving. Read by AST so the
test does not need feast or fastapi installed.
"""

import ast
from pathlib import Path

from ml.train_lib import FEAST_FEATURES

ROOT = Path(__file__).resolve().parents[2]


def _call_kwargs(node):
    """Keyword arguments of a call node, by name."""
    return {k.arg: k.value for k in node.keywords}


def feast_views():
    """{view name: field names} from every `FeatureView(...)` in feature_store/features.py."""
    tree = ast.parse((ROOT / "feature_store/features.py").read_text())
    views = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and getattr(node.func, "id", None) == "FeatureView":
            kwargs = _call_kwargs(node)
            views[ast.literal_eval(kwargs["name"])] = {
                ast.literal_eval(_call_kwargs(field)["name"]) for field in kwargs["schema"].elts
            }
    return views


def serving_views():
    """The `VIEWS` literal of serving/features.py."""
    tree = ast.parse((ROOT / "serving/features.py").read_text())
    node = next(n for n in tree.body if isinstance(n, ast.Assign) and n.targets[0].id == "VIEWS")
    return ast.literal_eval(node.value)


def test_serving_reads_only_feast_fields():
    """Every field the Feature API reads exists in that Feast view."""
    views = feast_views()
    for view, fields in serving_views().items():
        assert set(fields) <= views[view], view


def test_training_reads_only_feast_fields():
    """Every `view:field` training fetches exists in Feast."""
    views = feast_views()
    for ref in FEAST_FEATURES:
        view, field = ref.split(":")
        assert field in views[view], ref
