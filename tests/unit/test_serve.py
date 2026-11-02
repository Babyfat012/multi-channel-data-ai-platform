"""Kiểm ml/serve.py không cần KServe/Redis thật: ghép feature online, chấm điểm, cờ abstain."""

import sys
import types
from unittest.mock import Mock

import numpy as np
import pandas as pd
import pytest

# kserve/lightgbm có thể không cài trong môi trường test: thay bằng module giả tối thiểu.
sys.modules.setdefault("lightgbm", types.SimpleNamespace(Booster=object))
fake_kserve = types.ModuleType("kserve")
fake_kserve.Model = type("Model", (), {"__init__": lambda self, name: setattr(self, "name", name)})
fake_kserve.ModelServer = object
sys.modules.setdefault("kserve", fake_kserve)
fake_errors = types.ModuleType("kserve.errors")
fake_errors.InvalidInput = type("InvalidInput", (ValueError,), {})
sys.modules.setdefault("kserve.errors", fake_errors)

from ml.serve import ReturnRisk  # noqa: E402


class FakeStore:
    """Feast online giả: trả số line có nhãn theo bảng tra."""

    def __init__(self, history):
        """history: {sku: labeled_count_90d}."""
        self.history = history

    def get_online_features(self, features, entity_rows):
        """Cùng hình dạng kết quả với Feast: đối tượng có to_df()."""
        skus = [r["seller_sku"] for r in entity_rows]
        return types.SimpleNamespace(
            to_df=lambda: pd.DataFrame(
                {
                    "seller_sku": skus,
                    "labeled_count_90d": [self.history.get(s) for s in skus],
                    "return_or_refund_rate_90d": [0.3 for _ in skus],
                    "approved_returns_24h": [None for _ in skus],
                }
            )
        )


class FakeBooster:
    """Booster giả trả điểm hằng số."""

    def predict(self, frame):
        """Điểm hằng số cho mọi dòng."""
        return np.full(len(frame), 0.42)


def model(history):
    """Predictor đã nạp store/booster giả, ngưỡng 20."""
    m = ReturnRisk("return-risk")
    m.min_history, m.version = 20, "7"
    m.store, m.booster = FakeStore(history), FakeBooster()
    return m


ORDER = {
    "channel": "amazon",
    "country": "a",
    "ordered_basis_qty": 3,
    "ordered_product_net_minor": 30000,
}


@pytest.mark.parametrize(
    ("history", "abstain"),
    [(None, True), (0, True), (19, True), (20, False), (500, False)],
)
def test_abstain_boundary(history, abstain):
    """Biên 20 giữ nguyên; count thiếu phải có lý do riêng và không có điểm số."""
    out = model({"sku-1": history}).predict({"instances": [{"seller_sku": "sku-1", **ORDER}]})
    p = out["predictions"][0]
    assert p["abstain"] is abstain and p["model_version"] == "7"
    if history is None:
        assert p["score"] is None and p["reason"] == "feature_data_unavailable"
    else:
        assert p["score"] == 0.42
        assert p["reason"] == ("insufficient_history" if abstain else None)


@pytest.mark.parametrize("history", [None, np.nan, pd.NA])
def test_missing_required_feature_never_calls_booster(history):
    """Các dạng dữ liệu thiếu đều phải bỏ qua LightGBM, không chỉ đổi nhãn sau khi chấm."""
    m = model({"sku-1": history})
    m.booster.predict = Mock(side_effect=AssertionError("must not score missing history"))
    result = m.predict({"instances": [{"seller_sku": "sku-1", **ORDER}]})
    assert result["predictions"] == [
        {
            "seller_sku": "sku-1",
            "score": None,
            "abstain": True,
            "reason": "feature_data_unavailable",
            "model_version": "7",
            "batch_release": None,
            "batch_cutoff": None,
        }
    ]
    m.booster.predict.assert_not_called()


def test_mixed_batch_only_scores_available_rows_in_original_order():
    """Batch có SKU thiếu vẫn chấm phần hợp lệ, không tráo điểm giữa SKU hay các đơn cùng SKU."""
    m = model({"missing": None, "known": 1000, "new": 0})
    m.booster.predict = Mock(side_effect=lambda frame: frame["ordered_basis_qty"].to_numpy() / 10)
    rows = [
        {"seller_sku": sku, **ORDER, "ordered_basis_qty": qty}
        for sku, qty in [("missing", 1), ("known", 2), ("missing", 3), ("new", 4), ("known", 5)]
    ]
    result = m.predict({"instances": rows})["predictions"]
    assert [r["seller_sku"] for r in result] == [r["seller_sku"] for r in rows]
    assert [r["score"] for r in result] == [None, 0.2, None, 0.4, 0.5]
    assert [r["reason"] for r in result] == [
        "feature_data_unavailable",
        None,
        "feature_data_unavailable",
        "insufficient_history",
        None,
    ]
    m.booster.predict.assert_called_once()
    assert m.booster.predict.call_args.args[0]["ordered_basis_qty"].tolist() == [2, 4, 5]


def test_nullable_features_do_not_block_known_history():
    """Feature tùy chọn thiếu không bị coi là thiếu count bắt buộc; giữ cách LightGBM xử lý NaN."""
    m = model({"known": 1000})
    m.booster.predict = Mock(wraps=m.booster.predict)
    result = m.predict({"instances": [{"seller_sku": "known", **ORDER}]})["predictions"][0]
    assert result["score"] == 0.42 and result["abstain"] is False
    assert result["reason"] is None
    assert m.booster.predict.call_args.args[0]["approved_returns_24h"].isna().all()


def test_missing_order_field_is_rejected():
    """Thiếu thuộc tính đơn ⇒ lỗi rõ ràng, không chấm trên NaN."""
    from kserve.errors import InvalidInput

    with pytest.raises(InvalidInput, match="channel"):
        model({}).predict(
            {
                "instances": [
                    {
                        "seller_sku": "sku-1",
                        "country": "a",
                        "ordered_basis_qty": 1,
                        "ordered_product_net_minor": 1,
                    }
                ]
            }
        )


def test_order_of_rows_kept_for_many_skus():
    """Nhiều instance: kết quả giữ đúng thứ tự SKU đầu vào."""
    rows = [{"seller_sku": s, **ORDER} for s in ("b", "a", "c")]
    out = model({"a": 50, "b": 1, "c": 50}).predict({"instances": rows})
    assert [p["seller_sku"] for p in out["predictions"]] == ["b", "a", "c"]
    assert [p["abstain"] for p in out["predictions"]] == [True, False, False]


def test_publication_failure_returns_unavailable_without_scoring():
    """Con trỏ/lease batch lỗi phải từ chối dự đoán, không che thành ít lịch sử."""
    from feature_repo.publication import FeatureDataUnavailable

    m = model({"sku-1": 1000})
    m.store.get_online_features = Mock(side_effect=FeatureDataUnavailable("not published"))
    m.booster.predict = Mock(side_effect=AssertionError("must not score"))
    result = m.predict({"instances": [{"seller_sku": "sku-1", **ORDER}]})["predictions"][0]
    assert result["score"] is None and result["reason"] == "feature_data_unavailable"
    m.booster.predict.assert_not_called()
