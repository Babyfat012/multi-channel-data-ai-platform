"""Phát lại Return pipeline A: chuẩn thời gian cho Flink, khoá bỏ trùng theo Order Line."""

import pytest

from feature_repo.replay_return_events import event_ms, to_event


@pytest.mark.parametrize(
    "raw",
    [
        "2026-06-29T13:05:07.123456",
        "2026-06-29T20:05:07.123456+07:00",
        "2026-06-29 13:05:07.123456 UTC",
    ],
)
def test_event_time_is_utc_millis_for_flink(raw):
    """Trino trả nhiều dạng ISO; Flink cắt 23 ký tự đầu ⇒ phải luôn là UTC, đúng mili giây."""
    event = to_event({"seller_sku": "APP-00042", "label_available_at": raw, "order_line_key": "k1"})
    assert event["effective_at"] == "2026-06-29T13:05:07.123Z"
    assert (event["event_type"], event["quantity"], event["seller_sku"]) == (
        "approved",
        1,
        "APP-00042",
    )


def test_dedup_key_follows_order_line():
    """Cùng Order Line phát lại ⇒ cùng khoá (Flink giữ bản đầu); line khác ⇒ khoá khác."""
    row = {
        "seller_sku": "APP-00042",
        "label_available_at": "2026-06-29T13:05:07",
        "order_line_key": "k1",
    }
    assert to_event(row)["record_sha256"] == to_event(dict(row))["record_sha256"]
    assert (
        to_event(row)["record_sha256"] != to_event(row | {"order_line_key": "k2"})["record_sha256"]
    )


def test_feast_apply_skips_helper_scripts():
    """`feast apply` import mọi .py trong repo (Python 3.10 ở Airflow) ⇒ script ngoài features.py phải nằm trong .feastignore."""
    from pathlib import Path

    repo = Path(__file__).resolve().parents[2] / "feature_repo"
    ignored = {
        line.split("#")[0].strip() for line in (repo / ".feastignore").read_text().splitlines()
    }
    assert {p.name for p in repo.glob("*.py")} - {"features.py"} <= ignored


def test_event_ms_matches_event_time_for_watermark():
    """Timestamp bản ghi Kafka = effective_at: job PyFlink sinh watermark theo partition từ giá trị này."""
    event = to_event(
        {
            "label_available_at": "2026-06-27 01:00:00.123 UTC",
            "order_line_key": "l",
            "seller_sku": "s",
        }
    )
    assert event_ms(event) == 1782522000123
