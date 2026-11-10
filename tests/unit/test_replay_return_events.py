"""Pipeline A Return replay: Flink time format, dedup key per Order Line."""

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
    """Trino returns several ISO forms; Flink takes the first 23 characters ⇒ must always be UTC, exact to the millisecond."""
    event = to_event({"seller_sku": "APP-00042", "label_available_at": raw, "order_line_key": "k1"})
    assert event["effective_at"] == "2026-06-29T13:05:07.123Z"
    assert (event["event_type"], event["quantity"], event["seller_sku"]) == (
        "approved",
        1,
        "APP-00042",
    )


def test_dedup_key_follows_order_line():
    """Replaying the same Order Line ⇒ same key (Flink keeps the first); a different line ⇒ a different key."""
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
    """`feast apply` imports every .py in the repo (Python 3.10 in Airflow) ⇒ scripts other than features.py must be in .feastignore."""
    from pathlib import Path

    repo = Path(__file__).resolve().parents[2] / "feature_repo"
    ignored = {
        line.split("#")[0].strip() for line in (repo / ".feastignore").read_text().splitlines()
    }
    assert {p.name for p in repo.glob("*.py")} - {"features.py"} <= ignored


def test_event_ms_matches_event_time_for_watermark():
    """Kafka record timestamp = effective_at: the PyFlink job derives per-partition watermarks from this value."""
    event = to_event(
        {
            "label_available_at": "2026-06-27 01:00:00.123 UTC",
            "order_line_key": "l",
            "seller_sku": "s",
        }
    )
    assert event_ms(event) == 1782522000123
