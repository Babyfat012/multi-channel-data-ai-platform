"""Tests for reading and validating the config."""

from __future__ import annotations

from pathlib import Path

import pytest

from generator.config import load_config, parse_config


def test_empty_config_uses_design_defaults():
    """Prove an empty config takes the design defaults."""
    config = parse_config({})
    assert config.scale.n_skus == 2000
    assert config.storefront_weights["amazon_us"] == 0.45
    assert sum(config.storefront_weights.values()) == pytest.approx(1.0)


def test_unknown_key_is_rejected():
    """Prove unknown YAML keys are rejected, with a hint of the close name."""
    with pytest.raises(ValueError, match="order_per_day"):
        parse_config({"scale": {"order_per_day": 10}})


def test_storefront_weights_must_sum_to_one():
    """Prove Storefront weights not summing to 1 raise a clear error."""
    with pytest.raises(ValueError, match="must sum to 1"):
        parse_config({"storefront_weights": {"amazon_us": 0.5, "walmart_us": 0.2}})


def test_unknown_storefront_is_rejected():
    """Prove a Storefront unknown to the system is rejected."""
    with pytest.raises(ValueError, match="does not exist"):
        parse_config({"storefront_weights": {"ebay_us": 1.0}})


def test_cutoff_is_midnight_after_last_history_day():
    """Prove the Cutoff is the midnight right after the last history day."""
    config = parse_config({"scale": {"days_history": 180}})
    assert config.cutoff.isoformat() == "2026-06-30T00:00:00+00:00"


@pytest.mark.parametrize("name", ["small.yaml", "default.yaml"])
def test_shipped_config_files_are_valid(name):
    """Prove the config files shipped with the repo load without errors."""
    load_config(Path(__file__).parents[3] / "configs" / name)


@pytest.mark.parametrize(
    "prime_day, message",
    [
        ({"day": 180, "days": 2}, "history day range"),
        ({"day": 0}, "history day range"),
        ({"day": 10, "discount_share": 1.5}, "discount_share"),
        ({"day": 10, "deal_skus": 5000}, "deal_skus"),
        ({"day": 10, "dealskus": 5}, "drift.prime_day"),
    ],
)
def test_invalid_prime_day_is_rejected(prime_day, message):
    """Prove invalid Sale Event configs (outside history, out of bounds, wrong key) are all rejected."""
    with pytest.raises(ValueError, match=message):
        parse_config({"scale": {"days_history": 180}, "drift": {"prime_day": prime_day}})


def test_drift_is_optional_and_defaults_to_no_sale():
    """Prove that without drift there is no Sale Event by default."""
    assert parse_config({}).drift.prime_day is None
