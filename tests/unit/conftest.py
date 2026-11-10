"""Shared test fixtures: a very small config so tests run in a few seconds."""

from __future__ import annotations

import pytest

from generator.config import GeneratorConfig, parse_config


@pytest.fixture
def tiny_config(tmp_path) -> GeneratorConfig:
    """Config with 20 SKUs, 12 days, 60 Orders per day, written to the test's temp directory."""
    return parse_config(
        {
            "seed": 7,
            "scale": {"n_skus": 20, "days_history": 12, "orders_per_day": 60},
            "offline_problems": {
                "duplicate_rate": 0.02,
                "overlap_days": 3,
                "amazon_orders_api_switch_day": 6,
            },
            "storage": {"output_root": str(tmp_path / "out")},
        }
    )
