"""Tests for the Batch export layer: primary placement and duplicate injection."""

from __future__ import annotations

import dataclasses
from datetime import timedelta

from generator.core.rngs import rng_for
from generator.core.simulate import Simulator
from generator.delivery.batch_exports import (
    IDENTICAL_COPY,
    OLDER_VERSION,
    PRIMARY,
    day_of,
    plan_exports,
)


def _with_duplicate_rate(config, rate):
    """A copy of the config with a different `duplicate_rate`."""
    problems = dataclasses.replace(config.offline_problems, duplicate_rate=rate)
    return dataclasses.replace(config, offline_problems=problems)


def test_without_duplicates_each_order_appears_once_at_last_update(tiny_config):
    """Prove that with `duplicate_rate=0` each Order has exactly one primary copy, on the export day of its last status change before the Cutoff."""
    config = _with_duplicate_rate(tiny_config, 0.0)
    rng = rng_for(0, "test")
    for order in Simulator(config).simulate_day(10):
        placements = plan_exports(order, config, rng)
        assert [p.kind for p in placements] == [PRIMARY]
        last_change = order.status_at(config.cutoff - timedelta(seconds=1))
        assert placements[0].export_day == day_of(config, last_change.at)
        assert order.status_at(placements[0].as_of) == last_change


def test_duplicates_are_identical_later_or_older_earlier(tiny_config):
    """Prove a duplicate is either identical and arrives later, or an older version that arrives earlier, and always within the overlap range."""
    config = _with_duplicate_rate(tiny_config, 0.4)
    rng = rng_for(0, "test")
    kinds = set()
    for day in range(1, 11):
        for order in Simulator(config).simulate_day(day):
            primary, *copies = plan_exports(order, config, rng)
            for copy in copies:
                kinds.add(copy.kind)
                gap = copy.export_day - primary.export_day
                assert 1 <= abs(gap) <= config.offline_problems.overlap_days
                assert copy.export_day <= config.scale.days_history
                if copy.kind == IDENTICAL_COPY:
                    assert gap > 0
                    assert order.status_at(copy.as_of) == order.status_at(primary.as_of)
                else:
                    assert copy.kind == OLDER_VERSION and gap < 0
                    assert order.status_at(copy.as_of).at < order.status_at(primary.as_of).at
    assert kinds == {IDENTICAL_COPY, OLDER_VERSION}


def test_orders_after_cutoff_are_not_in_batch(tiny_config):
    """Prove Orders created after the Cutoff are not exported in the Batch."""
    after_cutoff_day = tiny_config.scale.days_history + 1
    rng = rng_for(0, "test")
    for order in Simulator(tiny_config).simulate_day(after_cutoff_day):
        assert plan_exports(order, tiny_config, rng) == []


def test_order_created_in_the_last_second_before_cutoff_is_left_out(tiny_config):
    """Orders created in the last second of history are excluded and do not make `status_at` raise.

    The Batch snapshot is taken at `cutoff - 1s`, so Orders created after that do not exist yet
    at snapshot time. The probability per Order is 1/86,400: with `default.yaml` (4,000 Orders/day)
    it almost never happens, but with `scale-100gb.yaml` (852,099 Orders/day) it averages
    ~10 Orders per run. Without this test the bug only shows up after several hours.
    """
    order = next(iter(Simulator(tiny_config).simulate_day(1)))
    rng = rng_for(0, "test")
    as_of = tiny_config.cutoff - timedelta(seconds=1)

    for offset, expected in [
        (timedelta(0), True),
        (timedelta(microseconds=1), False),
        (timedelta(milliseconds=500), False),
    ]:
        late = dataclasses.replace(order, created_at=as_of + offset)
        assert bool(plan_exports(late, tiny_config, rng)) is expected, offset
