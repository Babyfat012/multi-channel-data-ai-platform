"""Batch delivery layer: which Order lands on which export day, and duplicate injection.

Base rule (no faults yet): each Order appears **exactly once**, on the export
day holding its last update before `cutoff`, with the status at that time.
So with no faults injected, the duplicate rate by key is 0.

Duplicate injection (simulates overlapping exports: each export re-pulls the last few
days to catch updates). With probability `p`, an Order gets **one** extra copy:

- **Identical copy**: on an export day 1 to `overlap_days` days later. The status
  does not change because the last update already happened, so the content is identical.
- **Older version**: on an export day 1 to `overlap_days` days earlier, captured while
  the Order was still in its previous status. Same key as the primary but an older
  update time. Correct dedup must keep the latest version by update time, not just
  drop identical rows.

How to choose `p` so the measured duplicate rate equals `r` (config `duplicate_rate`):
the duplicate rate is measured as `1 - distinct keys / rows`. If N Orders get pN extra copies,
the rate is pN / (N + pN) = p / (1 + p). For it to equal r, p = r / (1 - r).
E.g. r = 2% gives p ≈ 2.04%.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

import numpy as np

from generator.config import GeneratorConfig
from generator.core.model import Order

PRIMARY = "primary"
IDENTICAL_COPY = "identical_copy"
OLDER_VERSION = "older_version"


@dataclass(frozen=True)
class ExportPlacement:
    """One appearance of an Order in the Batch data.

    Attributes:
        export_day: simulated day of the export, decides the `export_date` partition.
        as_of: time the Order status is captured (end of the export day, no later than `cutoff`).
        kind: `primary`, `identical_copy` or `older_version`.
    """

    export_day: int
    as_of: datetime
    kind: str


def day_of(config: GeneratorConfig, t: datetime) -> int:
    """Convert a UTC time to the simulated day number (day 1 is `start_date`)."""
    return (t.date() - config.scale.start_date).days + 1


def end_of_day(config: GeneratorConfig, day: int) -> datetime:
    """End of day `day` (23:59:59 UTC), when that day's export runs."""
    return config.day_start(day + 1) - timedelta(seconds=1)


def plan_exports(
    order: Order, config: GeneratorConfig, rng: np.random.Generator
) -> list[ExportPlacement]:
    """An Order's appearances in the Batch: the primary and possibly one duplicate.

    The snapshot is taken at `cutoff - 1s`; Orders created after that do not exist yet, so return empty. The
    1s is required: an Order created in the last second makes `status_at` raise, which is near certain at scale.
    """
    as_of = config.cutoff - timedelta(seconds=1)
    if order.created_at > as_of:
        return []

    last_change = order.status_at(as_of)
    primary_day = day_of(config, last_change.at)
    placements = [ExportPlacement(primary_day, end_of_day(config, primary_day), PRIMARY)]

    rate = config.offline_problems.duplicate_rate
    if rng.random() < rate / (1 - rate):
        duplicate = _pick_duplicate(order, primary_day, config, rng)
        if duplicate is not None:
            placements.append(duplicate)
    return placements


def _pick_duplicate(
    order: Order,
    primary_day: int,
    config: GeneratorConfig,
    rng: np.random.Generator,
) -> ExportPlacement | None:
    """Choose where to place the duplicate; if the kind drawn first is invalid, try the other.

    A later copy cannot pass the last history day, an earlier copy cannot precede the Order's creation day. If
    neither fits, skip it; the manifest records the actual number of duplicates.
    """
    shift = int(rng.integers(1, config.offline_problems.overlap_days + 1))
    first_day = day_of(config, order.created_at)
    last_day = config.scale.days_history

    later_day = primary_day + shift if primary_day + shift <= last_day else None
    earlier_day = primary_day - shift if primary_day - shift >= first_day else None
    candidates = [later_day, earlier_day] if rng.random() < 0.5 else [earlier_day, later_day]

    for day in candidates:
        if day is None:
            continue
        as_of = end_of_day(config, day)
        same_state = order.status_at(as_of) == order.status_at(end_of_day(config, primary_day))
        return ExportPlacement(day, as_of, IDENTICAL_COPY if same_state else OLDER_VERSION)
    return None
