"""Tests for `generator/lib/day_selection.py` — filtering source files down to N export days.

Why test this: it decides between 13 GB and 1.4 GB of data going into PostgreSQL. Two behaviors
are easy to get wrong with silent consequences:

  * Filtering the wrong way -> `seller.skus` is dropped entirely, the SKU table is empty, and no error is raised.
  * Taking the last N consecutive days instead of spreading them -> the sample is all large days, not representative.

`spark_jobs/lib/day_selection.py` has a duplicate `pick_sample_days`, tested separately in
`tests/unit/spark_jobs/lib/test_day_selection.py`. The reason for the duplication is in the module docstring.
"""

from __future__ import annotations

import pytest

from generator.day_selection import day_of, filter_to_sample_days, pick_sample_days

DAYS = [f"2026-{1 + i // 30:02d}-{1 + i % 30:02d}" for i in range(180)]
WALMART = [
    f"pg-load/walmart.orders/export_date={d}/part-sim{i:05d}.jsonl" for i, d in enumerate(DAYS)
]
SKUS = ["pg-load/seller.skus/part-00000.jsonl"]


def test_pick_matches_spark_jobs_copy():
    """Both code copies must give the same result, otherwise the two containers silently diverge."""
    from spark_jobs.lib.day_selection import pick_sample_days as spark_pick

    for n in (0, 1, 2, 20, 180, 500):
        assert pick_sample_days(DAYS, n) == spark_pick(DAYS, n), n


def test_day_of_reads_day_from_directory_name():
    """The day is in the `export_date=...` directory name, not in the file content."""
    assert day_of(WALMART[0]) == DAYS[0]
    assert day_of(SKUS[0]) is None


def test_sample_days_0_keeps_all():
    """The default 0 means 'load all' — the old behavior must not change."""
    paths = WALMART + SKUS
    assert filter_to_sample_days(paths, 0) == paths
    assert filter_to_sample_days(paths, -3) == paths


def test_keeps_exactly_n_distinct_days():
    """Requesting 20 days must yield exactly 20 distinct days among the remaining files."""
    kept = filter_to_sample_days(WALMART, 20)
    assert len({day_of(p) for p in kept}) == 20


def test_always_keeps_first_and_last_day():
    """The first day is the smallest (~872 KB) and the last the largest (~261 MB) — both ends of the range are needed."""
    kept_days = {day_of(p) for p in filter_to_sample_days(WALMART, 20)}
    assert DAYS[0] in kept_days
    assert DAYS[-1] in kept_days


def test_seller_skus_never_dropped():
    """Files without `export_date=` are always kept, even with very tight filtering.

    If this test breaks, table `seller.skus` will be empty after loading, with no error raised.
    """
    for n in (1, 2, 20):
        assert filter_to_sample_days(WALMART + SKUS, n)[-1:] == SKUS, n


def test_only_removes_never_adds_paths():
    """The function may only filter; it must not invent paths or reorder the remaining files."""
    paths = WALMART + SKUS
    kept = filter_to_sample_days(paths, 20)
    assert set(kept) <= set(paths)
    assert kept == [p for p in paths if p in set(kept)]


@pytest.mark.parametrize("n", [1, 2, 7, 20, 60])
def test_never_keeps_more_than_whole_range(n):
    """The number of kept files must not exceed the original count, for every n."""
    assert len(filter_to_sample_days(WALMART, n)) <= len(WALMART)


def test_no_days_keeps_all():
    """If the source is not split by day (only seller.skus), filtering is a no-op and does not return empty."""
    assert filter_to_sample_days(SKUS, 20) == SKUS
