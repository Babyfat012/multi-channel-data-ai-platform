"""Tests for `spark_jobs/lib/day_selection.py`: how the set of export days for a trial load is picked.

Why test this: it decides whether the Spark UI screenshot exposes skew. Taking consecutive days
at the end of the range makes every task nearly the same size and the skew evidence disappears, so "evenly spaced" is
behavior that must be locked down by a test, not an arbitrary detail.
"""

from __future__ import annotations

import pytest

from spark_jobs.lib.day_selection import pick_sample_days, pick_window_days

# 180 days like the real data: 2026-01-01 .. 2026-06-29.
DAYS = [f"d{i:03d}" for i in range(180)]


def test_takes_all_when_n_is_0_or_too_large():
    """n <= 0 and n >= the number of days both mean 'load all', returning the input list itself."""
    assert pick_sample_days(DAYS, 0) == DAYS
    assert pick_sample_days(DAYS, -5) == DAYS
    assert pick_sample_days(DAYS, 180) == DAYS
    assert pick_sample_days(DAYS, 500) == DAYS


def test_exact_count_and_no_duplicates():
    """Taking n days must yield exactly n distinct days, keeping ascending order."""
    picked = pick_sample_days(DAYS, 20)
    assert len(picked) == 20
    assert len(set(picked)) == 20
    assert picked == sorted(picked)


def test_always_includes_first_and_last_day():
    """This is the condition for skew to show: the first day of the range is smallest, the last largest."""
    for n in (2, 3, 7, 20, 179):
        picked = pick_sample_days(DAYS, n)
        assert picked[0] == DAYS[0], n
        assert picked[-1] == DAYS[-1], n


def test_evenly_spaced_not_bunched_at_one_end():
    """Gaps between the picked days must be roughly equal, differing by at most 1 step.

    If the function changed to 'last n days' this test would break: every gap would be 1 except one
    huge gap at the start.
    """
    picked = pick_sample_days(DAYS, 20)
    idx = [DAYS.index(d) for d in picked]
    gaps = [b - a for a, b in zip(idx, idx[1:])]
    assert max(gaps) - min(gaps) <= 1, gaps


def test_n_equals_1_takes_largest_day():
    """With only one day, take the last day, because it has the most data."""
    assert pick_sample_days(DAYS, 1) == [DAYS[-1]]


@pytest.mark.parametrize("n", [2, 5, 50])
def test_every_picked_day_exists(n):
    """The function may only pick from the input list, never invent new days."""
    assert set(pick_sample_days(DAYS, n)) <= set(DAYS)


class TestPickWindowDays:
    """Pick CONSECUTIVE days: the opposite purpose of `pick_sample_days`.

    Generator duplicates sit within `overlap_days = 3`, so two days 9-10 days apart
    never contain the same Order. Measured: 20 evenly spaced days let dedup catch
    exactly 0 duplicates (`docs/data-platform/reference/silver.md` section 10.3). So a way to pick consecutive days is needed.
    """

    DAYS = [f"2026-01-{d:02d}" for d in range(1, 21)]

    def test_consecutive_from_given_start(self):
        """Take `n` consecutive days starting at `start`."""
        got = pick_window_days(self.DAYS, 3, start="2026-01-05")
        assert got == ["2026-01-05", "2026-01-06", "2026-01-07"]

    def test_no_start_takes_MIDDLE_of_range(self):
        """Take the middle because both ends of the range are edge cases: early days too small, late days too large."""
        got = pick_window_days(self.DAYS, 4)
        assert got == ["2026-01-09", "2026-01-10", "2026-01-11", "2026-01-12"]

    def test_returned_days_are_CONTIGUOUS_without_gaps(self):
        """The returned days are consecutive, not spread like `pick_sample_days`."""
        got = pick_window_days(self.DAYS, 7, start="2026-01-02")
        idx = [self.DAYS.index(d) for d in got]
        assert idx == list(range(idx[0], idx[0] + 7))

    def test_hitting_end_of_range_shifts_back_to_keep_n(self):
        """Asking for 5 days from day 18 leaves only 3 days: must shift back to still get 5."""
        got = pick_window_days(self.DAYS, 5, start="2026-01-18")
        assert len(got) == 5
        assert got[-1] == "2026-01-20"

    def test_n_larger_than_range_takes_all(self):
        """`n` larger than the whole range takes everything."""
        assert pick_window_days(self.DAYS, 99) == self.DAYS

    def test_n_zero_is_empty(self):
        """`n = 0` returns empty."""
        assert pick_window_days(self.DAYS, 0) == []

    def test_wrong_start_day_RAISES(self):
        """Raise instead of silently picking the nearest day.

        Mistyping a day and then running 20 minutes into an unexpected table is worse than stopping right away.
        """
        with pytest.raises(ValueError, match="is not in the list"):
            pick_window_days(self.DAYS, 3, start="2026-99-99")

    def test_differs_from_pick_sample_days_on_same_input(self):
        """Both functions with the same n give different sets: that is exactly why there are two functions."""
        n = 5
        assert pick_window_days(self.DAYS, n) != pick_sample_days(self.DAYS, n)
