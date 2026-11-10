"""Pick export days to load; pure logic with no Spark dependency, so it is testable on the host."""

from __future__ import annotations


def pick_sample_days(all_days: list[str], n: int) -> list[str]:
    """Pick `n` evenly spaced days, always including the first and last; `n <= 0` or too large takes all.

    Spreading evenly gives one run both small and large days, so the Spark UI exposes skew.
    """
    if n <= 0 or n >= len(all_days):
        return all_days
    if n == 1:
        return [all_days[-1]]
    step = (len(all_days) - 1) / (n - 1)
    return [all_days[round(i * step)] for i in range(n)]


def pick_window_days(all_days: list[str], n: int, start: str | None = None) -> list[str]:
    """Pick `n` consecutive days; `start=None` takes the window in the middle of the range.

    Used to check dedup: duplicates only sit within `overlap_days` of each other, so evenly
    spaced days catch no duplicates. Requires `n >= 7`. A `start` not in the list
    raises `ValueError`.

    >>> days = [f"2026-01-{d:02d}" for d in range(1, 11)]
    >>> pick_window_days(days, 3, start="2026-01-05")
    ['2026-01-05', '2026-01-06', '2026-01-07']
    >>> pick_window_days(days, 4)
    ['2026-01-04', '2026-01-05', '2026-01-06', '2026-01-07']
    >>> pick_window_days(days, 0)
    []
    >>> pick_window_days(days, 99) == days
    True
    """
    if n <= 0:
        return []
    if n >= len(all_days):
        return all_days
    if start is None:
        first = (len(all_days) - n) // 2
    else:
        if start not in all_days:
            raise ValueError(f"start day {start!r} is not in the list of days on disk")
        first = all_days.index(start)
        # If the window hits the end of the range, shift back to keep `n` days instead of returning fewer.
        first = min(first, len(all_days) - n)
    return all_days[first : first + n]
