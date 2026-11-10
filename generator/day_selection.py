"""Choose export days to load; pure logic, does not touch PostgreSQL or fsspec.

`pick_sample_days` duplicates `spark_jobs/lib/day_selection.py` on purpose: the Spark container
does not install the `generator` package and the generator container does not mount `spark_jobs/`. Change
one side, change the other too; both sides have tests.
"""

from __future__ import annotations

DAY_MARKER = "export_date="


# Limitation: copy of spark_jobs/lib/day_selection.pick_sample_days (the two images cannot see each other).
# Merge once the generator and the Spark jobs share one package.
def pick_sample_days(all_days: list[str], n: int) -> list[str]:
    """Pick `n` evenly spread days, always including the first and last; `n <= 0` or too large takes all.

    Spread evenly because data grows over time: taking consecutive days at the end gives a sample that
    does not represent the whole range.
    """
    if n <= 0 or n >= len(all_days):
        return all_days
    if n == 1:
        return [all_days[-1]]
    step = (len(all_days) - 1) / (n - 1)
    return [all_days[round(i * step)] for i in range(n)]


def day_of(path: str) -> str | None:
    """The `export_date=` value in the path, or None."""
    if DAY_MARKER not in path:
        return None
    return path.split(DAY_MARKER, 1)[1].split("/", 1)[0]


def filter_to_sample_days(paths: list[str], n: int) -> list[str]:
    """Keep files belonging to `n` evenly spread export days; `n <= 0` keeps all.

    Filters by path, so files are not opened. Paths without `export_date=` (e.g.
    `seller.skus`) are always kept.
    """
    if n <= 0:
        return paths
    days = sorted({d for p in paths if (d := day_of(p)) is not None})
    if not days:
        return paths
    keep = set(pick_sample_days(days, n))
    return [p for p in paths if (d := day_of(p)) is None or d in keep]
