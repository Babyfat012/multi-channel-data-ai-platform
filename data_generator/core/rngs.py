"""Create reproducible random number generators for each part of the generator.

Problem: if the whole program shares one `np.random.Generator`, adding a single
draw in part A shifts every number drawn later in part B. Then the same seed with
a small code change yields completely different data, which is hard to compare.

Approach: each part asks for its own generator, named after that part, e.g.
`rng_for(seed, "orders", 17)` for the Orders of day 17. The generator is built from
(seed, name), so:
- The same seed and name always give the same sequence.
- Day 17 does not depend on how many numbers day 16 drew, so each day can be
  generated independently (less RAM, parallelizable later).
"""

from __future__ import annotations

import hashlib

import numpy as np


def rng_for(seed: int, *names: str | int) -> np.random.Generator:
    """Return a dedicated generator for the part named `names`, derived from `seed`.

    Names are hashed to an integer and combined with the seed, because numpy only accepts numbers as seeds.
    """
    return np.random.default_rng([seed, stable_int(*names)])


def stable_int(*names: str | int) -> int:
    """Hash the names to a 64-bit integer that is stable across runs.

    Python's `hash()` is not used because it changes value per run for strings.
    """
    key = "/".join(str(n) for n in names).encode()
    return int.from_bytes(hashlib.blake2b(key, digest_size=8).digest(), "big")
