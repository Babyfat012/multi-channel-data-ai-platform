"""Shared pieces for every Channel renderer.

A renderer does one thing: take a core `Order` and a snapshot time `as_of`,
and return the record in the Channel's native shape at that time. Renderers do not
draw random numbers. Everything that "looks random", such as Amazon order ids, is derived
from the canonical id by hashing, so the same Order always has the same native id in every
export, in both Batch and Stream.
"""

from __future__ import annotations

import string
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol

from generator.core.model import Order
from generator.core.rngs import stable_int

OBJECT_STORE = "object_store"  # MinIO, bucket source-exports
POSTGRES = "postgres"  # PostgreSQL simulates another department's system


@dataclass(frozen=True)
class RenderedRecord:
    """A record already in the Channel's native shape.

    Attributes:
        key: the Channel's dedup key (e.g. `AmazonOrderId`), used to count the duplicate rate.
        record: the content to write, a JSON-serializable dict.
    """

    key: str
    record: dict[str, Any]


class ChannelRenderer(Protocol):
    """Interface every Channel renderer must implement."""

    channel: str
    dataset: str
    storage: str

    def render(self, order: Order, as_of: datetime, export_day: int) -> RenderedRecord:
        """Render `order` as the Channel shows it to the Seller at time `as_of`."""
        ...

    def line_keys(self, order: Order) -> list[dict[str, Any]]:
        """Return the mapping from canonical `order_line_id` to the Channel's native ids."""
        ...


def native_digits(seed: int, kind: str, source_id: str, length: int) -> str:
    """Generate a fixed string of `length` digits for `source_id`.

    E.g. `native_digits(42, "amazon_order", "ORD-001-00001", 7)` always yields the same
    7 digits. `kind` keeps two id types generated from the same Order from colliding.
    """
    value = stable_int(seed, kind, source_id)
    return str(value % 10**length).zfill(length)


def native_code(seed: int, kind: str, source_id: str, length: int) -> str:
    """Like `native_digits` but uses uppercase letters and digits (e.g. the ASIN suffix)."""
    alphabet = string.ascii_uppercase + string.digits
    value = stable_int(seed, kind, source_id)
    chars = []
    for _ in range(length):
        value, i = divmod(value, len(alphabet))
        chars.append(alphabet[i])
    return "".join(chars)


def iso_z(t: datetime) -> str:
    """Format a UTC time as ISO 8601 with a Z suffix, e.g. `2026-01-01T08:30:00Z`."""
    return t.strftime("%Y-%m-%dT%H:%M:%SZ")


def epoch_millis(t: datetime) -> int:
    """Convert a time to milliseconds since 1970-01-01 UTC (the format Walmart uses)."""
    return int(t.timestamp() * 1000)


def money(amount: float, decimals: int) -> str:
    """Format an amount as a string with the currency's number of decimal places."""
    return f"{amount:.{decimals}f}"
