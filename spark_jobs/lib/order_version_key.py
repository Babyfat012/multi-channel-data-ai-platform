"""Amazon Order version key shared by the SCD2 dim and the Gold fact."""

from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F


def amazon_order_version_key(order_id: Column, changed_at: Column) -> Column:
    """Hash exactly the Order id and the UTC time the status changed."""
    identity = F.struct(order_id.alias("order_id"), changed_at.alias("valid_from_ts"))
    return F.when(order_id.isNotNull() & changed_at.isNotNull(), F.sha2(F.to_json(identity), 256))
