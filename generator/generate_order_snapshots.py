"""Run the Batch part: simulation core -> export layer -> renderer -> files.

For each simulated day `d`:
    1. The core generates the Orders created on day d (with their whole future).
    2. The export layer picks the export day for each Order and injects duplicates.
    3. The renderer writes native records to `export_date=.../part-sim<d>.jsonl`.
    4. Close every file of day d, then write the stamp `_checkpoint/day-<d>.json`.

Each simulated day writes its own files because an Order may be exported up to ~20 days
after its creation day; with shared files one could never say "day d is done". Readers glob the
whole `export_date=` directory, so the set of rows is unchanged.

Output layout (under `storage.output_root`):

    source-exports/channel=<channel>/dataset=orders/export_date=YYYY-MM-DD/part-simNNNNN.jsonl
    pg-load/walmart.orders/export_date=YYYY-MM-DD/part-simNNNNN.jsonl
    pg-load/seller.skus/part-00000.jsonl
    ground-truth/labels/part-simNNNNN.parquet
    ground-truth/order_line_keys/<channel>/part-simNNNNN.parquet
    _checkpoint/day-NNNNN.json
    run_manifest.json

Resuming after an interruption: the stamp of day d holds every counter up to the end of day d. The stamp is
only written after the files are `fsync`ed, via write-temp-then-rename. On startup, find the largest d for which
stamps 1..d all exist, delete the files of days after d, then run from d+1. `--restart` starts over.

`pg-load/` is loaded into PostgreSQL by the `load-postgres` command (see `load_postgres.py`).
"""

from __future__ import annotations

import re
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any

import numpy as np
import pyarrow as pa

from generator.channels.amazon import SCHEMA_2026, AmazonOrdersRenderer
from generator.channels.base import OBJECT_STORE, ChannelRenderer
from generator.channels.shopee import ShopeeOrdersRenderer
from generator.channels.shopify import ShopifyOrdersRenderer
from generator.channels.tiktok_batch import TikTokOrdersRenderer
from generator.channels.walmart import WalmartOrdersRenderer
from generator.config import GeneratorConfig
from generator.core.model import Order
from generator.core.rngs import rng_for
from generator.core.simulate import Simulator
from generator.core.storefronts import STOREFRONTS
from generator.dataset_identity import build_dataset_identity
from generator.delivery.batch_exports import PRIMARY, plan_exports
from generator.storage import JsonlWriterPool, OutputStore

CHECKPOINT_DIR = "_checkpoint"
DATASET_IDENTITY_PATH = "_dataset_identity.json"
# Every file a simulated day writes, listed explicitly so nothing else is deleted by mistake.
DAY_OWNED_PATTERNS = (
    "source-exports/channel=*/dataset=*/export_date=*/part-sim*.jsonl",
    "pg-load/*/export_date=*/part-sim*.jsonl",
    "ground-truth/labels/part-sim*.parquet",
    "ground-truth/order_line_keys/*/part-sim*.parquet",
    f"{CHECKPOINT_DIR}/day-*.json",
)
SIM_DAY_IN_NAME = re.compile(r"(?:part-sim|day-)(\d+)\.")


def _checkpoint_relative(day: int) -> str:
    """Stamp path of a simulated day."""
    return f"{CHECKPOINT_DIR}/day-{day:05d}.json"


LABELS_SCHEMA = pa.schema(
    [
        ("order_line_id", pa.string()),
        ("label", pa.int8()),
        ("delivered_at", pa.timestamp("us", tz="UTC")),
        ("label_available_at", pa.timestamp("us", tz="UTC")),
    ]
)
LINE_KEYS_SCHEMA = pa.schema(
    [
        ("order_line_id", pa.string()),
        ("storefront_id", pa.string()),
        ("channel_order_id", pa.string()),
        ("channel_line_id", pa.string()),
    ]
)


@dataclass
class DatasetStats:
    """Actual figures of a dataset, written to the manifest as the answer key."""

    rows: int = 0
    unique_keys: int = 0
    placements: Counter = field(default_factory=Counter)
    files: int = 0
    bytes: int = 0

    def summary(self) -> dict[str, Any]:
        """Summarize as a dict for JSON output."""
        return {
            "rows": self.rows,
            "unique_keys": self.unique_keys,
            "duplicate_rate": round(1 - self.unique_keys / self.rows, 6) if self.rows else 0.0,
            "rows_by_placement": dict(self.placements),
            "files": self.files,
            "bytes": self.bytes,
        }


class BatchRun:
    """One Batch generation, from config to output files and manifest."""

    def __init__(self, config: GeneratorConfig) -> None:
        """Set up the simulation core, per-Channel renderers and the output store."""
        self.config = config
        self.simulator = Simulator(config)
        self.store = OutputStore(config.storage.output_root)
        self.dataset_identity = build_dataset_identity(config)
        # Each Channel has a Batch source renderer. True labels stay in the separate ground truth.
        self.renderers: dict[str, ChannelRenderer] = {
            "amazon": AmazonOrdersRenderer(config),
            "tiktok_shop": TikTokOrdersRenderer(config),
            "shopify": ShopifyOrdersRenderer(config),
            "walmart": WalmartOrdersRenderer(config),
            "shopee": ShopeeOrdersRenderer(config),
        }
        # Each export day still awaiting rows has an open file, so RAM does not grow with order count.
        self.jsonl: JsonlWriterPool | None = None
        self.stats: dict[str, DatasetStats] = defaultdict(DatasetStats)
        # Buffers of one simulated day; cumulative counters must live in the stamp (`_counters`).
        self.labels: dict[str, list[Any]] = self._empty_labels()
        self.line_keys: dict[str, list[dict[str, Any]]] = defaultdict(list)
        self.label_rows = 0
        self.label_positives = 0
        self.category_labels: dict[str, Counter] = defaultdict(Counter)
        self.orders_by_storefront: Counter = Counter()
        # Sale Event figures, split by sale days and normal days (keys True/False).
        self.sale_days = 0
        self.amazon_orders: Counter = Counter()
        self.amazon_lines: Counter = Counter()
        self.amazon_discount_sum: dict[bool, float] = {True: 0.0, False: 0.0}
        self.amazon_labels: dict[bool, Counter] = {True: Counter(), False: Counter()}

    def run(self, restart: bool = False) -> dict[str, Any]:
        """Generate the whole Batch and return the manifest; by default resume from the latest stamp.

        `restart=True` deletes the old output and starts over from day 1. The result does not depend on whether
        the run was interrupted, since each day draws its own random numbers from `(seed, "batch_exports", day)`.
        """
        self._check_resume_identity(restart)
        resume_from = 0 if restart else self._last_complete_day()
        if resume_from:
            self._restore(self.store.read_json(_checkpoint_relative(resume_from)))
        self._discard_days_after(resume_from)
        if restart:
            # While a run is in progress, an old manifest must not look finished.
            self.store.remove("run_manifest.json")
        self.store.write_json_atomic(DATASET_IDENTITY_PATH, self.dataset_identity)
        with self.store.jsonl_writers() as jsonl:
            self.jsonl = jsonl
            for day in range(resume_from + 1, self.config.scale.days_history + 1):
                rng = rng_for(self.config.seed, "batch_exports", day)
                sale_day = self.simulator.is_sale_day(day)
                self.sale_days += sale_day
                for order in self.simulator.simulate_day(day):
                    self._handle_order(order, rng, sale_day, day)
                self._flush(day)
        self._write_skus()
        manifest = self._manifest()
        self.store.write_json("run_manifest.json", manifest)
        return manifest

    def _check_resume_identity(self, restart: bool) -> None:
        """Refuse to resume on stale output or a different world, before changing anything.

        `--restart` can still regenerate. Assumes a single writer; this is not a distributed lock.
        """
        if restart:
            return
        if not self.store.exists(DATASET_IDENTITY_PATH):
            owned = ("run_manifest.json", "pg-load/seller.skus/part-00000.jsonl")
            if any(self.store.exists(path) for path in owned) or any(
                self.store.glob(pattern) for pattern in DAY_OWNED_PATTERNS
            ):
                raise ValueError(
                    "Output has no dataset identity; use a new output_root or "
                    "explicit --restart to regenerate legacy data."
                )
            return
        previous = self.store.read_json(DATASET_IDENTITY_PATH)
        if previous != self.dataset_identity:
            raise ValueError(
                "Dataset identity mismatch (config/code/runtime changed); use a "
                "new output_root or explicit --restart to regenerate."
            )
        version = self.dataset_identity["dataset_version"]
        for path in self.store.glob(f"{CHECKPOINT_DIR}/day-*.json"):
            if self.store.read_json(path).get("dataset_version") != version:
                raise ValueError(f"Checkpoint dataset identity mismatch: {path}")
        if self.store.exists("run_manifest.json") and (
            self.store.read_json("run_manifest.json").get("dataset_version") != version
        ):
            raise ValueError("Manifest dataset identity mismatch")

    def _handle_order(
        self, order: Order, rng: np.random.Generator, sale_day: bool, sim_day: int
    ) -> None:
        """Process one Order: place it on export days, write records, collect labels."""
        self.orders_by_storefront[order.storefront_id] += 1
        self._collect_labels(order, sale_day)
        if STOREFRONTS[order.storefront_id].channel == "amazon":
            self._collect_sale_stats(order, sale_day)

        renderer = self.renderers.get(STOREFRONTS[order.storefront_id].channel)
        if renderer is None:
            return
        self.line_keys[renderer.channel].extend(
            {**keys, "storefront_id": order.storefront_id} for keys in renderer.line_keys(order)
        )
        # Newer Channels use a per-Order RNG so the Amazon/Walmart random sequence is unchanged.
        placement_rng = (
            rng
            if renderer.channel in ("amazon", "walmart")
            else rng_for(
                self.config.seed,
                "batch_exports_new_channel",
                sim_day,
                renderer.channel,
                order.order_id,
            )
        )
        for placement in plan_exports(order, self.config, placement_rng):
            rendered = renderer.render(order, placement.as_of, placement.export_day)
            key = (renderer.channel, placement.export_day, sim_day)
            self.jsonl.write(
                key, self._export_path(renderer, placement.export_day, sim_day), rendered.record
            )
            stats = self.stats[f"{renderer.channel}.{renderer.dataset}"]
            stats.rows += 1
            # Each Order has exactly one `primary` copy, so counting primaries counts unique keys
            # without holding a huge key set in RAM.
            stats.unique_keys += placement.kind == PRIMARY
            stats.placements[placement.kind] += 1

    def _collect_sale_stats(self, order: Order, sale_day: bool) -> None:
        """Count Amazon Orders and discounts, splitting sale days from normal days."""
        self.amazon_orders[sale_day] += 1
        self.amazon_lines[sale_day] += len(order.lines)
        self.amazon_discount_sum[sale_day] += sum(
            line.unit_discount / line.unit_price for line in order.lines if line.unit_price
        )

    def _collect_labels(self, order: Order, sale_day: bool = False) -> None:
        """Collect labels of Order Lines whose label is known by `cutoff`."""
        if order.delivered_at is None:
            return
        available_at = order.delivered_at + timedelta(days=self.config.labels.window_days)
        if available_at > self.config.cutoff:
            return
        for line in order.lines:
            self.labels["order_line_id"].append(line.order_line_id)
            self.labels["label"].append(line.label)
            self.labels["delivered_at"].append(order.delivered_at)
            self.labels["label_available_at"].append(available_at)
            self.label_rows += 1
            self.label_positives += line.label
            category = self.simulator.catalog.skus_by_id[line.sku_id].category
            self.category_labels[category][line.label] += 1
            if STOREFRONTS[order.storefront_id].channel == "amazon":
                self.amazon_labels[sale_day][line.label] += 1

    def _export_path(self, renderer: ChannelRenderer, export_day: int, sim_day: int) -> str:
        """File path: the directory is `export_date=`, the file name is the simulated day that wrote it."""
        export_date = self.config.day_start(export_day).date().isoformat()
        part = f"part-sim{sim_day:05d}.jsonl"
        if renderer.storage == OBJECT_STORE:
            return (
                f"source-exports/channel={renderer.channel}/dataset={renderer.dataset}/"
                f"export_date={export_date}/{part}"
            )
        return f"pg-load/{renderer.channel}.{renderer.dataset}/export_date={export_date}/{part}"

    def _flush(self, day: int) -> None:
        """Finish day `day`: close all its files, write Parquet, then write the stamp.

        The stamp must be the last step: everything it reports as "done" has been `fsync`ed before.
        """
        for key in sorted(self.jsonl.keys()):
            channel = key[0]
            renderer = self.renderers[channel]
            _, written = self.jsonl.close(key)
            stats = self.stats[f"{channel}.{renderer.dataset}"]
            stats.bytes += written
            stats.files += 1
        self._flush_ground_truth(day)
        self.store.write_json_atomic(_checkpoint_relative(day), self._counters())

    def _flush_ground_truth(self, day: int) -> None:
        """Write the labels and key mapping of day `day` as a separate Parquet file.

        One file per day because the Parquet footer is only written on close; a file kept open for the whole run
        would be lost entirely on interruption. Days without rows create no file.
        """
        if self.labels["order_line_id"]:
            with self.store.parquet_writer(
                f"ground-truth/labels/part-sim{day:05d}.parquet", LABELS_SCHEMA
            ) as writer:
                writer.write(self.labels)
        self.labels = self._empty_labels()
        for channel, rows in self.line_keys.items():
            if not rows:
                continue
            columns = {name: [row[name] for row in rows] for name in LINE_KEYS_SCHEMA.names}
            with self.store.parquet_writer(
                f"ground-truth/order_line_keys/{channel}/part-sim{day:05d}.parquet",
                LINE_KEYS_SCHEMA,
            ) as writer:
                writer.write(columns)
        self.line_keys.clear()

    @staticmethod
    def _empty_labels() -> dict[str, list[Any]]:
        """Empty label buffer, one list per column of `LABELS_SCHEMA`."""
        return {name: [] for name in LABELS_SCHEMA.names}

    def _last_complete_day(self) -> int:
        """The largest day `d` for which stamps of all days 1..d exist.

        A stray stamp after a gap does not prove that earlier days are done.
        """
        present = {
            int(match.group(1))
            for path in self.store.glob(f"{CHECKPOINT_DIR}/day-*.json")
            if (match := SIM_DAY_IN_NAME.search(path))
        }
        day = 0
        while day + 1 in present:
            day += 1
        return day

    def _discard_days_after(self, day: int) -> list[str]:
        """Delete every file of simulated days > `day` (including leftover `.tmp`), returning the deleted list.

        Those files have no stamp, so it is unknown whether they are complete; regenerating yields exactly the same data.
        """
        removed = []
        for pattern in DAY_OWNED_PATTERNS:
            for path in self.store.glob(pattern):
                match = SIM_DAY_IN_NAME.search(path)
                if match and int(match.group(1)) > day:
                    self.store.remove(path)
                    removed.append(path)
        for path in self.store.glob(f"{CHECKPOINT_DIR}/*.tmp"):
            self.store.remove(path)
            removed.append(path)
        return removed

    def _counters(self) -> dict[str, Any]:
        """All cumulative counters, JSON-serializable, to write into the stamp.

        Must cover everything `_manifest` reads, otherwise a resumed run produces a manifest with missing figures
        without any error. Bool keys are written as "sale"/"normal", numeric keys as strings.
        """
        return {
            "dataset_version": self.dataset_identity["dataset_version"],
            "datasets": {
                name: {
                    "rows": st.rows,
                    "unique_keys": st.unique_keys,
                    "placements": dict(st.placements),
                    "files": st.files,
                    "bytes": st.bytes,
                }
                for name, st in sorted(self.stats.items())
            },
            "label_rows": self.label_rows,
            "label_positives": self.label_positives,
            "category_labels": {
                c: dict(counts) for c, counts in sorted(self.category_labels.items())
            },
            "orders_by_storefront": dict(self.orders_by_storefront),
            "sale_days": self.sale_days,
            "amazon": {
                name: {
                    "orders": self.amazon_orders[flag],
                    "lines": self.amazon_lines[flag],
                    "discount_sum": self.amazon_discount_sum[flag],
                    "labels": dict(self.amazon_labels[flag]),
                }
                for name, flag in (("sale", True), ("normal", False))
            },
        }

    def _restore(self, data: dict[str, Any]) -> None:
        """Reload the counters from the stamp; the inverse of `_counters`."""
        for name, values in data["datasets"].items():
            stats = self.stats[name]
            stats.rows = values["rows"]
            stats.unique_keys = values["unique_keys"]
            stats.placements = Counter(values["placements"])
            stats.files = values["files"]
            stats.bytes = values["bytes"]
        self.label_rows = data["label_rows"]
        self.label_positives = data["label_positives"]
        self.category_labels = defaultdict(
            Counter,
            {
                category: Counter({int(label): n for label, n in counts.items()})
                for category, counts in data["category_labels"].items()
            },
        )
        self.orders_by_storefront = Counter(data["orders_by_storefront"])
        self.sale_days = data["sale_days"]
        for name, flag in (("sale", True), ("normal", False)):
            values = data["amazon"][name]
            self.amazon_orders[flag] = values["orders"]
            self.amazon_lines[flag] = values["lines"]
            self.amazon_discount_sum[flag] = values["discount_sum"]
            self.amazon_labels[flag] = Counter(
                {int(label): n for label, n in values["labels"].items()}
            )

    def _write_skus(self) -> None:
        """Write the Seller's SKU catalog (with `category`) for loading into `seller.skus`."""
        rows = [
            {
                "sku": sku.sku_id,
                "name": sku.name,
                "category": sku.category,
                "base_price_usd": sku.base_price_usd,
                "unit_cost_usd": sku.unit_cost_usd,
            }
            for sku in self.simulator.catalog.skus
        ]
        stats = self.stats["seller.skus"]
        stats.rows = len(rows)
        stats.unique_keys = len({row["sku"] for row in rows})
        stats.bytes = self.store.write_jsonl("pg-load/seller.skus/part-00000.jsonl", rows)
        stats.files = 1

    def _prime_day_summary(self) -> dict[str, Any] | None:
        """Actual Sale Event measurements, split sale days / normal days, to compare with the config.

        The Return rate is `null` if the sale falls within the last 30 days of history.
        """
        sale = self.config.drift.prime_day
        if sale is None or not self.sale_days:
            return None
        normal_days = self.config.scale.days_history - self.sale_days

        def per_day(on_sale: bool) -> float:
            """Mean Amazon Orders per day for the sale-day or normal-day group."""
            days = self.sale_days if on_sale else normal_days
            return round(self.amazon_orders[on_sale] / days, 1) if days else 0.0

        def mean_discount(on_sale: bool) -> float:
            """Mean discount ratio per Order Line of the group."""
            lines = self.amazon_lines[on_sale]
            return round(self.amazon_discount_sum[on_sale] / lines, 4) if lines else 0.0

        def positive_rate(on_sale: bool) -> float | None:
            """None when no line of this group has a known label before `cutoff`."""
            counts = self.amazon_labels[on_sale]
            total = counts[0] + counts[1]
            return round(counts[1] / total, 6) if total else None

        return {
            "days": [
                self.config.day_start(sale.day + i).date().isoformat() for i in range(sale.days)
            ],
            "amazon_orders_per_day": {"sale": per_day(True), "normal": per_day(False)},
            "amazon_mean_discount_fraction": {
                "sale": mean_discount(True),
                "normal": mean_discount(False),
            },
            "amazon_positive_rate": {"sale": positive_rate(True), "normal": positive_rate(False)},
        }

    def _manifest(self) -> dict[str, Any]:
        """Collect the run's answer key: config, row counts, injected faults, labels."""
        n_labels = self.label_rows
        positives = self.label_positives
        switch_day = self.config.offline_problems.amazon_orders_api_switch_day
        return {
            "dataset_version": self.dataset_identity["dataset_version"],
            "generator_sha256": self.dataset_identity["generator_sha256"],
            "config": self.config.to_dict(),
            "cutoff": self.config.cutoff.isoformat(),
            "orders_by_storefront": dict(self.orders_by_storefront),
            "datasets": {name: s.summary() for name, s in sorted(self.stats.items())},
            "schema_evolution": {
                "amazon.orders": {
                    "new_schema": SCHEMA_2026,
                    "first_export_date": self.config.day_start(switch_day).date().isoformat(),
                },
            },
            "labels": {
                "rows": n_labels,
                "positive_rate": round(positives / n_labels, 6) if n_labels else 0.0,
                "positive_rate_by_category": {
                    c: round(counts[1] / (counts[0] + counts[1]), 6)
                    for c, counts in sorted(self.category_labels.items())
                },
            },
            "drift": {"prime_day": self._prime_day_summary()},
            "not_rendered_yet": sorted(
                {s.channel for s in STOREFRONTS.values()} - set(self.renderers)
            ),
            "placement_kinds": {
                PRIMARY: "primary copy",
                "identical_copy": "identical duplicate",
                "older_version": "older version with the same key",
            },
        }
