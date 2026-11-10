"""Read and validate the generator's YAML config file.

Each YAML section (`scale`, `labels`, ...) maps to a dataclass here.
Reading into dataclasses instead of raw dicts has two benefits:
- A mistyped key (e.g. `order_per_day`) fails immediately instead of being silently ignored.
- Downstream code writes `config.scale.orders_per_day`, which reads well and gets IDE completion.

Only keys the generator already uses are present. Keys for parts not yet built
will be added when those parts are built.
"""

from __future__ import annotations

import dataclasses
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any

import yaml

from generator.core.storefronts import DEFAULT_STOREFRONT_WEIGHTS, STOREFRONTS


@dataclass(frozen=True)
class ScaleConfig:
    """Data scale: number of SKUs, number of days, Orders per day."""

    n_skus: int = 2000
    start_date: date = date(2026, 1, 1)
    days_history: int = 180
    days_stream: int = 30
    orders_per_day: int = 4000
    sku_zipf_exponent: float = 0.8


@dataclass(frozen=True)
class LabelConfig:
    """Parameters for generating Return/Refund labels."""

    window_days: int = 30


@dataclass(frozen=True)
class OfflineProblemsConfig:
    """Data problems injected into the Batch."""

    duplicate_rate: float = 0.02
    overlap_days: int = 3
    amazon_orders_api_switch_day: int = 100


@dataclass(frozen=True)
class PacingSegment:
    """Simulated days `from_day` to `to_day` (inclusive) emitted at their own pace."""

    from_day: int
    to_day: int
    seconds_per_sim_day: float


@dataclass(frozen=True)
class PacingConfig:
    """Stream emit pace: how many real seconds per simulated day."""

    default_seconds_per_sim_day: float = 60.0
    segments: tuple[PacingSegment, ...] = ()

    def seconds_per_day(self, day: int) -> float:
        """Real seconds spent on simulated day `day`."""
        for seg in self.segments:
            if seg.from_day <= day <= seg.to_day:
                return seg.seconds_per_sim_day
        return self.default_seconds_per_sim_day


@dataclass(frozen=True)
class AmazonLateConfig:
    """Delay of Amazon notifications.

    Attributes:
        median_minutes: median delay of the slightly late messages (under 1 hour).
        share_over_1h: share of messages delayed more than 1 hour.
    """

    median_minutes: float = 5.0
    share_over_1h: float = 0.05


@dataclass(frozen=True)
class StreamingProblemsConfig:
    """Data problems injected into the Stream.

    `duplicate_rate` per Storefront, measured as `1 - count(distinct key) / count(*)`.
    """

    pacing: PacingConfig = field(default_factory=PacingConfig)
    duplicate_rate: dict[str, float] = field(default_factory=lambda: {"tiktok_shop_us": 0.015})
    amazon_late: AmazonLateConfig = field(default_factory=AmazonLateConfig)


@dataclass(frozen=True)
class PrimeDayConfig:
    """A Prime Day style Sale Event on the Amazon Storefronts.

    Attributes:
        day: simulated day the sale starts.
        days: how many days the sale lasts.
        order_multiplier: how many times a normal day's Orders the Amazon Storefronts get.
        discount_share: share of Amazon Order Lines discounted on sale days.
        deal_skus: number of promoted SKUs that draw most of the demand on sale days.
    """

    day: int = 120
    days: int = 2
    order_multiplier: float = 4.0
    discount_share: float = 0.7
    deal_skus: int = 50

    def covers(self, day: int) -> bool:
        """Whether simulated day `day` falls within the sale."""
        return self.day <= day < self.day + self.days


@dataclass(frozen=True)
class TikTokLiveConfig:
    """A TikTok Shop livestream after `cutoff` that causes a burst in the Stream.

    Attributes:
        day: simulated day of the livestream.
        start_hour_utc: start hour (UTC).
        duration_hours: how many hours it lasts.
        burst_multiplier: range of the TikTok Shop Order multiplier. The actual
            multiplier is drawn once per run and written to the manifest as the answer key.
    """

    day: int = 195
    start_hour_utc: float = 12.0
    duration_hours: float = 6.0
    burst_multiplier: tuple[float, float] = (5.0, 8.0)

    def window(self, day_start: datetime) -> tuple[datetime, datetime]:
        """Livestream time window, given that `day_start` is 00:00 of `day`."""
        start = day_start + timedelta(hours=self.start_hour_utc)
        return start, start + timedelta(hours=self.duration_hours)


@dataclass(frozen=True)
class DriftConfig:
    """Scheduled events that change the core's parameters. `None` means none."""

    prime_day: PrimeDayConfig | None = None
    tiktok_live: TikTokLiveConfig | None = None


@dataclass(frozen=True)
class StorageConfig:
    """Where data is written.

    `output_root` is an fsspec URL: a local directory path (`./out`)
    or an S3/MinIO bucket (`s3://...`). The writing code does not need to know which.
    """

    output_root: str = "./out"


@dataclass(frozen=True)
class GeneratorConfig:
    """Full configuration of one run."""

    seed: int = 42
    scale: ScaleConfig = field(default_factory=ScaleConfig)
    storefront_weights: dict[str, float] = field(
        default_factory=lambda: dict(DEFAULT_STOREFRONT_WEIGHTS)
    )
    labels: LabelConfig = field(default_factory=LabelConfig)
    offline_problems: OfflineProblemsConfig = field(default_factory=OfflineProblemsConfig)
    streaming_problems: StreamingProblemsConfig = field(default_factory=StreamingProblemsConfig)
    drift: DriftConfig = field(default_factory=DriftConfig)
    storage: StorageConfig = field(default_factory=StorageConfig)

    # --- Timestamps derived from the config ------------------------------

    def day_start(self, day: int) -> datetime:
        """Return 00:00 UTC of simulated day `day` (day 1 is `start_date`)."""
        d = self.scale.start_date + timedelta(days=day - 1)
        return datetime.combine(d, time(0, 0), tzinfo=timezone.utc)

    @property
    def cutoff(self) -> datetime:
        """The "today" mark: end of the last history day, i.e. 00:00 UTC of the next day."""
        return self.day_start(self.scale.days_history + 1)

    @property
    def stream_end(self) -> datetime:
        """End of the last Stream day."""
        return self.day_start(self.scale.days_history + self.scale.days_stream + 1)

    def to_dict(self) -> dict[str, Any]:
        """Convert the config to a plain dict to write into `run_manifest.json`."""
        return _jsonable(dataclasses.asdict(self))


def load_config(path: str | Path) -> GeneratorConfig:
    """Read a YAML file and return a validated `GeneratorConfig`.

    Keys missing from the YAML take the dataclass defaults.
    Unknown keys raise `ValueError` to catch typos early.
    """
    raw = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
    return parse_config(raw)


def parse_config(raw: dict[str, Any]) -> GeneratorConfig:
    """Build a `GeneratorConfig` from a dict (read from YAML), then validate the values."""
    sections = {
        "scale": ScaleConfig,
        "labels": LabelConfig,
        "offline_problems": OfflineProblemsConfig,
        "storage": StorageConfig,
    }
    _reject_unknown(
        raw, {"seed", "storefront_weights", "drift", "streaming_problems", *sections}, where="root"
    )

    kwargs: dict[str, Any] = {}
    if "seed" in raw:
        kwargs["seed"] = int(raw["seed"])
    for name, cls in sections.items():
        if name in raw:
            section = raw[name] or {}
            _reject_unknown(section, {f.name for f in dataclasses.fields(cls)}, where=name)
            kwargs[name] = cls(**section)
    if "storefront_weights" in raw:
        kwargs["storefront_weights"] = {k: float(v) for k, v in raw["storefront_weights"].items()}
    if "drift" in raw:
        kwargs["drift"] = _parse_drift(raw["drift"] or {})
    if "streaming_problems" in raw:
        kwargs["streaming_problems"] = _parse_streaming(raw["streaming_problems"] or {})

    config = GeneratorConfig(**kwargs)
    _validate(config)
    return config


def _parse_drift(raw: dict[str, Any]) -> DriftConfig:
    """Read the `drift` section; each event is a nested dataclass."""
    _reject_unknown(raw, {"prime_day", "tiktok_live"}, where="drift")
    kwargs: dict[str, Any] = {}
    if raw.get("prime_day") is not None:
        kwargs["prime_day"] = _section(raw["prime_day"], PrimeDayConfig, "drift.prime_day")
    if raw.get("tiktok_live") is not None:
        live = dict(raw["tiktok_live"])
        if "burst_multiplier" in live:
            live["burst_multiplier"] = tuple(float(x) for x in live["burst_multiplier"])
        kwargs["tiktok_live"] = _section(live, TikTokLiveConfig, "drift.tiktok_live")
    return DriftConfig(**kwargs)


def _parse_streaming(raw: dict[str, Any]) -> StreamingProblemsConfig:
    """Read the `streaming_problems` section."""
    _reject_unknown(
        raw,
        {f.name for f in dataclasses.fields(StreamingProblemsConfig)},
        where="streaming_problems",
    )
    kwargs: dict[str, Any] = {}
    if "pacing" in raw:
        pacing = dict(raw["pacing"] or {})
        pacing["segments"] = tuple(
            _section(s, PacingSegment, "streaming_problems.pacing.segments")
            for s in pacing.get("segments") or ()
        )
        kwargs["pacing"] = _section(pacing, PacingConfig, "streaming_problems.pacing")
    if "duplicate_rate" in raw:
        kwargs["duplicate_rate"] = {k: float(v) for k, v in (raw["duplicate_rate"] or {}).items()}
    if "amazon_late" in raw:
        kwargs["amazon_late"] = _section(
            raw["amazon_late"] or {}, AmazonLateConfig, "streaming_problems.amazon_late"
        )
    return StreamingProblemsConfig(**kwargs)


def _section(raw: dict[str, Any], cls: type, where: str) -> Any:
    """Build dataclass `cls` from `raw`, raising on unknown keys."""
    _reject_unknown(raw, {f.name for f in dataclasses.fields(cls)}, where=where)
    return cls(**raw)


def _reject_unknown(section: dict[str, Any], allowed: set[str], where: str) -> None:
    """Raise if `section` has keys not in `allowed`."""
    unknown = set(section) - allowed
    if unknown:
        raise ValueError(f"Invalid keys in section '{where}': {sorted(unknown)}")


def _validate(config: GeneratorConfig) -> None:
    """Check constraints between config values."""
    weights = config.storefront_weights
    unknown = set(weights) - set(STOREFRONTS)
    if unknown:
        raise ValueError(f"Storefront does not exist: {sorted(unknown)}")
    if abs(sum(weights.values()) - 1.0) > 1e-6:
        raise ValueError(f"storefront_weights must sum to 1, got {sum(weights.values())}")
    if not 0 <= config.offline_problems.duplicate_rate < 0.5:
        raise ValueError("duplicate_rate must be in [0, 0.5)")
    if (
        not 1
        <= config.offline_problems.amazon_orders_api_switch_day
        <= config.scale.days_history + 1
    ):
        raise ValueError("amazon_orders_api_switch_day must be within the history day range")

    sale = config.drift.prime_day
    if sale is not None:
        if sale.days < 1 or not 1 <= sale.day <= config.scale.days_history - sale.days + 1:
            raise ValueError("drift.prime_day must lie entirely within the history day range")
        if sale.order_multiplier < 1:
            raise ValueError("drift.prime_day.order_multiplier must be at least 1")
        if not 0 <= sale.discount_share <= 1:
            raise ValueError("drift.prime_day.discount_share must be in [0, 1]")
        if not 1 <= sale.deal_skus <= config.scale.n_skus:
            raise ValueError("drift.prime_day.deal_skus must be between 1 and n_skus")

    first_stream_day = config.scale.days_history + 1
    last_stream_day = config.scale.days_history + config.scale.days_stream
    if config.scale.days_stream < 1:
        raise ValueError("days_stream must be at least 1")
    live = config.drift.tiktok_live
    if live is not None:
        if not first_stream_day <= live.day <= last_stream_day:
            raise ValueError("drift.tiktok_live.day must be within the Stream day range")
        if not (
            live.start_hour_utc >= 0
            and live.duration_hours > 0
            and live.start_hour_utc + live.duration_hours <= 24
        ):
            raise ValueError("drift.tiktok_live must lie entirely within one day")
        low, high = live.burst_multiplier
        if not 1 <= low <= high:
            raise ValueError(
                "drift.tiktok_live.burst_multiplier must be [low, high] with 1 <= low <= high"
            )
    streaming = config.streaming_problems
    unknown = set(streaming.duplicate_rate) - set(STOREFRONTS)
    if unknown:
        raise ValueError(
            f"streaming_problems.duplicate_rate has a Storefront that does not exist: {sorted(unknown)}"
        )
    if not all(0 <= r < 0.5 for r in streaming.duplicate_rate.values()):
        raise ValueError("streaming_problems.duplicate_rate must be in [0, 0.5)")
    late = streaming.amazon_late
    if not 0 < late.median_minutes < 60 or not 0 <= late.share_over_1h < 1:
        raise ValueError("amazon_late needs 0 < median_minutes < 60 and 0 <= share_over_1h < 1")
    if streaming.pacing.default_seconds_per_sim_day <= 0 or any(
        s.seconds_per_sim_day <= 0 or s.from_day > s.to_day for s in streaming.pacing.segments
    ):
        raise ValueError("streaming_problems.pacing needs a positive pace and from_day <= to_day")


def _jsonable(value: Any) -> Any:
    """Convert `date`/`datetime` to ISO strings so `json.dumps` can write them."""
    if isinstance(value, dict):
        return {k: _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return value
