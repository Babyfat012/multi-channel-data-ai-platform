"""Stream delivery layer: turns status changes after `cutoff` into messages.

Each time an Amazon or TikTok Shop Order changes Order Status within
`[cutoff, stream_end)`, one message is generated. This layer decides **when** the message
arrives (`arrive_at`) and whether it is sent twice. Bursts do not live here: they come from
the simulation core (the livestream raises the Order count); this layer only delivers them.

| Problem | Channel | How it is injected |
|---|---|---|
| Late | Amazon | `share_over_1h` of messages are 1 to 6 hours late, the rest are delayed by a log-normal distribution with median `median_minutes`, always under 1 hour |
| Duplicate | TikTok Shop | With probability `p = r / (1 - r)`, the message is resent after 2 or 30 minutes (TikTok's retry schedule), keeping the same `tts_notification_id` |

Formula for `p`: with n original messages, each getting one extra copy with probability p,
the total is n(1 + p) and the duplicate rate is `1 - n / (n(1 + p)) = p / (1 + p)`. Set it equal to r
and solve for `p = r / (1 - r)`.

Messages are yielded in `arrive_at` order, i.e. the order Kafka receives them. Late Amazon
messages therefore land after messages with a later `event_time`: they arrive out of order.

Keeping RAM low: the core generates Orders by creation day. An Order created on day d only has
status changes from day d on, and a message always arrives after its status change. So once
day d is generated, every message arriving before 00:00 of day d + 1 is fully known and can be
emitted. Two priority queues (heaps) hold only what is not due yet.
"""

from __future__ import annotations

import heapq
import itertools
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

import numpy as np

from generator.channels.amazon import AmazonOrderNotificationRenderer
from generator.channels.tiktok import TikTokOrderWebhookRenderer
from generator.config import GeneratorConfig
from generator.core.model import Order, StatusChange
from generator.core.rngs import rng_for
from generator.core.simulate import Simulator
from generator.core.storefronts import STOREFRONTS

TOPICS = {"amazon": "amazon.order-notifications", "tiktok_shop": "tiktok_shop.order-webhooks"}

# The longest Order lifetime is about 17 days (payment, up to 3 days of preparation, 6 days
# in transit, 7 days to completion). Regenerating that many days before the Cutoff is enough to
# catch every Order that still changes status after the Cutoff.
LOOKBACK_DAYS = 20
LATE_SIGMA = 0.8  # spread of the slightly late part (log-normal)
LATE_OVER_1H_MINUTES = (60, 360)
TIKTOK_DELAY_SECONDS = 5  # mean delay of a normal webhook
TIKTOK_RETRY_MINUTES = (2, 30)  # TikTok Shop's first two retries
TIKTOK_RETRY_WEIGHTS = (0.8, 0.2)


@dataclass(frozen=True)
class StreamMessage:
    """A message to send to Kafka.

    Attributes:
        topic: topic Kafka, theo Channel.
        key: the Kafka key, the native order id, so an Order's messages go to the same partition.
        dedup_key: the notification id, the key Flink uses to drop duplicates.
        value: JSON content in the Channel's native shape.
        storefront_id, event_time, arrive_at: used for measurement, not sent.
        is_duplicate: True if it is a resend.
    """

    topic: str
    key: str
    dedup_key: str
    value: dict[str, Any]
    storefront_id: str
    event_time: datetime
    arrive_at: datetime
    is_duplicate: bool = False


def stream_messages(config: GeneratorConfig) -> Iterator[StreamMessage]:
    """Generate every Stream message, in arrival order."""
    sim = Simulator(config)
    renderers = {
        "amazon": AmazonOrderNotificationRenderer(config),
        "tiktok_shop": TikTokOrderWebhookRenderer(config),
    }
    rng = rng_for(config.seed, "stream_delivery")
    seq = itertools.count()  # heap tie-breaker that keeps the order reproducible

    events: list[tuple[datetime, int, Order, StatusChange]] = []
    messages: list[tuple[datetime, int, StreamMessage]] = []

    first_day = max(1, config.scale.days_history - LOOKBACK_DAYS + 1)
    last_day = config.scale.days_history + config.scale.days_stream
    for day in range(first_day, last_day + 1):
        for order in sim.simulate_day(day):
            if STOREFRONTS[order.storefront_id].channel not in renderers:
                continue
            for change in order.timeline:
                if config.cutoff <= change.at < config.stream_end:
                    heapq.heappush(events, (change.at, next(seq), order, change))

        known_until = config.day_start(day + 1)
        while events and events[0][0] < known_until:
            _, _, order, change = heapq.heappop(events)
            for msg in _deliver(config, renderers, rng, order, change):
                heapq.heappush(messages, (msg.arrive_at, next(seq), msg))
        while messages and messages[0][0] < known_until:
            yield heapq.heappop(messages)[2]

    while messages:
        yield heapq.heappop(messages)[2]


def _deliver(
    config: GeneratorConfig,
    renderers: dict[str, Any],
    rng: np.random.Generator,
    order: Order,
    change: StatusChange,
) -> list[StreamMessage]:
    """Render one status change, then decide its arrival time and duplicate."""
    channel = STOREFRONTS[order.storefront_id].channel
    rendered = renderers[channel].render_event(order, change)
    if channel == "amazon":
        arrive = change.at + _amazon_delay(config, rng)
        value = rendered.record
        value["NotificationMetadata"]["PublishTime"] = arrive.strftime("%Y-%m-%dT%H:%M:%SZ")
        key = value["Payload"]["OrderChangeNotification"]["AmazonOrderId"]
    else:
        arrive = change.at + timedelta(seconds=float(rng.exponential(TIKTOK_DELAY_SECONDS)))
        value = rendered.record
        key = value["data"]["order_id"]

    original = StreamMessage(
        TOPICS[channel], key, rendered.key, value, order.storefront_id, change.at, arrive
    )
    out = [original]
    rate = config.streaming_problems.duplicate_rate.get(order.storefront_id, 0.0)
    if rate and rng.random() < rate / (1 - rate):
        retry = float(rng.choice(TIKTOK_RETRY_MINUTES, p=TIKTOK_RETRY_WEIGHTS))
        out.append(
            StreamMessage(
                original.topic,
                key,
                rendered.key,
                value,
                order.storefront_id,
                change.at,
                arrive + timedelta(minutes=retry),
                is_duplicate=True,
            )
        )
    return out


def _amazon_delay(config: GeneratorConfig, rng: np.random.Generator) -> timedelta:
    """Delay of one Amazon notification."""
    late = config.streaming_problems.amazon_late
    if rng.random() < late.share_over_1h:
        return timedelta(minutes=float(rng.uniform(*LATE_OVER_1H_MINUTES)))
    minutes = late.median_minutes * float(np.exp(LATE_SIGMA * rng.standard_normal()))
    return timedelta(minutes=min(minutes, 59.9))


def wall_offset(config: GeneratorConfig, t: datetime) -> float:
    """Real seconds from the start of emitting until emitting the message arriving at `t`.

    Each simulated day takes `seconds_per_day(day)` real seconds, so the livestream
    day emits more slowly and the burst lasts long enough to see in the Flink UI.
    """
    pacing = config.streaming_problems.pacing
    elapsed = (t - config.cutoff).total_seconds() / 86_400
    first = config.scale.days_history + 1
    whole = int(elapsed)
    total = sum(pacing.seconds_per_day(first + i) for i in range(whole))
    return total + (elapsed - whole) * pacing.seconds_per_day(first + whole)
