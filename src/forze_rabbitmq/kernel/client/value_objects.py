from datetime import timedelta
from typing import final

import attrs

from forze.base.exceptions import exc

# ----------------------- #


@final
@attrs.define(frozen=True, slots=True, kw_only=True)
class RabbitMQConfig:
    heartbeat: timedelta = timedelta(seconds=60)
    connect_timeout: timedelta = timedelta(seconds=5)
    queue_durable: bool = attrs.field(default=True)
    persistent_messages: bool = attrs.field(default=True)
    publisher_confirms: bool = attrs.field(default=True)
    prefetch_count: int = 100
    pending_watermark: int = 10_000
    """Soft watermark for the unacked pending-delivery map.

    A *warning threshold*, not a hard cap: rejecting or dropping deliveries
    past a cap would silently lose messages or surprise consumers, and in
    healthy operation the map is naturally bounded by the channel prefetch.
    Growth past the watermark therefore indicates leaked deliveries —
    typically handlers that crashed between receive and ack/nack — which
    the warning surfaces without changing delivery behavior.
    """

    dead_letter_exchange: str | None = attrs.field(default=None)
    """Opt-in poison sink. When set, work queues are declared with this DLX
    (``x-dead-letter-exchange``), and a fanout exchange of this name plus a bound
    durable dead-letter queue (``<dlx>.dlq``) are declared on first use — so a
    ``nack(requeue=False)`` (an undecodable / schema-drift message) dead-letters there
    instead of being silently dropped. Default ``None`` keeps the current
    drop-on-reject behaviour. **Enabling it on a pre-existing queue requires recreating
    that queue** — AMQP queue arguments are immutable, so re-declaring with a new DLX
    fails with ``PRECONDITION_FAILED``.
    """

    redelivery_counting: bool = attrs.field(default=False)
    """Opt-in per-message redelivery counting. When ``True``, ``nack(requeue=True)``
    republishes the message with an incremented ``x-forze-delivery`` header and acks the
    original (instead of a plain broker requeue), so the delivery count survives the requeue
    and ``max_deliveries >= 2`` poison-parking actually fires — a plain requeue never advances
    the count past the broker's ``redelivered``-flag ceiling of ``2``. Consumer inbox dedup
    covers the brief republish→ack crash window (the message id is preserved). Default ``False``
    keeps in-place requeue (original position/order).
    """

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.heartbeat.total_seconds() <= 0:
            raise exc.configuration("Heartbeat must be positive")

        if self.connect_timeout.total_seconds() <= 0:
            raise exc.configuration("Connect timeout must be positive")

        if self.pending_watermark <= 0:
            raise exc.configuration("Pending watermark must be positive")
