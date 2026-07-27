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
        # Whole seconds, not merely positive: AMQP carries the heartbeat as an integer
        # number of seconds, and 0 means *disabled*. A sub-second value passed a
        # "must be positive" check and then truncated to 0 on the way to the URL — the
        # tightest-looking setting silently turning heartbeats off. There is no way to ask
        # for 0 deliberately here, so nothing is lost by refusing everything below 1s.
        if self.heartbeat < timedelta(seconds=1):
            raise exc.configuration(
                "Heartbeat must be at least 1s: AMQP carries it as whole seconds, and a "
                "sub-second value truncates to 0, which the broker reads as disabled."
            )

        if self.heartbeat % timedelta(seconds=1):
            raise exc.configuration(
                f"Heartbeat must be a whole number of seconds, got {self.heartbeat}: the "
                "fractional part is dropped on the wire rather than rounded."
            )

        if self.connect_timeout.total_seconds() <= 0:
            raise exc.configuration("Connect timeout must be positive")

        if self.pending_watermark <= 0:
            raise exc.configuration("Pending watermark must be positive")

        if self.redelivery_counting and not self.publisher_confirms:
            # Counted requeue republishes the message and then acks the original. Without
            # publisher confirms the republish is fire-and-forget, so a publish that never
            # reaches the broker would still ack the original — dropping the message. The
            # publish-then-ack at-least-once guarantee only holds under publisher confirms.
            raise exc.configuration(
                "redelivery_counting requires publisher_confirms",
            )
