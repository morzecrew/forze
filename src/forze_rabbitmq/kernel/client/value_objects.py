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

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.heartbeat.total_seconds() <= 0:
            raise exc.configuration("Heartbeat must be positive")

        if self.connect_timeout.total_seconds() <= 0:
            raise exc.configuration("Connect timeout must be positive")

        if self.pending_watermark <= 0:
            raise exc.configuration("Pending watermark must be positive")
