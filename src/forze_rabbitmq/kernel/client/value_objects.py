from datetime import timedelta
from typing import final

import attrs

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
