from datetime import datetime
from typing import final

import attrs

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class RabbitMQQueueMessage:
    queue: str
    id: str
    body: bytes
    type: str | None = None
    enqueued_at: datetime | None = None
    key: str | None = None
