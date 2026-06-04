from typing import final

import attrs

from forze.application.integrations.queue import BaseQueueMessage

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class RabbitMQQueueMessage(BaseQueueMessage):
    """Raw RabbitMQ queue message envelope."""
