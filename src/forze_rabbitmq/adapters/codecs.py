from typing import final

import attrs
from pydantic import BaseModel

from forze.application.contracts.queue import QueueMessage

from ..kernel.platform import RabbitMQQueueMessage

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class RabbitMQQueueCodec[M: BaseModel]:
    """RabbitMQ queue codec."""

    model: type[M]
    """Pydantic model type of payloads."""

    # ....................... #

    def encode(self, payload: M) -> bytes:
        """Encode a payload into a bytes string."""

        return payload.model_dump_json().encode("utf-8")

    # ....................... #

    def decode(self, queue: str, raw: RabbitMQQueueMessage) -> QueueMessage[M]:
        """Decode a raw RabbitMQ queue message into a QueueMessage."""

        body = raw["body"]

        return QueueMessage(
            queue=queue,
            id=raw["id"],
            payload=self.model.model_validate_json(body),
            type=raw.get("type"),
            enqueued_at=raw.get("enqueued_at"),
            key=raw.get("key"),
        )
