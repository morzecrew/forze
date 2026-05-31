from typing import Any, final

import attrs

from forze.application.contracts.queue import QueueMessage
from forze.base.serialization import RecordMappingCodec

from ..kernel.client import RabbitMQQueueMessage

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class RabbitMQQueueCodec[M]:
    """RabbitMQ queue payload codec backed by a record-mapping codec."""

    payload_codec: RecordMappingCodec[M, Any]
    """Codec for queue message payloads."""

    # ....................... #

    def encode(self, payload: M) -> bytes:
        """Encode a payload into a bytes string."""

        return self.payload_codec.encode_json_bytes(payload)

    # ....................... #

    def decode(self, queue: str, raw: RabbitMQQueueMessage) -> QueueMessage[M]:
        """Decode a raw RabbitMQ queue message into a QueueMessage."""

        body = raw["body"]

        return QueueMessage(
            queue=queue,
            id=raw["id"],
            payload=self.payload_codec.decode_json_bytes(body),
            type=raw.get("type"),
            enqueued_at=raw.get("enqueued_at"),
            key=raw.get("key"),
        )
