from typing import Any, final

import attrs

from forze.application.contracts.queue import QueueMessage
from forze.base.serialization import RecordMappingCodec

from ..kernel.client import SQSQueueMessage

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class SQSQueueCodec[M]:
    """SQS queue payload codec backed by a record-mapping codec."""

    payload_codec: RecordMappingCodec[M, Any]
    """Codec for queue message payloads."""

    # ....................... #

    def encode(self, payload: M) -> bytes:
        return self.payload_codec.encode_json_bytes(payload)

    # ....................... #

    def decode(self, queue: str, raw: SQSQueueMessage) -> QueueMessage[M]:
        return QueueMessage(
            queue=queue,
            id=raw.id,
            payload=self.payload_codec.decode_json_bytes(raw.body),
            type=raw.type,
            enqueued_at=raw.enqueued_at,
            key=raw.key,
        )
