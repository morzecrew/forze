from typing import final

import attrs
from pydantic import BaseModel

from forze.application.contracts.queue import QueueMessage

from ..kernel.platform import SQSQueueMessage

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class SQSQueueCodec[M: BaseModel]:
    model: type[M]

    # ....................... #

    def encode(self, payload: M) -> bytes:
        return payload.model_dump_json().encode("utf-8")

    # ....................... #

    def decode(self, queue: str, raw: SQSQueueMessage) -> QueueMessage[M]:
        body = raw["body"]

        return QueueMessage(
            queue=queue,
            id=raw["id"],
            payload=self.model.model_validate_json(body),
            type=raw.get("type"),
            enqueued_at=raw.get("enqueued_at"),
            key=raw.get("key"),
        )
