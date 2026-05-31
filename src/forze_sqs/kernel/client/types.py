from datetime import datetime
from typing import TypedDict

# ----------------------- #


class SQSQueueMessage(TypedDict):
    queue: str
    id: str
    body: bytes
    type: str | None
    enqueued_at: datetime | None
    key: str | None
