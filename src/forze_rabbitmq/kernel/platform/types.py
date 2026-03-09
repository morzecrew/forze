from datetime import datetime
from typing import Optional, TypedDict

# ----------------------- #


class RabbitMQQueueMessage(TypedDict):
    queue: str
    id: str
    body: bytes
    type: Optional[str]
    enqueued_at: Optional[datetime]
    key: Optional[str]
