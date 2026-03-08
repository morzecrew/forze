from datetime import datetime
from typing import (
    AsyncIterator,
    Awaitable,
    Optional,
    Protocol,
    Sequence,
    runtime_checkable,
)

from pydantic import BaseModel

from .types import PubSubMessage

# ----------------------- #


@runtime_checkable
class PubSubPublishPort[M: BaseModel](Protocol):
    def publish(
        self,
        topic: str,  # noqa: F841
        payload: M,
        *,
        type: Optional[str] = None,
        key: Optional[str] = None,
        published_at: Optional[datetime] = None,
    ) -> Awaitable[None]: ...


# ....................... #


@runtime_checkable
class PubSubSubscribePort[M: BaseModel](Protocol):
    def subscribe(
        self,
        topics: Sequence[str],  # noqa: F841
    ) -> AsyncIterator[PubSubMessage[M]]: ...
