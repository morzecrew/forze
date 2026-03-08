from typing import TYPE_CHECKING, Protocol, TypeVar, runtime_checkable

from pydantic import BaseModel

from ..deps import DepKey
from .ports import PubSubPublishPort, PubSubSubscribePort
from .specs import PubSubSpec

if TYPE_CHECKING:
    from forze.application.execution.context import ExecutionContext

# ----------------------- #

M = TypeVar("M", bound=BaseModel)

# ....................... #


@runtime_checkable
class PubSubPublishDepPort(Protocol):
    def __call__(
        self,
        context: "ExecutionContext",
        spec: PubSubSpec[M],
    ) -> PubSubPublishPort[M]: ...


# ....................... #


@runtime_checkable
class PubSubSubscribeDepPort(Protocol):
    def __call__(
        self,
        context: "ExecutionContext",
        spec: PubSubSpec[M],
    ) -> PubSubSubscribePort[M]: ...


# ....................... #

PubSubPublishDepKey = DepKey[PubSubPublishDepPort]("pubsub_publish")
PubSubSubscribeDepKey = DepKey[PubSubSubscribeDepPort]("pubsub_subscribe")
