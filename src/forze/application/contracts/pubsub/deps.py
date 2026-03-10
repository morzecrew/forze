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
    """Factory protocol for building :class:`PubSubPublishPort` instances."""

    def __call__(
        self,
        context: "ExecutionContext",
        spec: PubSubSpec[M],
    ) -> PubSubPublishPort[M]:
        """Build a pubsub publish port bound to the given context and spec."""
        ...


# ....................... #


@runtime_checkable
class PubSubSubscribeDepPort(Protocol):
    """Factory protocol for building :class:`PubSubSubscribePort` instances."""

    def __call__(
        self,
        context: "ExecutionContext",
        spec: PubSubSpec[M],
    ) -> PubSubSubscribePort[M]:
        """Build a pubsub subscribe port bound to the given context and spec."""
        ...


# ....................... #

PubSubPublishDepKey = DepKey[PubSubPublishDepPort]("pubsub_publish")
"""Key used to register the :class:`PubSubPublishDepPort` implementation."""

PubSubSubscribeDepKey = DepKey[PubSubSubscribeDepPort]("pubsub_subscribe")
"""Key used to register the :class:`PubSubSubscribeDepPort` implementation."""
