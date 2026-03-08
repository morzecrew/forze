from typing import Any, Protocol

from .deps import PubSubPublishDepPort, PubSubSubscribeDepPort
from .ports import PubSubPublishPort, PubSubSubscribePort

# ----------------------- #


class PubSubConformity(PubSubPublishPort[Any], PubSubSubscribePort[Any], Protocol):
    """Conformity protocol used only to ensure that implementation conforms to
    :class:`PubSubPublishPort` and :class:`PubSubSubscribePort` simultaneously.
    """


class PubSubDepConformity(PubSubPublishDepPort, PubSubSubscribeDepPort, Protocol):
    """Conformity protocol used only to ensure that implementation conforms to
    :class:`PubSubPublishDepPort` and :class:`PubSubSubscribeDepPort` simultaneously.
    """
