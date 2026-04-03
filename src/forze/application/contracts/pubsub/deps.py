from typing import Any

from ..base import BaseDepPort, DepKey
from .ports import PubSubPublishPort, PubSubSubscribePort
from .specs import PubSubSpec

# ----------------------- #

PubSubPublishDepKey = DepKey[
    BaseDepPort[
        PubSubSpec[Any],
        PubSubPublishPort[Any],
    ]
]("pubsub_publish")
"""Key used to register the :class:`PubSubPublishPort` builder implementation."""

PubSubSubscribeDepKey = DepKey[
    BaseDepPort[
        PubSubSpec[Any],
        PubSubSubscribePort[Any],
    ]
]("pubsub_subscribe")
"""Key used to register the :class:`PubSubSubscribePort` builder implementation."""
