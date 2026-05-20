from typing import Any

from ..base import ConfigurableDepPort, DepKey
from .ports import PubSubCommandPort, PubSubQueryPort
from .specs import PubSubSpec

# ----------------------- #

PubSubQueryDepPort = ConfigurableDepPort[PubSubSpec[Any], PubSubQueryPort[Any]]
"""Pubsub query dependency port."""

PubSubCommandDepPort = ConfigurableDepPort[PubSubSpec[Any], PubSubCommandPort[Any]]
"""Pubsub command dependency port."""

PubSubQueryDepKey = DepKey[PubSubQueryDepPort]("pubsub_query")
"""Key used to register the :class:`PubSubQueryPort` builder implementation."""

PubSubCommandDepKey = DepKey[PubSubCommandDepPort]("pubsub_command")
"""Key used to register the :class:`PubSubCommandPort` builder implementation."""
