from typing import Any

from ..base import ConfigurableDepPort, DepKey
from .ports import QueueCommandPort, QueueQueryPort
from .specs import QueueSpec

# ----------------------- #

QueueQueryDepPort = ConfigurableDepPort[QueueSpec[Any], QueueQueryPort[Any]]
"""Queue query dependency port."""

QueueCommandDepPort = ConfigurableDepPort[QueueSpec[Any], QueueCommandPort[Any]]
"""Queue command dependency port."""

QueueQueryDepKey = DepKey[QueueQueryDepPort]("queue_query")
"""Key used to register the :class:`QueueQueryPort` builder implementation."""

QueueCommandDepKey = DepKey[QueueCommandDepPort]("queue_command")
"""Key used to register the :class:`QueueCommandPort` builder implementation."""
