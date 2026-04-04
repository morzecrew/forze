from typing import Any

from ..base import BaseDepPort, DepKey
from .ports import QueueCommandPort, QueueQueryPort
from .specs import QueueSpec

# ----------------------- #

QueueQueryDepPort = BaseDepPort[QueueSpec[Any], QueueQueryPort[Any]]
"""Queue query dependency port."""

QueueCommandDepPort = BaseDepPort[QueueSpec[Any], QueueCommandPort[Any]]
"""Queue command dependency port."""

QueueQueryDepKey = DepKey[QueueQueryDepPort]("queue_query")
"""Key used to register the :class:`QueueQueryPort` builder implementation."""

QueueCommandDepKey = DepKey[QueueCommandDepPort]("queue_command")
"""Key used to register the :class:`QueueCommandPort` builder implementation."""
