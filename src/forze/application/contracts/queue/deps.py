from typing import Any

from ..base import BaseDepPort, DepKey
from .ports import QueueReadPort, QueueWritePort
from .specs import QueueSpec

# ----------------------- #

QueueReadDepPort = BaseDepPort[QueueSpec[Any], QueueReadPort[Any]]
"""Queue read dependency port."""

QueueWriteDepPort = BaseDepPort[QueueSpec[Any], QueueWritePort[Any]]
"""Queue write dependency port."""

QueueReadDepKey = DepKey[QueueReadDepPort]("queue_read")
"""Key used to register the :class:`QueueReadPort` builder implementation."""

QueueWriteDepKey = DepKey[QueueWriteDepPort]("queue_write")
"""Key used to register the :class:`QueueWritePort` builder implementation."""
