from typing import Any, Protocol

from .deps import QueueReadDepPort, QueueWriteDepPort
from .ports import QueueReadPort, QueueWritePort

# ----------------------- #


class QueueConformity(QueueReadPort[Any], QueueWritePort[Any], Protocol):
    """Conformity protocol used only to ensure that implementation conforms to
    :class:`QueueReadPort` and :class:`QueueWritePort` simultaneously.
    """


class QueueDepConformity(QueueReadDepPort, QueueWriteDepPort, Protocol):
    """Conformity protocol used only to ensure that implementation conforms to
    :class:`QueueReadDepPort` and :class:`QueueWriteDepPort` simultaneously.
    """
