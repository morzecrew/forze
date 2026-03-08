from typing import Any, Protocol

from .deps import StreamReadDepPort, StreamWriteDepPort
from .ports import StreamReadPort, StreamWritePort

# ----------------------- #


class StreamConformity(StreamReadPort[Any], StreamWritePort[Any], Protocol):
    """Conformity protocol used only to ensure that the implementation conforms
    to the :class:`StreamReadPort` and :class:`StreamWritePort` protocols simultaneously.
    """


class StreamDepConformity(StreamReadDepPort, StreamWriteDepPort, Protocol):
    """Conformity protocol used only to ensure that the implementation conforms
    to the :class:`StreamReadDepPort` and :class:`StreamWriteDepPort` protocols simultaneously.
    """
