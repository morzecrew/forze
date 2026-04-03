from typing import Any

from ..base import BaseDepPort, DepKey
from .ports import StreamGroupPort, StreamReadPort, StreamWritePort
from .specs import StreamSpec

# ----------------------- #

StreamReadDepKey = DepKey[
    BaseDepPort[
        StreamSpec[Any],
        StreamReadPort[Any],
    ]
]("stream_read")
"""Key used to register the :class:`StreamReadPort` builder implementation."""

StreamWriteDepKey = DepKey[
    BaseDepPort[
        StreamSpec[Any],
        StreamWritePort[Any],
    ]
]("stream_write")
"""Key used to register the :class:`StreamWritePort` builder implementation."""

StreamGroupDepKey = DepKey[
    BaseDepPort[
        StreamSpec[Any],
        StreamGroupPort[Any],
    ]
]("stream_group")
"""Key used to register the :class:`StreamGroupPort` builder implementation."""
