"""Stream contracts for append-only event backends (e.g. Redis Streams).

Provides :class:`StreamPort` and :class:`StreamEvent` for publish, read,
subscribe, and consumer group operations.
"""

from .ports import StreamPort
from .types import StreamEvent

# ----------------------- #

__all__ = [
    "StreamEvent",
    "StreamPort",
]
