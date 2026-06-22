"""Transport-neutral helpers shared by realtime/RPC transport adapters.

The inbound frame boundary (:func:`guard_frame`) lets Socket.IO, FastAPI
websockets, and SSE share one error-handling shape over the operation registry,
each rendering the resulting :class:`~forze.base.exceptions.ErrorEnvelope` into
its own wire format.
"""

from .frames import (
    FrameErr,
    FrameOk,
    FrameOutcome,
    ServerErrorHook,
    guard_frame,
)

# ----------------------- #

__all__ = [
    "FrameOk",
    "FrameErr",
    "FrameOutcome",
    "ServerErrorHook",
    "guard_frame",
]
