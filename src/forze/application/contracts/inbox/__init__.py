"""Consumer-side message deduplication (inbox) contracts."""

from .deps import InboxDepKey, InboxDepPort, InboxDeps
from .ports import InboxPort
from .specs import InboxSpec

# ----------------------- #

__all__ = [
    "InboxDepKey",
    "InboxDepPort",
    "InboxDeps",
    "InboxPort",
    "InboxSpec",
]
