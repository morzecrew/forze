"""HLC checkpoint dependency key and port alias."""

from ..deps import DepKey, SimpleDepPort
from .ports import HlcCheckpointPort

# ----------------------- #

HlcCheckpointDepPort = SimpleDepPort[HlcCheckpointPort]
"""HLC checkpoint dependency port — a node-global singleton built per scope from ``ctx``
(there is one clock per runtime, so the mark it persists is node-scoped, not per-route)."""

HlcCheckpointDepKey = DepKey[HlcCheckpointDepPort]("hlc_checkpoint")
"""Key used to register the :class:`~.HlcCheckpointPort` builder.

Optional: when unregistered, a restarted clock simply resumes from ``(0, 0)`` (the prior
behavior) — recovery is a no-op rather than an error."""
