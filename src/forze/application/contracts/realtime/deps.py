"""Realtime port dependency key and resolver."""

from ..deps import ConvenientDeps, DepKey, SimpleDepPort
from .ports import RealtimePort

# ----------------------- #

RealtimeDepPort = SimpleDepPort[RealtimePort]
"""Realtime port dependency port (built per scope from ``ctx``)."""

RealtimeDepKey = DepKey[RealtimeDepPort]("realtime")
"""Key used to register the :class:`RealtimePort` builder."""


# ....................... #


class RealtimeDeps(ConvenientDeps):
    """Resolve the realtime push port for the current scope."""

    def __call__(self) -> RealtimePort:
        """Resolve the realtime port."""

        return self._resolve_simple(RealtimeDepKey)
