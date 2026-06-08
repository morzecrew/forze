"""Inbox dependency key and resolver."""

from ..deps import ConfigurableDepPort, ConvenientDeps, DepKey
from .ports import InboxPort
from .specs import InboxSpec

# ----------------------- #

InboxDepPort = ConfigurableDepPort[InboxSpec, InboxPort]
"""Inbox dependency port."""

InboxDepKey = DepKey[InboxDepPort]("inbox")
"""Key used to register the :class:`InboxPort` builder implementation."""


# ....................... #


class InboxDeps(ConvenientDeps):
    """Resolve an inbox port for a spec."""

    def __call__(self, spec: InboxSpec) -> InboxPort:
        """Resolve the inbox port for the given spec."""

        return self._resolve_configurable(InboxDepKey, spec, route=spec.name)
