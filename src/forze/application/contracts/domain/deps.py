"""Domain-event dispatcher dependency key and resolver."""

from ..deps import ConvenientDeps, DepKey, SimpleDepPort
from .ports import DomainEventDispatcherPort

# ----------------------- #

DomainEventDispatcherDepPort = SimpleDepPort[DomainEventDispatcherPort]
"""Domain-event dispatcher dependency port (built per scope from ``ctx``)."""

DomainEventDispatcherDepKey = DepKey[DomainEventDispatcherDepPort](
    "domain_event_dispatcher",
)
"""Key used to register the :class:`DomainEventDispatcherPort` builder."""


# ....................... #


class DomainDeps(ConvenientDeps):
    """Resolve the in-process domain-event dispatcher."""

    def __call__(self) -> DomainEventDispatcherPort:
        """Resolve the domain-event dispatcher for the current scope."""

        return self._resolve_simple(DomainEventDispatcherDepKey)
