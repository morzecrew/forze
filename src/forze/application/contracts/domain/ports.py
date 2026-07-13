"""Port for dispatching domain events to in-process handlers."""

from collections.abc import Awaitable, Sequence
from typing import Protocol, runtime_checkable

from forze.domain.models import DomainEvent

# ----------------------- #


@runtime_checkable
class DomainEventDispatcherPort(Protocol):
    """Dispatches domain events to in-process handlers within the current scope."""

    def dispatch(self, events: Sequence[DomainEvent]) -> Awaitable[None]:
        """Run the registered handlers for each event, in registration order."""

        ...  # pragma: no cover
