"""Domain-event handler types and registry."""

from typing import Awaitable, Callable, cast, final

import attrs

from forze.domain.models import DomainEvent

from ..context import ExecutionContext

# ----------------------- #

DomainEventHandler = Callable[[DomainEvent], Awaitable[None]]
"""Running handler: invoked with a domain event only (no execution context)."""

DomainEventHandlerFactory = Callable[[ExecutionContext], DomainEventHandler]
"""Builds a :data:`DomainEventHandler` from the scope's context.

The factory resolves the narrow capabilities it needs from ``ctx`` and returns a
context-free handler — so the running handler never holds the execution context.
"""


# ....................... #


@final
@attrs.define(slots=True)
class DomainEventRegistry:
    """Maps domain-event types to handler factories (isinstance-matched)."""

    _factories: list[tuple[type[DomainEvent], DomainEventHandlerFactory]] = attrs.field(
        factory=list,
    )

    # ....................... #

    def register[E: DomainEvent](
        self,
        event_type: type[E],
        factory: Callable[[ExecutionContext], Callable[[E], Awaitable[None]]],
    ) -> None:
        """Register a handler factory for events that are instances of *event_type*."""

        self._factories.append((event_type, cast(DomainEventHandlerFactory, factory)))

    # ....................... #

    def factories_for(self, event: DomainEvent) -> list[DomainEventHandlerFactory]:
        """Return handler factories whose registered type matches *event* (subclass-aware)."""

        return [factory for tp, factory in self._factories if isinstance(event, tp)]
