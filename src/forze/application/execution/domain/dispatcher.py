"""In-process domain-event dispatcher."""

from typing import Sequence, final

import attrs

from forze.domain.models import DomainEvent

from ..context import ExecutionContext
from .handler import DomainEventRegistry

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True)
class InProcessDomainEventDispatcher:
    """Builds and runs the registered handlers for each domain event, in order.

    Bound to the current scope's :class:`ExecutionContext`; each handler is built
    from the context (resolving its narrow capabilities) and then invoked with only
    the event — so handlers never hold the context. Handlers run in the caller's
    transaction, so their writes / outbox staging are atomic with it.
    """

    registry: DomainEventRegistry
    ctx: ExecutionContext

    # ....................... #

    async def dispatch(self, events: Sequence[DomainEvent]) -> None:
        """Dispatch *events* to their registered handlers within the current scope."""

        for event in events:
            for factory in self.registry.factories_for(event):
                handler = factory(self.ctx)
                await handler(event)
