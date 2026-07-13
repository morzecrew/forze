"""In-process domain-event dispatcher."""

from collections.abc import Sequence
from typing import final

import attrs

from forze.domain.models import DomainEvent

from ..context import ExecutionContext
from ..tracing.emit import record
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
            record(
                domain="domain",
                op="dispatch",
                surface=type(event).__name__,
                deps=self.ctx.deps,
            )
            for factory in self.registry.factories_for(event):
                handler = factory(self.ctx)
                await handler(event)
