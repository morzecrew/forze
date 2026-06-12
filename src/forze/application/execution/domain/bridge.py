"""Bridge a domain event to a transactional-outbox integration event."""

from typing import Awaitable, Callable

from pydantic import BaseModel

from forze.application.contracts.outbox import OutboxSpec
from forze.domain.models import DomainEvent

from ..context import ExecutionContext

# ----------------------- #


def outbox_event_handler[E: DomainEvent, M: BaseModel](
    spec: OutboxSpec[M],
    event_type: str,
    to_payload: Callable[[E], M],
) -> Callable[[ExecutionContext], Callable[[E], Awaitable[None]]]:
    """Build a domain-event handler **factory** that stages an integration event.

    Register it on a :class:`~forze.application.execution.domain.DomainEventRegistry`.
    The factory resolves the outbox command port from ``ctx`` once; the returned
    handler closes over only that port (not the context), and the staged integration
    event flushes atomically with the aggregate write.
    """

    def _factory(ctx: ExecutionContext) -> Callable[[E], Awaitable[None]]:
        command = ctx.outbox.command(spec)

        async def _handler(event: E) -> None:
            await command.stage(event_type, to_payload(event))

        return _handler

    return _factory
