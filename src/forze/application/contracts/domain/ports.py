"""Port for dispatching domain events to in-process handlers."""

from collections.abc import Awaitable, Callable, Sequence
from typing import Any, Protocol, runtime_checkable

from forze.base.exceptions import exc
from forze.domain.models import AggregateRoot, DomainEvent

# ----------------------- #


@runtime_checkable
class DomainEventDispatcherPort(Protocol):
    """Dispatches domain events to in-process handlers within the current scope."""

    def dispatch(self, events: Sequence[DomainEvent]) -> Awaitable[None]:
        """Run the registered handlers for each event, in registration order."""

        ...  # pragma: no cover


# ....................... #


async def drain_domain_events(
    domains: Sequence[Any],
    *,
    dispatcher_provider: Callable[[], DomainEventDispatcherPort | None],
    document_name: str,
) -> None:
    """Drain and dispatch domain events from any aggregate-root domains, in-tx.

    A no-op for non-aggregate documents (the common case). Raises if an aggregate
    emitted events but no dispatcher is registered, so events are never dropped.

    :param domains: Domain models written by the command, some of which may be aggregates.
    :param dispatcher_provider: Resolves the dispatcher registered for the current scope.
    :param document_name: Document spec name, for the refusal message.
    """

    events: list[DomainEvent] = []

    for domain in domains:
        if isinstance(domain, AggregateRoot) and domain.has_pending_events:
            events.extend(domain.collect_events())

    if not events:
        return

    dispatcher = dispatcher_provider()

    if dispatcher is None:
        raise exc.configuration(
            f"Aggregate emitted domain events for document {document_name!r} but "
            "no DomainEventsDepsModule is registered to dispatch them."
        )

    await dispatcher.dispatch(events)
