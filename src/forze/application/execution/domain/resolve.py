"""Resolve the domain-event dispatcher for injection into write adapters."""

from collections.abc import Callable

from forze.application.contracts.domain import (
    DomainEventDispatcherDepKey,
    DomainEventDispatcherPort,
)

from ..context import ExecutionContext

# ----------------------- #


def domain_dispatcher_provider(
    ctx: ExecutionContext,
) -> Callable[[], DomainEventDispatcherPort | None]:
    """Build a lazy provider resolving the domain dispatcher, or ``None`` if unregistered.

    Injected into document write adapters so they dispatch an aggregate's collected
    domain events in-transaction — but never *require* a dispatcher for non-aggregate
    documents (the provider returns ``None`` when no ``DomainEventsDepsModule`` is
    registered, and the adapter only raises if an aggregate actually emitted events).
    """

    def _provide() -> DomainEventDispatcherPort | None:
        return ctx.domain() if ctx.deps.exists(DomainEventDispatcherDepKey) else None

    return _provide
