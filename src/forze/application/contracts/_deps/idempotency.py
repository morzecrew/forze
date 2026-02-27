from datetime import timedelta
from typing import TYPE_CHECKING, Protocol, runtime_checkable

from .._ports.idempotency import IdempotencyPort

if TYPE_CHECKING:
    from forze.application.execution.context import ExecutionContext

# ----------------------- #


@runtime_checkable
class IdempotencyDepPort(Protocol):
    """Factory protocol for building :class:`IdempotencyPort` instances."""

    def __call__(
        self,
        context: "ExecutionContext",
        ttl: timedelta = timedelta(seconds=30),
    ) -> IdempotencyPort:
        """Build an idempotency port bound to the given context and TTL."""
        ...


# Dependency key is not implemented as we typically don't need to use idempotency dependency
# within the application code, only in interfaces (e.g. HTTP API).
