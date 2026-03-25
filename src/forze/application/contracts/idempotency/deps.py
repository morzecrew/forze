"""Idempotency dependency keys."""

from datetime import timedelta
from typing import TYPE_CHECKING, Protocol, runtime_checkable

from ..deps import DepKey
from .ports import IdempotencyPort

if TYPE_CHECKING:
    from forze.application.execution.context import ExecutionContext

# ----------------------- #
#! TODO: add spec instead of plain ttl


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


# ....................... #

IdempotencyDepKey = DepKey[IdempotencyDepPort]("idempotency")
"""Key used to register the :class:`IdempotencyDepPort` implementation."""
