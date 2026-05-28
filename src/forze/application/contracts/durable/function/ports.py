from datetime import datetime
from typing import Awaitable, Callable, Protocol, TypeVar, runtime_checkable

# ----------------------- #

T = TypeVar("T")


# ....................... #


@runtime_checkable
class DurableFunctionEventCommandPort[M](Protocol):
    """Contract for emitting events that may trigger durable functions."""

    def send(
        self,
        payload: M,
        *,
        event_id: str | None = None,  # noqa: F841
        occurred_at: datetime | None = None,  # noqa: F841
    ) -> Awaitable[str]:
        """Send an event and return its identifier.

        :param event_id: Optional idempotent event id (provider deduplicates triggers).
        :param occurred_at: Optional UTC instant; future times may defer execution.
        """
        ...  # pragma: no cover


# ....................... #


@runtime_checkable
class DurableFunctionStepPort(Protocol):
    """Contract for memoized steps inside a durable function run.

    Only available during function execution (worker / integration scope).
    """

    def run[T](
        self,
        step_id: str,
        fn: Callable[[], Awaitable[T]],
    ) -> Awaitable[T]:
        """Run *fn* as a durable, retriable step."""
        ...  # pragma: no cover
