"""Port for monotonic application counters."""

from typing import Awaitable, Optional, Protocol, runtime_checkable

# ----------------------- #


@runtime_checkable
class CounterPort(Protocol):
    """Distributed counter abstraction used for issuing sequential numbers."""

    def incr(self, by: int = 1, *, suffix: Optional[str] = None) -> Awaitable[int]:
        """Increase the counter by ``by`` and return the new value.

        :param by: Increment step.
        :param suffix: Optional suffix used to partition counters within
            the same namespace.
        """
        ...

    def incr_batch(
        self,
        size: int = 2,
        *,
        suffix: Optional[str] = None,
    ) -> Awaitable[list[int]]:
        """Allocate a batch of counter values.

        :param size: Number of sequential values to allocate.
        :param suffix: Optional suffix used to partition counters.
        :returns: A list of allocated integer values in ascending order.
        """
        ...

    def decr(self, by: int = 1, *, suffix: Optional[str] = None) -> Awaitable[int]:
        """Decrease the counter by ``by`` and return the new value."""
        ...

    def reset(self, value: int = 1, *, suffix: Optional[str] = None) -> Awaitable[int]:
        """Reset the counter to the given value and return it."""
        ...
