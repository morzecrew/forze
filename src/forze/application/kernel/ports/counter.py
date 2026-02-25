"""Port for monotonic application counters."""

from typing import Optional, Protocol, runtime_checkable

# ----------------------- #


@runtime_checkable
class CounterPort(Protocol):
    """Distributed counter abstraction used for issuing sequential numbers."""

    async def incr(self, by: int = 1, *, suffix: Optional[str] = None) -> int:
        """Increase the counter by ``by`` and return the new value.

        :param by: Increment step.
        :param suffix: Optional suffix used to partition counters within
            the same namespace.
        """
        ...

    async def incr_batch(
        self,
        size: int = 2,
        *,
        suffix: Optional[str] = None,
    ) -> list[int]:
        """Allocate a batch of counter values.

        :param size: Number of sequential values to allocate.
        :param suffix: Optional suffix used to partition counters.
        :returns: A list of allocated integer values in ascending order.
        """
        ...

    async def decr(self, by: int = 1, *, suffix: Optional[str] = None) -> int:
        """Decrease the counter by ``by`` and return the new value."""
        ...

    async def reset(self, value: int = 1, *, suffix: Optional[str] = None) -> int:
        """Reset the counter to the given value and return it."""
        ...
