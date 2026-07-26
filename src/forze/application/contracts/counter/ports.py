"""Port for application counters.

Counters expose a single command-style port: values are only meaningful at
allocation time (``incr``/``incr_batch``), so a separate read-only query port
is deliberately not provided.

**Semantics every backend agrees on**, pinned by the shared conformance battery rather
than left to each adapter:

- a counter that has never been touched reads as ``0``, so the first ``incr()`` returns 1;
- values are signed 64-bit (see :mod:`~forze.application.contracts.counter.bounds`);
  leaving that range raises ``counter_value_out_of_range`` and changes nothing;
- counters are *not* required to be non-negative — ``decr`` below zero is legal, and a
  negative ``by`` is an ordinary decrease;
- ``incr(by=0)`` returns the current value without moving it, which is the only read the
  port offers. It still *creates* the counter at 0 if it did not exist.
"""

from collections.abc import Awaitable
from typing import Protocol, runtime_checkable

# ----------------------- #


@runtime_checkable
class CounterPort(Protocol):
    """Distributed counter abstraction used for issuing sequential numbers."""

    def incr(self, by: int = 1, *, suffix: str | None = None) -> Awaitable[int]:
        """Increase the counter by ``by`` and return the new value.

        :param by: Increment step.
        :param suffix: Optional suffix used to partition counters within
            the same namespace.
        """
        ...  # pragma: no cover

    def incr_batch(
        self,
        size: int = 2,
        *,
        suffix: str | None = None,
    ) -> Awaitable[list[int]]:
        """Allocate a batch of counter values.

        :param size: Number of sequential values to allocate.
        :param suffix: Optional suffix used to partition counters.
        :returns: A list of allocated integer values in ascending order.
        """
        ...  # pragma: no cover

    def decr(self, by: int = 1, *, suffix: str | None = None) -> Awaitable[int]:
        """Decrease the counter by ``by`` and return the new value."""
        ...  # pragma: no cover

    def reset(self, value: int = 1, *, suffix: str | None = None) -> Awaitable[int]:
        """Reset the counter to the given value and return it."""
        ...  # pragma: no cover
