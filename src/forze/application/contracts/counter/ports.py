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
  port offers. It does **not** create the counter: a read of an absent counter returns 0 and
  leaves nothing behind, so a guard that reads on every request does not grow the key space.

**Lifetimes** (``ttl``, see :mod:`~forze.application.contracts.counter.expiry`):

- an increment (``incr``, ``incr_batch``, ``decr``) that **creates** the counter starts its
  lifetime: the counter expires ``ttl`` later. Creating covers a counter that has lapsed;
- no later increment moves that lifetime, with or without a ``ttl`` — a fixed window ends
  when it was created to end, however busy it is;
- ``reset`` replaces the counter whole, value *and* lifetime: ``reset(v, ttl=t)`` expires
  ``t`` after the reset, and ``reset(v)`` leaves a counter that never expires;
- a lapsed counter reads as absent: ``incr(0)`` returns 0, the next increment starts again
  from 0, and enumeration does not list it;
- a backend that cannot expire refuses a ``ttl`` with ``configuration``
  (``counter_ttl_unsupported``) instead of ignoring it. Ask
  :func:`~forze.application.contracts.counter.counter_capabilities` before relying on it.

Lifetimes are as precise as the backend's clock: the store's own clock for Redis and
Postgres, the application's :func:`~forze.base.primitives.utcnow` for Mongo, Firestore and
the in-memory mock (so a frozen or simulated clock drives expiry there).
"""

from collections.abc import Awaitable
from datetime import timedelta
from typing import Protocol, runtime_checkable

# ----------------------- #


@runtime_checkable
class CounterPort(Protocol):
    """Distributed counter abstraction used for issuing sequential numbers."""

    def incr(
        self,
        by: int = 1,
        *,
        suffix: str | None = None,
        ttl: timedelta | None = None,
    ) -> Awaitable[int]:
        """Increase the counter by ``by`` and return the new value.

        :param by: Increment step. ``0`` reads the current value without creating the
            counter.
        :param suffix: Optional suffix used to partition counters within
            the same namespace.
        :param ttl: Lifetime started if this call creates the counter (see the module
            docstring); ignored by a call that finds the counter live.
        :raises CoreException: ``configuration`` when *ttl* is given and the backend cannot
            expire counters; ``precondition`` when *ttl* is not positive.
        """
        ...  # pragma: no cover

    def incr_batch(
        self,
        size: int = 2,
        *,
        suffix: str | None = None,
        ttl: timedelta | None = None,
    ) -> Awaitable[list[int]]:
        """Allocate a batch of counter values.

        :param size: Number of sequential values to allocate.
        :param suffix: Optional suffix used to partition counters.
        :param ttl: Lifetime started if this call creates the counter, as for :meth:`incr`.
        :returns: A list of allocated integer values in ascending order.
        """
        ...  # pragma: no cover

    def decr(
        self,
        by: int = 1,
        *,
        suffix: str | None = None,
        ttl: timedelta | None = None,
    ) -> Awaitable[int]:
        """Decrease the counter by ``by`` and return the new value.

        :param ttl: Lifetime started if this call creates the counter, as for :meth:`incr`.
        """
        ...  # pragma: no cover

    def reset(
        self,
        value: int = 1,
        *,
        suffix: str | None = None,
        ttl: timedelta | None = None,
    ) -> Awaitable[int]:
        """Reset the counter to the given value and return it.

        :param ttl: The counter's new lifetime, replacing any it had; ``None`` leaves a
            counter that never expires.
        """
        ...  # pragma: no cover
