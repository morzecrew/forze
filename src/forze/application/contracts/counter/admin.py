"""Read-only enumeration over a counter spec's allocated values (ops / export surfaces).

:class:`~forze.application.contracts.counter.CounterPort` deliberately has **no read verb**,
and its module says why: a counter's value is only meaningful at the instant it is allocated,
so a handler that read one would be reading a number that another allocation has already
moved. That doctrine is right for handlers and wrong for everyone else. A counter is durable
state — it is *the* durable state behind every invoice number, order number and ticket
sequence an application has handed out — and state that cannot be read cannot be carried. A
migration that rebuilt every other plane and left the counters at zero would reissue sequence
numbers already in customers' hands, silently, with no error anywhere.

So the read lives here instead of on ``CounterPort``, which keeps both properties at once:
handlers still cannot read a counter (reading one is a race by construction), while operators
and a portable export can. The write side needs nothing new — ``CounterPort.reset(value,
suffix=)`` is already the verb an import calls.
"""

from collections.abc import Awaitable, Sequence
from typing import Protocol, final, runtime_checkable

import attrs

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class CounterEntry:
    """One allocated counter under a spec: which partition, and how far it has got."""

    suffix: str | None
    """The ``suffix`` this counter was allocated under, or ``None`` for the unsuffixed one.

    ``None`` is a real, distinct counter — not "no counter". A spec that is never passed a
    suffix allocates exactly one, and an enumeration that dropped it would export every
    partition of a sequence except the one most applications actually use.
    """

    value: int
    """The counter's current value — the last number it handed out.

    Point-in-time: an allocation racing the enumeration moves it. Enumerate a counter you
    intend to *carry* only when nothing is allocating from it (a stopped fleet), or the
    import will replay a number the source has since issued again.
    """


# ....................... #


@runtime_checkable
class CounterAdminPort(Protocol):
    """Read-only enumeration over one counter spec's partitions."""

    def list_counters(self) -> Awaitable[Sequence[CounterEntry]]:
        """Every counter allocated under this spec, one entry per ``suffix``.

        Returns the **complete** set, driving the backend's cursor to exhaustion internally
        rather than handing pages back. That is deliberate: Redis ``SCAN`` may return an
        *empty* page with a non-zero cursor and may return the same key on more than one
        page, so a paged contract would make every caller responsible for a termination rule
        that is silently wrong in exactly one direction — stop on the first empty page and
        you under-report the counters, which is indistinguishable from an application that
        has none. The adapter is the one place that rule can be enforced once.

        The cost of that choice is that the whole set is held in memory. Each entry is a
        short string and an integer, so the bound is generous, but it *is* a bound: a spec
        partitioned into millions of suffixes should page, and does not yet.

        Scoped to the ambient tenant like every other counter call — a tenant-aware route
        enumerates that tenant's counters, so a full export walks it once per tenant.
        """
        ...  # pragma: no cover
