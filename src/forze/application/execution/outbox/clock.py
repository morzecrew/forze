"""Process-global Hybrid Logical Clock for causal outbox ordering.

One clock per process (HLC models node-local causality): every integration event
this process stages is stamped from it, and consuming an event carrying an
``HEADER_HLC`` merges that producer's timestamp in — so an event produced *in
reaction* to a consumed one always sorts after its cause, even across replicas
whose wall clocks disagree.

It is deliberately **not** a ``ContextVar``: the clock is shared by every task in
the process (request handlers, the relay loop, consumers), not scoped to one
invocation. A small mutable holder (rather than a rebound module global) lets
:func:`set_outbox_clock` swap it at the composition root or in tests.

The default clock carries a :data:`_DEFAULT_MAX_DRIFT` skew guard: ``HEADER_HLC``
is **untrusted input** (any producer with broker access can forge it), so a
forged far-future timestamp must not be able to drag this process's clock
arbitrarily ahead and permanently distort causal ordering. ``update`` rejects a
remote timestamp more than the drift ahead of local wall time, bounding the skew
a hostile message can induce to the drift.
"""

from __future__ import annotations

from datetime import timedelta

from forze.base.primitives import HybridLogicalClock

# ----------------------- #

_DEFAULT_MAX_DRIFT = timedelta(minutes=5)
"""Skew guard for inbound HLC merges: generous enough to tolerate gross NTP
misconfiguration between replicas, tight enough that a forged future timestamp
cannot push the clock more than this ahead of real time."""


class _ClockHolder:
    """Mutable single-attribute cell holding the process clock (avoids ``global``)."""

    __slots__ = ("clock",)

    def __init__(self) -> None:
        self.clock = HybridLogicalClock(max_drift=_DEFAULT_MAX_DRIFT)


_holder = _ClockHolder()


def outbox_clock() -> HybridLogicalClock:
    """The process-global HLC stamping staged events and merging inbound causality."""

    return _holder.clock


def set_outbox_clock(clock: HybridLogicalClock) -> None:
    """Replace the process-global outbox clock (composition root / tests)."""

    _holder.clock = clock
