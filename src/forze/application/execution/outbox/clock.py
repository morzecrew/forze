"""Per-runtime Hybrid Logical Clock for causal outbox ordering.

One clock per runtime (an HLC models node-local causality, and a runtime is a
node): every integration event a runtime stages is stamped from *its*
:attr:`~forze.application.execution.context.ExecutionContext.outbox_clock`, and
consuming an event carrying an ``HEADER_HLC`` merges that producer's timestamp in
— so an event produced *in reaction* to a consumed one always sorts after its
cause, even across nodes whose wall clocks disagree.

The clock is owned by the :class:`ExecutionContext`, not a process global: in a
single-process deployment that is one clock per process (unchanged), and under
multi-runtime simulation each node gets an independent clock — so a missed
inbound merge surfaces as a causality violation instead of being masked by a
shared timeline. It is deliberately not a ``ContextVar``: within a runtime the
clock is shared by every task (request handlers, the relay loop, consumers), not
scoped to one invocation.

The clock carries a :data:`_DEFAULT_MAX_DRIFT` skew guard: ``HEADER_HLC`` is
**untrusted input** (any producer with broker access can forge it), so a forged
far-future timestamp must not be able to drag a node's clock arbitrarily ahead
and permanently distort causal ordering. ``update`` rejects a remote timestamp
more than the drift ahead of local wall time, bounding the skew a hostile message
can induce to the drift.
"""

from __future__ import annotations

from datetime import timedelta

from forze.base.primitives import HybridLogicalClock

# ----------------------- #

_DEFAULT_MAX_DRIFT = timedelta(minutes=5)
"""Skew guard for inbound HLC merges: generous enough to tolerate gross NTP
misconfiguration between replicas, tight enough that a forged future timestamp
cannot push the clock more than this ahead of real time."""


def new_outbox_clock() -> HybridLogicalClock:
    """A fresh node-local outbox HLC with the default skew guard.

    The factory for :attr:`ExecutionContext.outbox_clock`; each runtime builds one.
    """

    return HybridLogicalClock(max_drift=_DEFAULT_MAX_DRIFT)
