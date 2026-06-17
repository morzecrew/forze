"""Process-global Hybrid Logical Clock for causal outbox ordering.

One clock per process (HLC models node-local causality): every integration event
this process stages is stamped from it, and consuming an event carrying an
``HEADER_HLC`` merges that producer's timestamp in — so an event produced *in
reaction* to a consumed one always sorts after its cause, even across replicas
whose wall clocks disagree. A module-level singleton mirrors the resilience
executor's process-default; :func:`set_outbox_clock` swaps it at the composition
root or in tests.
"""

from __future__ import annotations

from forze.base.primitives import HybridLogicalClock

# ----------------------- #

_clock = HybridLogicalClock()


def outbox_clock() -> HybridLogicalClock:
    """The process-global HLC stamping staged events and merging inbound causality."""

    return _clock


def set_outbox_clock(clock: HybridLogicalClock) -> None:
    """Replace the process-global outbox clock (composition root / tests)."""

    global _clock
    _clock = clock
