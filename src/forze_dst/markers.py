"""DST markup you sprinkle in application code — no-ops in production, signal under simulation.

The two annotations you add to your *app / handler* code (not your tests): they record into the
active recorder when a simulation is driving the code and do nothing otherwise, so they are cheap
and safe to leave in production. The *assertions* about them live in ``forze_dst.invariants`` —
e.g. ``sometimes`` proves a ``reached(label)`` fired, and ``expect`` reads ``record_event`` facts.
"""

from __future__ import annotations

from forze_dst.oracle.reachability import reached
from forze_dst.oracle.recorder import record_event

__all__ = ["record_event", "reached"]
