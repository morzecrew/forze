"""Deterministic simulation runtime: a native virtual-time event loop + clock seams.

The substrate for deterministic simulation testing (DST) — run an async scenario in
virtual time, seed-replayable, with no real I/O. Framework-owned (no external loop
dependency); the simulation clock drives the ambient ``TimeSource`` so application
time reads track it.
"""

from __future__ import annotations

from forze_mock.simulation.loop import (
    RealIOForbidden,
    SimulationDeadlock,
    SimulationEventLoop,
)
from forze_mock.simulation.runtime import run_simulation
from forze_mock.simulation.time_source import DEFAULT_EPOCH, SimulationTimeSource

# ----------------------- #

__all__ = [
    "SimulationEventLoop",
    "SimulationTimeSource",
    "run_simulation",
    "RealIOForbidden",
    "SimulationDeadlock",
    "DEFAULT_EPOCH",
]
