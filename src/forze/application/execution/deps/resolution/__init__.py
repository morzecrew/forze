"""Dependency-resolution machinery: the frame, the per-task resolution stack, the
edge-tracer, and the resolution-edge DAG.

Grouped here because they're one subsystem — tracking how a configurable port is
resolved (cycle detection, the resolution graph for diagnostics). Distinct from
runtime *port-event* tracing, which lives in ``execution.tracing``.
"""

from .context import ResolutionContext
from .frame import ResolutionFrame, format_cycle_error, frame_for
from .graph import DepsResolutionTrace
from .tracer import (
    NOOP_RESOLUTION_TRACER,
    NoopResolutionTracer,
    RecordingResolutionTracer,
    ResolutionTracer,
    resolution_tracer_from_flag,
)

__all__ = [
    "NOOP_RESOLUTION_TRACER",
    "DepsResolutionTrace",
    "NoopResolutionTracer",
    "RecordingResolutionTracer",
    "ResolutionContext",
    "ResolutionFrame",
    "ResolutionTracer",
    "format_cycle_error",
    "frame_for",
    "resolution_tracer_from_flag",
]
