# Deps / DepsModule are defined in `forze.application.contracts.deps` (their home) and surfaced on
# the kernel front door `forze.application.execution`; this subpackage exports only what it defines.
from .frozen import FrozenDeps, FrozenDepsRegistry
from .registry import DepsRegistry
from .resolution import (
    NOOP_RESOLUTION_TRACER,
    DepsResolutionTrace,
    NoopResolutionTracer,
    RecordingResolutionTracer,
    ResolutionContext,
    ResolutionFrame,
    ResolutionTracer,
    resolution_tracer_from_flag,
)

# ----------------------- #

__all__ = [
    "DepsRegistry",
    "DepsResolutionTrace",
    "FrozenDeps",
    "FrozenDepsRegistry",
    "NOOP_RESOLUTION_TRACER",
    "NoopResolutionTracer",
    "RecordingResolutionTracer",
    "ResolutionContext",
    "ResolutionFrame",
    "ResolutionTracer",
    "resolution_tracer_from_flag",
]
