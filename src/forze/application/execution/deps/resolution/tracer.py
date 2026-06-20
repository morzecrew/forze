"""Optional recorder for observed dependency resolution edges."""

from typing import Protocol, final, runtime_checkable

import attrs

from forze.base.primitives import ContextVarTrace

from .frame import ResolutionFrame
from .graph import DepsResolutionTrace

# ----------------------- #


@runtime_checkable
class ResolutionTracer(Protocol):
    """Records directed resolution edges for development diagnostics."""

    @property
    def enabled(self) -> bool:
        """Whether edge recording is active."""
        ...

    def init_task(self) -> None:
        """Ensure a per-task trace buffer exists when recording."""
        ...

    def record_edge(self, parent: ResolutionFrame, child: ResolutionFrame) -> None:
        """Record ``parent -> child`` (idempotent per edge pair)."""
        ...

    def snapshot(self) -> DepsResolutionTrace | None:
        """Return the current task trace, if any."""
        ...


# ....................... #


@final
@attrs.define(slots=True, frozen=True)
class NoopResolutionTracer:
    """Resolution tracer that records nothing."""

    @property
    def enabled(self) -> bool:
        return False

    def init_task(self) -> None:
        return

    def record_edge(
        self,
        parent: ResolutionFrame,
        child: ResolutionFrame,
    ) -> None:
        return

    def snapshot(self) -> DepsResolutionTrace | None:
        return None


# ....................... #

NOOP_RESOLUTION_TRACER = NoopResolutionTracer()
"""Shared noop resolution tracer instance."""

# ....................... #


@final
@attrs.define(slots=True)
class RecordingResolutionTracer:
    """Per-:class:`~forze.application.execution.deps.container.Deps` resolution edge recorder."""

    _trace: ContextVarTrace[DepsResolutionTrace] = attrs.field(
        factory=lambda: ContextVarTrace(DepsResolutionTrace, "deps_resolution_trace"),
        init=False,
        repr=False,
        eq=False,
        hash=False,
    )

    # ....................... #

    @property
    def enabled(self) -> bool:
        return True

    # ....................... #

    def init_task(self) -> None:
        self._trace.init_task()

    # ....................... #

    def record_edge(
        self,
        parent: ResolutionFrame,
        child: ResolutionFrame,
    ) -> None:
        self._trace.get_or_create().add_edge(parent, child)

    def snapshot(self) -> DepsResolutionTrace | None:
        return self._trace.snapshot()


# ....................... #


def resolution_tracer_from_flag(enabled: bool) -> ResolutionTracer:
    """Return a recording or noop resolution tracer."""

    return RecordingResolutionTracer() if enabled else NOOP_RESOLUTION_TRACER
