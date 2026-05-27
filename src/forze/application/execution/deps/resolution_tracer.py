"""Optional recorder for observed dependency resolution edges."""

from __future__ import annotations

from contextvars import ContextVar
from typing import Protocol, final, runtime_checkable

import attrs

from .resolution import ResolutionFrame
from .trace import DepsResolutionTrace

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

    _trace: ContextVar[DepsResolutionTrace | None] = attrs.field(
        factory=lambda: ContextVar("deps_resolution_trace", default=None),
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
        if self._trace.get() is None:
            self._trace.set(DepsResolutionTrace())

    # ....................... #

    def _trace_get_or_create(self) -> DepsResolutionTrace:
        trace = self._trace.get()

        if trace is None:
            trace = DepsResolutionTrace()
            self._trace.set(trace)

        return trace

    # ....................... #

    def record_edge(
        self,
        parent: ResolutionFrame,
        child: ResolutionFrame,
    ) -> None:
        self._trace_get_or_create().add_edge(parent, child)

    def snapshot(self) -> DepsResolutionTrace | None:
        return self._trace.get()


# ....................... #


def resolution_tracer_from_flag(enabled: bool) -> ResolutionTracer:
    """Return a recording or noop resolution tracer."""

    if enabled:
        return RecordingResolutionTracer()

    return NOOP_RESOLUTION_TRACER
