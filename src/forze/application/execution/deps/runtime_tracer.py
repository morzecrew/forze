"""Optional recorder for runtime port and transaction events."""

from __future__ import annotations

from contextvars import ContextVar
from typing import Protocol, final, runtime_checkable

import attrs

from forze.application.execution.tracing.buffer import RuntimeTrace

# ----------------------- #


@runtime_checkable
class RuntimeTracer(Protocol):
    """Records runtime events for development diagnostics."""

    @property
    def enabled(self) -> bool:
        """Whether event recording is active."""
        ...

    def init_task(self) -> None:
        """Ensure a per-task trace buffer exists when recording."""
        ...

    def record(
        self,
        *,
        domain: str,
        op: str,
        surface: str | None = None,
        route: str | None = None,
        phase: str | None = None,
        tx_depth: int = 0,
        tx_route: str | None = None,
    ) -> None:
        """Append a runtime event when enabled."""
        ...

    def snapshot(self) -> RuntimeTrace | None:
        """Return the current task trace, if any."""
        ...


# ....................... #


@final
@attrs.define(slots=True, frozen=True)
class NoopRuntimeTracer:
    """Runtime tracer that records nothing."""

    @property
    def enabled(self) -> bool:
        return False

    def init_task(self) -> None:
        return

    def record(
        self,
        *,
        domain: str,
        op: str,
        surface: str | None = None,
        route: str | None = None,
        phase: str | None = None,
        tx_depth: int = 0,
        tx_route: str | None = None,
    ) -> None:
        del domain, op, surface, route, phase, tx_depth, tx_route
        return

    def snapshot(self) -> RuntimeTrace | None:
        return None


# ....................... #

NOOP_RUNTIME_TRACER = NoopRuntimeTracer()
"""Shared noop runtime tracer instance."""


# ....................... #


@final
@attrs.define(slots=True)
class RecordingRuntimeTracer:
    """Per-:class:`~forze.application.execution.deps.container.Deps` runtime event recorder."""

    _trace: ContextVar[RuntimeTrace | None] = attrs.field(
        factory=lambda: ContextVar("deps_runtime_trace", default=None),
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
            self._trace.set(RuntimeTrace())

    # ....................... #

    def _trace_get_or_create(self) -> RuntimeTrace:
        trace = self._trace.get()

        if trace is None:
            trace = RuntimeTrace()
            self._trace.set(trace)

        return trace

    # ....................... #

    def record(
        self,
        *,
        domain: str,
        op: str,
        surface: str | None = None,
        route: str | None = None,
        phase: str | None = None,
        tx_depth: int = 0,
        tx_route: str | None = None,
    ) -> None:
        self._trace_get_or_create().next_event(
            domain=domain,
            op=op,
            surface=surface,
            route=route,
            phase=phase,
            tx_depth=tx_depth,
            tx_route=tx_route,
        )

    # ....................... #

    def snapshot(self) -> RuntimeTrace | None:
        return self._trace.get()


# ....................... #


def runtime_tracer_from_flag(enabled: bool) -> RuntimeTracer:
    """Return a recording or noop runtime tracer."""

    if enabled:
        return RecordingRuntimeTracer()

    return NOOP_RUNTIME_TRACER
