"""Optional recorders for runtime port and transaction events.

``RuntimeTracer`` records port/coordinator calls into the per-task ``RuntimeTrace``;
``TxTracer`` records transaction scope boundaries and delegates to a ``RuntimeTracer``.
Both ship a noop (production default, zero cost) and a recording implementation.
"""

from typing import Any, Mapping, Protocol, final, runtime_checkable

import attrs

from forze.base.primitives import ContextVarTrace, monotonic

from .trace import RuntimeTrace

# ----------------------- #


@runtime_checkable
class RuntimeTracer(Protocol):
    """Records runtime events for development diagnostics."""

    @property
    def enabled(self) -> bool:
        """Whether event recording is active."""
        ...

    @property
    def capture_values(self) -> bool:
        """Whether to capture redaction-applied call values (payloads/results) on the trace.

        Off by default and in production — the trace stays id-only (no PII, no cost). Turned on
        only under deterministic simulation, where the data is synthetic and value-level
        invariants want the actual values written/read."""
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
        key: str | None = None,
        outcome: str | None = None,
        error: str | None = None,
        corr: int | None = None,
        nested: bool = False,
        payload: Mapping[str, Any] | None = None,
        result: Mapping[str, Any] | None = None,
    ) -> int | None:
        """Append a runtime event when enabled; return its ``seq`` (``None`` when disabled)."""
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

    @property
    def capture_values(self) -> bool:
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
        key: str | None = None,
        outcome: str | None = None,
        error: str | None = None,
        corr: int | None = None,
        nested: bool = False,
        payload: Mapping[str, Any] | None = None,
        result: Mapping[str, Any] | None = None,
    ) -> int | None:
        del domain, op, surface, route, phase, tx_depth, tx_route, key, outcome, error
        del corr, nested, payload, result
        return None

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

    capture_values: bool = False
    """When set, the wrapped ports capture redaction-applied call values onto the trace (used by
    DST for value-level invariants); off by default so production tracing stays id-only."""

    _trace: ContextVarTrace[RuntimeTrace] = attrs.field(
        factory=lambda: ContextVarTrace(RuntimeTrace, "deps_runtime_trace"),
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
        key: str | None = None,
        outcome: str | None = None,
        error: str | None = None,
        corr: int | None = None,
        nested: bool = False,
        payload: Mapping[str, Any] | None = None,
        result: Mapping[str, Any] | None = None,
    ) -> int | None:
        event = self._trace.get_or_create().next_event(
            domain=domain,
            op=op,
            surface=surface,
            route=route,
            phase=phase,
            tx_depth=tx_depth,
            tx_route=tx_route,
            at=monotonic(),
            key=key,
            outcome=outcome,
            error=error,
            corr=corr,
            nested=nested,
            payload=payload,
            result=result,
        )
        return event.seq

    # ....................... #

    def snapshot(self) -> RuntimeTrace | None:
        return self._trace.snapshot()


# ....................... #


def runtime_tracer_from_flag(
    enabled: bool, *, capture_values: bool = False
) -> RuntimeTracer:
    """Return a recording or noop runtime tracer."""

    if enabled:
        return RecordingRuntimeTracer(capture_values=capture_values)

    return NOOP_RUNTIME_TRACER


# ....................... #


@runtime_checkable
class TxTracer(Protocol):
    """Records transaction scope boundaries for development diagnostics."""

    @property
    def enabled(self) -> bool:
        """Whether event recording is active."""
        ...

    def on_scope_enter(self, *, route: str, depth: int) -> None:
        """Record root transaction scope entry."""
        ...

    def on_scope_exit(self, *, route: str, depth: int) -> None:
        """Record root transaction scope exit."""
        ...


# ....................... #


@final
@attrs.define(slots=True, frozen=True)
class NoopTxTracer:
    """Transaction tracer that records nothing."""

    @property
    def enabled(self) -> bool:
        return False

    def on_scope_enter(self, *, route: str, depth: int) -> None:
        del route, depth
        return

    def on_scope_exit(self, *, route: str, depth: int) -> None:
        del route, depth
        return


# ....................... #

NOOP_TX_TRACER = NoopTxTracer()
"""Shared noop transaction tracer instance."""


# ....................... #


@final
@attrs.define(slots=True, frozen=True)
class RuntimeBackedTxTracer:
    """Delegates transaction events to a :class:`RuntimeTracer`."""

    runtime: RuntimeTracer
    """Underlying runtime event recorder."""

    # ....................... #

    @property
    def enabled(self) -> bool:
        return self.runtime.enabled

    # ....................... #

    def on_scope_enter(self, *, route: str, depth: int) -> None:
        self.runtime.record(
            domain="tx",
            op="enter",
            route=route,
            tx_route=route,
            tx_depth=depth,
        )

    # ....................... #

    def on_scope_exit(self, *, route: str, depth: int) -> None:
        self.runtime.record(
            domain="tx",
            op="exit",
            route=route,
            tx_route=route,
            tx_depth=depth,
        )


# ....................... #


def tx_tracer_from_runtime(runtime: RuntimeTracer) -> TxTracer:
    """Return a noop or runtime-backed transaction tracer."""

    if not runtime.enabled:
        return NOOP_TX_TRACER

    return RuntimeBackedTxTracer(runtime=runtime)
