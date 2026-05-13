"""Capability trace events and store."""

from __future__ import annotations

from typing import Any, Callable, Literal, Protocol

import attrs

from ..context import ExecutionContext

# ----------------------- #

CapabilityTraceKind = Literal["guard", "effect", "after_commit"]
CapabilityTraceAction = Literal["ran", "skipped_missing", "skipped_return", "error"]

# ....................... #


class SchedulableCapabilitySpec(Protocol):
    """Structural type for middleware specs in capability scheduling and segments."""

    priority: int
    requires: frozenset[str]
    provides: frozenset[str]
    step_label: str | None
    factory: Callable[[ExecutionContext], Any]


# ....................... #


@attrs.define(slots=True, frozen=True, kw_only=True)
class CapabilitySkip:
    """Return this from a guard or effect to skip without aborting the usecase."""

    reason: str | None = None


GuardSkip = CapabilitySkip
"""Backward-compatible alias for :class:`CapabilitySkip`."""

# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class CapabilityExecutionEvent:
    """One capability-segment step outcome (for tests and diagnostics)."""

    bucket: str
    label: str
    kind: CapabilityTraceKind
    action: CapabilityTraceAction
    detail: str | None = None


# ....................... #


def capability_step_label(spec: SchedulableCapabilitySpec, impl: object) -> str:
    if spec.step_label:
        return spec.step_label

    fact = spec.factory
    fn = getattr(fact, "__qualname__", None) or getattr(fact, "__name__", None)

    if isinstance(fn, str) and fn:
        return fn

    return type(impl).__qualname__


# ....................... #


@attrs.define(slots=True)
class CapabilityStore:
    """Tracks capability readiness for one usecase invocation (shared across segments)."""

    _ready: set[str] = attrs.field(factory=set, repr=False)
    _missing: set[str] = attrs.field(factory=set, repr=False)
    trace_events: list[CapabilityExecutionEvent] | None = attrs.field(
        default=None,
        repr=False,
    )
    # ....................... #

    def is_ready(self, keys: frozenset[str]) -> bool:
        if not keys:
            return True

        return all(k in self._ready and k not in self._missing for k in keys)

    # ....................... #

    def mark_success(self, keys: frozenset[str]) -> None:
        for k in keys:
            self._missing.discard(k)
            self._ready.add(k)

    # ....................... #

    def mark_missing(self, keys: frozenset[str]) -> None:
        for k in keys:
            self._ready.discard(k)
            self._missing.add(k)

    # ....................... #

    def record_execution(
        self,
        *,
        bucket: str,
        spec: SchedulableCapabilitySpec,
        impl: object,
        kind: CapabilityTraceKind,
        action: CapabilityTraceAction,
        detail: str | None = None,
    ) -> None:
        if self.trace_events is None:
            return

        self.trace_events.append(
            CapabilityExecutionEvent(
                bucket=bucket,
                label=capability_step_label(spec, impl),
                kind=kind,
                action=action,
                detail=detail,
            )
        )

    # ....................... #

    @property
    def execution_trace(self) -> tuple[CapabilityExecutionEvent, ...]:
        """Read-only view of events recorded for this invocation (if tracing was enabled)."""

        if self.trace_events is None:
            return ()

        return tuple(self.trace_events)
