"""Validate runtime traces with caller-supplied rules."""

from __future__ import annotations

from collections.abc import Callable, Sequence

from .buffer import RuntimeTrace
from .events import TracingEvent, TracingViolation

# ----------------------- #

RuntimeTraceValidator = Callable[[Sequence[TracingEvent]], list[TracingViolation]]
"""Callable that inspects observed events and returns rule violations."""

# ....................... #


def validate_runtime_trace(
    trace: RuntimeTrace | Sequence[TracingEvent],
    *,
    validator: RuntimeTraceValidator,
) -> list[TracingViolation]:
    """Return violations reported by *validator* (empty when valid)."""

    events: Sequence[TracingEvent] = (
        trace.events if isinstance(trace, RuntimeTrace) else trace
    )
    return validator(events)
