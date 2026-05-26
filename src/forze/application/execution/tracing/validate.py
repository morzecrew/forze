"""Validate runtime traces with caller-supplied rules."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Literal

from .buffer import RuntimeTrace
from .events import TracingEvent, TracingViolation
from .report import format_runtime_trace_report

# ----------------------- #

RuntimeTraceValidator = Callable[[Sequence[TracingEvent]], list[TracingViolation]]
"""Callable that inspects observed events and returns rule violations."""


class RuntimeTraceValidationError(Exception):
    """Raised when :meth:`validate_runtime_trace` is called with ``on_violation='raise'``."""


# ....................... #


def validate_runtime_trace(
    trace: RuntimeTrace | Sequence[TracingEvent] | None,
    *,
    validator: RuntimeTraceValidator,
    on_violation: Literal["return", "raise"] = "return",
) -> list[TracingViolation]:
    """Return violations reported by *validator* (empty when valid)."""

    if trace is None:
        events: Sequence[TracingEvent] = ()
    elif isinstance(trace, RuntimeTrace):
        events = trace.events
    else:
        events = trace

    violations = validator(events)

    if violations and on_violation == "raise":
        buffer = trace if isinstance(trace, RuntimeTrace) else None
        report = format_runtime_trace_report(buffer, violations)
        raise RuntimeTraceValidationError(report)

    return violations


def assert_runtime_trace_valid(
    trace: RuntimeTrace | Sequence[TracingEvent] | None,
    *validators: RuntimeTraceValidator,
) -> None:
    """Raise :class:`RuntimeTraceValidationError` when any *validator* reports violations."""

    all_violations: list[TracingViolation] = []

    for validator in validators:
        all_violations.extend(
            validate_runtime_trace(trace, validator=validator, on_violation="return")
        )

    if all_violations:
        buffer = trace if isinstance(trace, RuntimeTrace) else None
        report = format_runtime_trace_report(buffer, all_violations)
        raise RuntimeTraceValidationError(report)
