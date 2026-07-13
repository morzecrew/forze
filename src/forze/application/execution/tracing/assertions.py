"""Test-facing runtime-trace checks: validators, golden-trace matching, and formatting."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Literal, final

import attrs

from forze.base.exceptions import exc

from .trace import RuntimeTrace, TracingEvent, TracingViolation

# ----------------------- #


def format_violation(violation: TracingViolation) -> str:
    """Return a single-line description of *violation*."""

    return f"[{violation.profile}] seq={violation.at_seq}: {violation.message}"


def format_violations(violations: Sequence[TracingViolation]) -> str:
    """Return all *violations* as newline-separated lines."""

    return "\n".join(format_violation(v) for v in violations)


def format_runtime_trace_report(
    trace: RuntimeTrace | None,
    violations: Sequence[TracingViolation],
) -> str:
    """Return a human-readable report with violations and trace lines."""

    parts: list[str] = []

    if violations:
        parts.append("Runtime trace violations:")
        parts.append(format_violations(violations))
    else:
        parts.append("Runtime trace violations: (none)")

    parts.append("")
    parts.append("Runtime trace:")

    if trace is None or not trace.events:
        parts.append("(no trace recorded — enable Deps.trace_runtime)")
    else:
        parts.append(trace.format_lines())

    return "\n".join(parts)


# ----------------------- #

RuntimeTraceValidator = Callable[[Sequence[TracingEvent]], list[TracingViolation]]
"""Callable that inspects observed events and returns rule violations."""

# ....................... #


class RuntimeTraceValidationError(Exception):
    """Raised when :meth:`validate_runtime_trace` is called with ``on_violation='raise'``.

    Deliberately a raw ``Exception``, not ``CoreException``: a test/CI harness
    signal that must fail the run loudly and must never be caught or mapped by
    the framework error envelope.
    """


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


# ....................... #


def assert_runtime_trace_valid(
    trace: RuntimeTrace | Sequence[TracingEvent] | None,
    *validators: RuntimeTraceValidator,
) -> None:
    """Raise :class:`RuntimeTraceValidationError` when any *validator* reports violations."""

    all_violations: list[TracingViolation] = []

    for validator in validators:
        all_violations.extend(
            validate_runtime_trace(
                trace,
                validator=validator,
                on_violation="return",
            )
        )

    if all_violations:
        buffer = trace if isinstance(trace, RuntimeTrace) else None
        report = format_runtime_trace_report(buffer, all_violations)

        raise RuntimeTraceValidationError(report)


# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class TraceExpectation:
    """Expected event subset for ordered trace matching."""

    domain: str
    op: str
    surface: str | None = None
    route: str | None = None
    phase: str | None = None
    tx_depth: int | None = None
    """When set, ``TracingEvent.tx_depth`` must equal this value."""


# ....................... #


def _event_matches(event: TracingEvent, expectation: TraceExpectation) -> bool:
    if event.domain != expectation.domain or event.op != expectation.op:
        return False

    if expectation.surface is not None and event.surface != expectation.surface:
        return False

    if expectation.route is not None and event.route != expectation.route:
        return False

    if expectation.phase is not None and event.phase != expectation.phase:
        return False

    return not (expectation.tx_depth is not None and event.tx_depth != expectation.tx_depth)


# ....................... #


def _events_from(
    trace: RuntimeTrace | Sequence[TracingEvent],
) -> Sequence[TracingEvent]:
    if isinstance(trace, RuntimeTrace):
        return trace.events

    return trace


# ....................... #


def assert_trace_contains(
    trace: RuntimeTrace | Sequence[TracingEvent] | None,
    expectations: Sequence[TraceExpectation],
) -> None:
    """Assert *expectations* appear in order within *trace* (not necessarily adjacent)."""

    if trace is None:
        events: Sequence[TracingEvent] = ()

    else:
        events = _events_from(trace)

    index = 0

    for expectation in expectations:
        matched = False

        while index < len(events):
            if _event_matches(events[index], expectation):
                matched = True
                index += 1
                break
            index += 1

        if not matched:
            msg = (
                f"Expected trace to contain {expectation!r} "
                f"after index {index - 1}; trace has {len(events)} event(s)"
            )
            raise exc.internal(msg)


# ....................... #


def assert_trace_equals(
    trace: RuntimeTrace | Sequence[TracingEvent] | None,
    expectations: Sequence[TraceExpectation],
) -> None:
    """Assert *trace* has exactly len(expectations) events matching in order."""

    if trace is None:
        events: list[TracingEvent] = []

    else:
        events = list(_events_from(trace))

    if len(events) != len(expectations):
        msg = f"Expected {len(expectations)} trace event(s), got {len(events)}"

        raise exc.internal(msg)

    for event, expectation in zip(events, expectations, strict=True):
        if not _event_matches(event, expectation):
            msg = f"Event {event!r} does not match expectation {expectation!r}"

            raise exc.internal(msg)
