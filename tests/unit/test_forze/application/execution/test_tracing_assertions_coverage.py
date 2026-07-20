"""Coverage for the test-facing runtime-trace assertion helpers.

Exercises the formatting, validation, and ordered-matching paths directly against
hand-built ``TracingEvent`` / ``TracingViolation`` values — no runtime needed.
"""

from __future__ import annotations

from collections.abc import Sequence

import pytest

from forze.application.execution.tracing.assertions import (
    RuntimeTraceValidationError,
    TraceExpectation,
    assert_runtime_trace_valid,
    assert_trace_contains,
    assert_trace_equals,
    format_runtime_trace_report,
    validate_runtime_trace,
)
from forze.application.execution.tracing.trace import (
    RuntimeTrace,
    TracingEvent,
    TracingViolation,
)
from forze.base.exceptions import CoreException

# ----------------------- #


def _event(**overrides: object) -> TracingEvent:
    base: dict[str, object] = {"seq": 0, "domain": "document", "op": "get"}
    base.update(overrides)
    return TracingEvent(**base)  # type: ignore[arg-type]


def _one_violation(_events: Sequence[TracingEvent]) -> list[TracingViolation]:
    return [TracingViolation(profile="rule", message="boom", at_seq=0)]


def _no_violation(_events: Sequence[TracingEvent]) -> list[TracingViolation]:
    return []


# ....................... #


class TestFormatRuntimeTraceReport:
    def test_no_violations_no_trace(self) -> None:
        report = format_runtime_trace_report(None, [])

        assert "(none)" in report
        assert "no trace recorded" in report

    def test_with_violations_and_trace_events(self) -> None:
        trace = RuntimeTrace()
        trace.next_event(domain="document", op="get")

        report = format_runtime_trace_report(
            trace, [TracingViolation(profile="rule", message="boom", at_seq=0)]
        )

        assert "boom" in report  # format_violations
        assert "document" in report  # trace.format_lines()


# ....................... #


class TestValidateRuntimeTrace:
    def test_none_trace_returns_empty(self) -> None:
        assert validate_runtime_trace(None, validator=_no_violation) == []

    def test_raw_sequence_input(self) -> None:
        events = [_event()]

        assert validate_runtime_trace(events, validator=_no_violation) == []

    def test_raise_on_violation(self) -> None:
        trace = RuntimeTrace()
        trace.next_event(domain="document", op="get")

        with pytest.raises(RuntimeTraceValidationError):
            validate_runtime_trace(
                trace, validator=_one_violation, on_violation="raise"
            )


# ....................... #


class TestAssertRuntimeTraceValid:
    def test_passes_with_no_violations(self) -> None:
        assert_runtime_trace_valid(RuntimeTrace(), _no_violation)

    def test_raises_with_violations(self) -> None:
        with pytest.raises(RuntimeTraceValidationError):
            assert_runtime_trace_valid(RuntimeTrace(), _one_violation)


# ....................... #


class TestEventMatching:
    """Each mismatched attribute makes ``_event_matches`` return ``False``."""

    def test_surface_mismatch(self) -> None:
        with pytest.raises(CoreException):
            assert_trace_contains(
                [_event(surface="document_query")],
                [TraceExpectation(domain="document", op="get", surface="other")],
            )

    def test_route_mismatch(self) -> None:
        with pytest.raises(CoreException):
            assert_trace_contains(
                [_event(route="a")],
                [TraceExpectation(domain="document", op="get", route="b")],
            )

    def test_phase_mismatch(self) -> None:
        with pytest.raises(CoreException):
            assert_trace_contains(
                [_event(phase="query")],
                [TraceExpectation(domain="document", op="get", phase="command")],
            )

    def test_tx_depth_mismatch(self) -> None:
        with pytest.raises(CoreException):
            assert_trace_contains(
                [_event(tx_depth=1)],
                [TraceExpectation(domain="document", op="get", tx_depth=2)],
            )


# ....................... #


class TestAssertTraceContains:
    def test_matches_in_order_raw_sequence(self) -> None:
        events = [_event(op="a"), _event(seq=1, op="b")]

        assert_trace_contains(
            events,
            [
                TraceExpectation(domain="document", op="a"),
                TraceExpectation(domain="document", op="b"),
            ],
        )

    def test_none_trace_raises(self) -> None:
        with pytest.raises(CoreException):
            assert_trace_contains(None, [TraceExpectation(domain="document", op="a")])


# ....................... #


class TestAssertTraceEquals:
    def test_exact_match(self) -> None:
        assert_trace_equals(
            [_event(op="a")], [TraceExpectation(domain="document", op="a")]
        )

    def test_none_trace_with_expectations_raises(self) -> None:
        with pytest.raises(CoreException):
            assert_trace_equals(None, [TraceExpectation(domain="document", op="a")])

    def test_event_mismatch_raises(self) -> None:
        with pytest.raises(CoreException):
            assert_trace_equals(
                [_event(op="a")], [TraceExpectation(domain="document", op="b")]
            )
