"""Tests for trace expectation matching."""

from __future__ import annotations

import pytest

from forze.application.execution import (
    RuntimeTrace,
    TraceExpectation,
    assert_trace_contains,
    assert_trace_equals,
)
from forze.base.exceptions import CoreException

# ----------------------- #


class TestAssertTraceContains:
    def test_subsequence_match(self) -> None:
        trace = RuntimeTrace()
        trace.next_event(domain="tx", op="enter", tx_depth=1)
        trace.next_event(
            domain="document", op="get", surface="document_query", tx_depth=0
        )
        trace.next_event(domain="tx", op="exit", tx_depth=1)

        assert_trace_contains(
            trace,
            [
                TraceExpectation(domain="document", op="get"),
                TraceExpectation(domain="tx", op="exit"),
            ],
        )

    def test_missing_expectation_raises(self) -> None:
        trace = RuntimeTrace()
        trace.next_event(domain="tx", op="enter", tx_depth=1)

        with pytest.raises(CoreException, match="Expected trace to contain"):
            assert_trace_contains(
                trace,
                [TraceExpectation(domain="document", op="create")],
            )


class TestAssertTraceEquals:
    def test_exact_match(self) -> None:
        trace = RuntimeTrace()
        trace.next_event(domain="tx", op="enter", tx_depth=1)
        trace.next_event(domain="tx", op="exit", tx_depth=1)

        assert_trace_equals(
            trace,
            [
                TraceExpectation(domain="tx", op="enter", tx_depth=1),
                TraceExpectation(domain="tx", op="exit", tx_depth=1),
            ],
        )

    def test_length_mismatch_raises(self) -> None:
        trace = RuntimeTrace()
        trace.next_event(domain="tx", op="enter", tx_depth=1)

        with pytest.raises(CoreException, match="Expected 2 trace event"):
            assert_trace_equals(
                trace,
                [
                    TraceExpectation(domain="tx", op="enter"),
                    TraceExpectation(domain="tx", op="exit"),
                ],
            )
