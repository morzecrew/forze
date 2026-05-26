"""Tests for strict runtime trace validation."""

from __future__ import annotations

import pytest

from forze.application.execution import RuntimeTrace, TracingViolation
from forze.application.execution.tracing import (
    RuntimeTraceValidationError,
    assert_runtime_trace_valid,
    validate_runtime_trace,
)

# ----------------------- #


def _always_violate(_events) -> list[TracingViolation]:
    return [
        TracingViolation(profile="test", message="bad", at_seq=0),
    ]


class TestValidateRuntimeTraceStrict:
    def test_on_violation_raise(self) -> None:
        trace = RuntimeTrace()

        with pytest.raises(RuntimeTraceValidationError, match="bad"):
            validate_runtime_trace(
                trace,
                validator=_always_violate,
                on_violation="raise",
            )

    def test_assert_runtime_trace_valid(self) -> None:
        trace = RuntimeTrace()

        with pytest.raises(RuntimeTraceValidationError):
            assert_runtime_trace_valid(trace, _always_violate)
