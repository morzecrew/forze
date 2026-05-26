"""Tests for RuntimeTrace buffer limits."""

from __future__ import annotations

from forze.application.execution import RuntimeTrace

# ----------------------- #


class TestRuntimeTraceMaxEvents:
    def test_truncation_marker(self) -> None:
        trace = RuntimeTrace()
        original_max = RuntimeTrace.MAX_EVENTS
        RuntimeTrace.MAX_EVENTS = 3

        try:
            for i in range(5):
                trace.next_event(domain="document", op=f"op{i}")

            assert len(trace.events) == 4
            assert trace.events[-1].domain == "tracing"
            assert trace.events[-1].op == "truncated"
        finally:
            RuntimeTrace.MAX_EVENTS = original_max
