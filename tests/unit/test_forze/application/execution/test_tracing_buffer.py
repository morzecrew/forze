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

    def test_record_ignored_after_truncation(self) -> None:
        trace = RuntimeTrace()
        original_max = RuntimeTrace.MAX_EVENTS
        RuntimeTrace.MAX_EVENTS = 1

        try:
            trace.next_event(domain="a", op="one")
            trace.next_event(domain="b", op="two")
            assert len(trace.events) == 2
            trace.record(trace.events[0])
            assert len(trace.events) == 2
        finally:
            RuntimeTrace.MAX_EVENTS = original_max


class TestRuntimeTraceFormatLines:
    def test_format_includes_optional_fields(self) -> None:
        trace = RuntimeTrace()
        trace.next_event(
            domain="document",
            op="query",
            surface="postgres",
            route="projects",
            phase="enter",
            tx_depth=2,
            tx_route="main",
        )

        text = trace.format_lines()

        assert "0000 document query" in text
        assert "surface=postgres" in text
        assert "route=projects" in text
        assert "phase=enter" in text
        assert "tx=main" in text
        assert "depth=2" in text
