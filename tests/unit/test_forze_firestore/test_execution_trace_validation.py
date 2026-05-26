"""Unit tests for Firestore runtime-trace validation."""

from __future__ import annotations

from forze.application.execution import RuntimeTrace, validate_runtime_trace
from forze_firestore.execution.trace_validation import (
    validate_reads_before_writes_in_tx,
)

# ----------------------- #

_VALIDATOR = validate_reads_before_writes_in_tx


class TestFirestoreReadsBeforeWritesInTx:
    def test_create_then_get_outside_tx_is_valid(self) -> None:
        trace = RuntimeTrace()
        trace.next_event(
            domain="document",
            op="create",
            surface="document_command",
            route="projects",
            phase="command",
            tx_depth=0,
        )
        trace.next_event(
            domain="document",
            op="get",
            surface="document_query",
            route="projects",
            phase="query",
            tx_depth=0,
        )

        assert validate_runtime_trace(trace, validator=_VALIDATOR) == []

    def test_read_after_write_in_tx_violates(self) -> None:
        trace = RuntimeTrace()
        trace.next_event(
            domain="tx",
            op="enter",
            route="mock",
            tx_route="mock",
            tx_depth=1,
        )
        trace.next_event(
            domain="document",
            op="create",
            surface="document_command",
            route="projects",
            phase="command",
            tx_depth=1,
        )
        trace.next_event(
            domain="document",
            op="get",
            surface="document_query",
            route="projects",
            phase="query",
            tx_depth=1,
        )

        violations = validate_runtime_trace(trace, validator=_VALIDATOR)

        assert len(violations) == 1
        assert violations[0].profile == "reads_before_writes_in_tx"
        assert "after a command write" in violations[0].message
        assert violations[0].at_seq == 2

    def test_tx_exit_resets_segment(self) -> None:
        trace = RuntimeTrace()
        trace.next_event(
            domain="tx",
            op="enter",
            route="mock",
            tx_route="mock",
            tx_depth=1,
        )
        trace.next_event(
            domain="document",
            op="create",
            surface="document_command",
            route="projects",
            phase="command",
            tx_depth=1,
        )
        trace.next_event(
            domain="tx",
            op="exit",
            route="mock",
            tx_route="mock",
            tx_depth=1,
        )
        trace.next_event(
            domain="document",
            op="get",
            surface="document_query",
            route="projects",
            phase="query",
            tx_depth=0,
        )

        assert validate_runtime_trace(trace, validator=_VALIDATOR) == []
