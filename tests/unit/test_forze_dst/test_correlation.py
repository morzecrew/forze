"""Exact per-call attribution (E3.1) — terminals correlate to the precise invoke.

The trace projection (`_project_operation_events`) pairs each operation terminal to its invoke
by a correlation id (the invoke's `seq`, carried back on the terminal by `run_operation`), not
per-op FIFO. These tests pin the cases FIFO got wrong: two concurrent calls of the *same* op
whose terminals complete out of invoke order (the verdict-flipping bug for `completes_within` /
`single_key_per_operation`), and cascade sub-operations that have no top-level `op_start` anchor.
"""

from __future__ import annotations

from forze.application.execution.tracing.events import TracingEvent
from forze_dst.harness import (  # pyright: ignore[reportPrivateUsage]
    _project_operation_events,
)
from forze_dst.recorder import Recorder, bind_recorder

# ----------------------- #


def _invoke(seq: int, op: str, *, at: float, nested: bool = False) -> TracingEvent:
    return TracingEvent(seq=seq, at=at, domain="operation", op=op, phase="invoke", nested=nested)


def _terminal(
    seq: int, op: str, *, at: float, corr: int, outcome: str, error: str | None = None
) -> TracingEvent:
    return TracingEvent(
        seq=seq, at=at, domain="operation", op=op,
        phase="complete" if outcome == "ok" else "error",
        outcome=outcome, error=error, corr=corr,
    )


def _project(trace_events: list[TracingEvent], *, op_starts: list[int]) -> list[dict]:
    """Project *trace_events* under a recorder seeded with the given top-level op_start call ids."""

    recorder = Recorder(seed=0)
    with bind_recorder(recorder):
        for call_id in op_starts:
            recorder.record("op_start", call_id=call_id, op="pay")
        _project_operation_events(trace_events)

    return [
        dict(event.fields)
        for event in recorder.history.events
        if event.kind == "operation"
    ]


# ....................... #


class TestExactPairing:
    def test_same_op_terminals_out_of_invoke_order_attribute_correctly(self) -> None:
        # Two concurrent "pay" calls: A (invoke seq 0) is slow and errors; B (invoke seq 1) is
        # fast and succeeds, so B's terminal lands *first*. FIFO would credit A with B's quick ok.
        trace = [
            _invoke(0, "pay", at=0.0),
            _invoke(1, "pay", at=0.0),
            _terminal(2, "pay", at=0.1, corr=1, outcome="ok"),  # B finishes first
            _terminal(3, "pay", at=0.5, corr=0, outcome="error", error="KeyError"),  # A later
        ]

        ops = _project(trace, op_starts=[0, 1])
        by_invoke = {op["start_seq"]: op for op in ops}

        # A (invoke 0) is the slow error; B (invoke 1) is the fast ok — paired by corr, not FIFO.
        assert by_invoke[0]["outcome"] == "error"
        assert by_invoke[0]["returned_at"] == 0.5
        assert by_invoke[0]["call_id"] == 0
        assert by_invoke[1]["outcome"] == "ok"
        assert by_invoke[1]["returned_at"] == 0.1
        assert by_invoke[1]["call_id"] == 1

    def test_completes_within_verdict_is_now_exact(self) -> None:
        # The duration each call is credited (returned_at - invoked_at) follows the corr pairing,
        # so a per-call latency invariant reads the right elapsed for the right call.
        trace = [
            _invoke(0, "pay", at=0.0),
            _invoke(1, "pay", at=0.0),
            _terminal(2, "pay", at=0.2, corr=1, outcome="ok"),
            _terminal(3, "pay", at=1.0, corr=0, outcome="ok"),
        ]
        ops = _project(trace, op_starts=[0, 1])
        elapsed = {op["start_seq"]: op["returned_at"] - op["invoked_at"] for op in ops}

        assert elapsed[0] == 1.0  # the slow call, not mis-credited the fast 0.2
        assert elapsed[1] == 0.2


class TestCascadeAttribution:
    def test_cascade_invoke_consumes_no_anchor_and_is_marked(self) -> None:
        # A top-level "pay" (invoke 0) whose handler cascades into a "charge" sub-op (invoke 1,
        # nested). The cascade must not steal the next op_start anchor; it is attributed -1.
        trace = [
            _invoke(0, "pay", at=0.0),
            _invoke(1, "charge", at=0.1, nested=True),
            _terminal(2, "charge", at=0.2, corr=1, outcome="ok"),
            _terminal(3, "pay", at=0.3, corr=0, outcome="ok"),
        ]
        ops = _project(trace, op_starts=[0])  # only ONE top-level driver
        by_op = {op["op"]: op for op in ops}

        assert by_op["pay"]["call_id"] == 0  # the top-level driver keeps its anchor
        assert by_op["charge"]["call_id"] == -1  # the cascade is not mis-anchored

    def test_cascade_after_top_level_does_not_shift_later_anchors(self) -> None:
        # pay#0 cascades to charge; then a second top-level pay#1 runs. The cascade between them
        # must not consume pay#1's anchor (the FIFO-by-ordinal bug).
        trace = [
            _invoke(0, "pay", at=0.0),
            _invoke(1, "charge", at=0.1, nested=True),
            _terminal(2, "charge", at=0.2, corr=1, outcome="ok"),
            _terminal(3, "pay", at=0.3, corr=0, outcome="ok"),
            _invoke(4, "pay", at=0.4),
            _terminal(5, "pay", at=0.5, corr=4, outcome="ok"),
        ]
        ops = _project(trace, op_starts=[0, 1])
        pays = sorted(
            (op for op in ops if op["op"] == "pay"), key=lambda op: op["start_seq"]
        )

        assert [op["call_id"] for op in pays] == [0, 1]  # both top-level pays anchored in order


class TestIncomplete:
    def test_invoke_without_a_terminal_is_incomplete(self) -> None:
        # A crash mid-call leaves an invoke with no terminal — projected ``incomplete``.
        trace = [_invoke(0, "pay", at=0.0)]
        ops = _project(trace, op_starts=[0])
        assert ops[0]["outcome"] == "incomplete"
        assert ops[0]["call_id"] == 0
