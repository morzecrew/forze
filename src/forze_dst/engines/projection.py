"""Trace projection — fold the engine's runtime trace into history, then derive operation events.

The bridge between the execution engine's :class:`~forze.application.execution.tracing` trace and
the oracle's :class:`~forze_dst.oracle.recorder.History`. After a run, :func:`fold_runtime_trace` copies
every engine trace event into the recorded history (keeping its virtual-time stamp), then
:func:`project_operation_events` derives one convenience ``operation`` event per invoke→terminal
boundary — pairing each terminal to the *exact* invoke it belongs to by the correlation id the
engine stamps (so concurrent same-op calls attribute precisely). :func:`outcome_signature` distils
a run to its observable effect order, the equivalence key the DPOR engine prunes by.
"""

from __future__ import annotations

from typing import Any, Sequence

from forze.application.execution import ExecutionContext
from forze_dst.oracle.recorder import History, current_recorder, record_event

# ----------------------- #


def outcome_signature(history: History) -> tuple[Any, ...]:
    """The observable effect order of a run — operations + recorded facts, ignoring trace.

    Two interleavings with the same signature are observationally equivalent (same effects in
    the same order), so the explorer need not expand both — a partial-order reduction.
    """

    return tuple(
        (
            (event.kind, event.fields.get("op"), event.fields.get("outcome"))
            if event.kind == "operation"
            else (event.kind, tuple(sorted(event.fields.items(), key=lambda kv: kv[0])))
        )
        for event in history.events
        if event.kind not in ("trace", "op_start")
    )


# ....................... #


def fold_runtime_trace(ctx: ExecutionContext) -> None:
    """Fold the engine's runtime trace into history, then project per-op ``operation`` events.

    The engine trace is the single source of truth for execution events — ports, transactions,
    domain dispatch, and the operation invoke→complete|error boundary (the engine classifies the
    terminal ``ok`` / ``failed`` / ``error``). Each event is folded as a ``trace`` event keeping
    its stamp. Operation outcomes are then **projected** into convenience ``operation`` events,
    one per boundary, sourced entirely from the trace and correlated to the harness's ``op_start``
    anchors for the call id (an ``op_start`` is immediately followed by its invoke with no
    intervening await, so the i-th anchor matches the i-th invoke). The harness records no
    operation outcome of its own — the trace is the single source (decision D6=c)."""

    trace = ctx.deps.runtime_trace()

    if trace is None:
        return

    for event in trace.events:
        record_event(
            "trace",
            at=event.at,
            trace_seq=event.seq,
            trace_domain=event.domain,
            op=event.op,
            surface=event.surface,
            route=event.route,
            phase=event.phase,
            tx_depth=event.tx_depth,
            tx_id=event.tx_id,
            key=event.key,
            outcome=event.outcome,
            error=event.error,
            payload=event.payload,
            result=event.result,
            result_native=event.result_native,
        )

    project_operation_events(trace.events)


# ....................... #


def project_operation_events(trace_events: Sequence[Any]) -> None:
    """Project ``operation`` events from the folded trace's operation boundaries.

    Each invoke is matched to its terminal (complete/error) per op in FIFO order, and to the
    recorder's ``op_start`` anchors by global ordinal (for the call id). The projected event
    carries the trace's own sequence numbers as the span interval (``start_seq``/``end_seq`` —
    true execution order, which never collides, unlike a shared virtual-time stamp). A boundary
    with no terminal (the process crashed mid-call) is projected ``incomplete``.

    Correlation guarantee: per-*call* attribution is **exact**. Each terminal carries a
    correlation id (its invoke's ``seq``), so a terminal pairs to the precise invoke it belongs
    to — even for concurrent calls of the same op whose terminals complete out of invoke order.
    Top-level invokes are matched to the harness's ``op_start`` anchors in order (an ``op_start``
    is immediately followed by its invoke with no await), while *cascade* invokes (a saga /
    event-handler sub-operation, flagged ``nested`` on the trace) consume no anchor and are
    attributed ``call_id=-1``. The verdicts of ``completes_within`` / ``single_key_per_operation``
    and the report's ``call_id`` are therefore precise, not best-effort."""

    recorder = current_recorder()

    if recorder is None:
        return

    op_starts = [e for e in recorder.history.events if e.kind == "op_start"]

    # Single pass over the trace: collect invokes and index terminals by the correlation
    # id (their invoke's seq) ``run_operation`` stamps.
    invokes: list[Any] = []
    by_corr: dict[int, Any] = {}

    for event in trace_events:
        if event.domain != "operation":
            continue

        if event.phase == "invoke":
            invokes.append(event)

        elif event.phase in ("complete", "error"):
            by_corr[event.corr] = event

    top_level = 0

    for invoke in invokes:
        call_id: Any = -1  # a cascade has no top-level driver / op_start anchor

        if not invoke.nested:
            anchor = op_starts[top_level] if top_level < len(op_starts) else None
            if anchor is not None:
                call_id = anchor.fields.get("call_id")
            top_level += 1

        terminal = by_corr.get(invoke.seq)

        if terminal is None:
            outcome, error = "incomplete", None
            returned_at, end_seq = invoke.at, invoke.seq

        else:
            outcome = terminal.outcome or "ok"
            error = terminal.error
            returned_at, end_seq = terminal.at, terminal.seq

        record_event(
            "operation",
            at=returned_at,
            call_id=call_id,
            op=invoke.op,
            outcome=outcome,
            error=error,
            invoked_at=invoke.at,
            returned_at=returned_at,
            start_seq=invoke.seq,
            end_seq=end_seq,
        )
