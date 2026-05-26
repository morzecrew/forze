"""Runtime-trace validators for Firestore transaction semantics."""

from __future__ import annotations

from collections.abc import Sequence

from forze.application.execution.tracing import TracingEvent, TracingViolation

# ----------------------- #

_PROFILE = "reads_before_writes_in_tx"

_WRITE_PREFIXES = (
    "create",
    "update",
    "touch",
    "kill",
    "delete",
    "restore",
    "ensure",
    "upsert",
)

_READ_PREFIXES = (
    "get",
    "find",
    "count",
)


def _is_write_op(op: str) -> bool:
    return any(op == prefix or op.startswith(f"{prefix}_") for prefix in _WRITE_PREFIXES)


def _is_read_op(op: str) -> bool:
    return any(op == prefix or op.startswith(f"{prefix}_") for prefix in _READ_PREFIXES)


def validate_reads_before_writes_in_tx(
    events: Sequence[TracingEvent],
) -> list[TracingViolation]:
    """Flag document query reads after command writes within the same tx segment.

    Matches Firestore's requirement that all reads precede writes in a transaction.
    Pass as ``validator`` to
    :func:`forze.application.execution.tracing.validate_runtime_trace`.
    """

    violations: list[TracingViolation] = []
    write_seen_in_segment = False
    segment_depth = 0

    for event in events:
        if event.domain == "tx":
            if event.op == "enter":
                write_seen_in_segment = False
                segment_depth = event.tx_depth
            elif event.op == "exit" and event.tx_depth == segment_depth:
                write_seen_in_segment = False
                segment_depth = 0
            continue

        if event.tx_depth == 0:
            continue

        if event.surface == "document_command" and _is_write_op(event.op):
            write_seen_in_segment = True
            continue

        if (
            write_seen_in_segment
            and event.surface == "document_query"
            and _is_read_op(event.op)
        ):
            violations.append(
                TracingViolation(
                    profile=_PROFILE,
                    message=(
                        f"document query read '{event.op}' on route '{event.route}' "
                        f"after a command write in the same transaction (seq {event.seq})"
                    ),
                    at_seq=event.seq,
                )
            )

    return violations
