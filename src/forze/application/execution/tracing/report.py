"""Format runtime traces and validation reports for logs and test failures."""

from __future__ import annotations

from collections.abc import Sequence

from .buffer import RuntimeTrace
from .events import TracingViolation

# ----------------------- #


def format_violation(violation: TracingViolation) -> str:
    """Return a single-line description of *violation*."""

    return (
        f"[{violation.profile}] seq={violation.at_seq}: {violation.message}"
    )


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
