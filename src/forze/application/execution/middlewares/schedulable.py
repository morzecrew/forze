"""Validation helpers for capability-scheduled hooks."""

from __future__ import annotations

from forze.base.errors import CoreError

from .value_objects import Skip


def ensure_schedulable_control(out: object, *, kind: str) -> None | Skip:
    """Reject invalid return values from guards and success hooks in capability stages.

    Capability hooks scheduled via :mod:`forze.application.execution.engine.capabilities`
    must return ``None`` (continue) or :class:`Skip` (skip providing capabilities).
    """

    if out is None:
        return None
    if isinstance(out, Skip):
        return out
    msg = f"{kind} capability must return None or Skip, got {type(out).__qualname__!r}."
    raise CoreError(msg)
