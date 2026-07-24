"""The drain-gate refusal classification, shared by every consumer runner.

A rolling deploy flips the drain gate before a consumer loop's stop signal arrives, so
a handler's dispatch can be refused with ``THROTTLED``/``code="draining"`` mid-batch.
That is a shutdown artifact, never poison — but the consequence lives in each runner's
own ladder (the queue runner requeues without counting, the offset-log runner stops
without committing past), and when the classification was inline per runner, only the
runner it was written in had it: the commit-stream twin dead-lettered a healthy message
on graceful shutdown. One predicate here, imported by both, so the ladders cannot
drift on *what counts as draining* again.
"""

from typing import Final

from forze.base.exceptions import CoreException

# ----------------------- #

_DRAINING_CODE: Final[str] = "draining"
"""Drain-gate refusal code (``THROTTLED``/``code="draining"``): the runtime is
quiescing, not a handler defect. Kept in sync with ``DrainGate.admit`` in the
execution plane."""


def is_draining_refusal(error: BaseException) -> bool:
    """Whether *error* is the drain gate refusing admission (a shutdown signal).

    Every runner must branch on this **before** its retry/poison ladder: a draining
    refusal is not a delivery attempt, must never count toward ``max_attempts``, and
    must never dead-letter — the loop stops and the message is redelivered by the
    next process.
    """

    return isinstance(error, CoreException) and error.code == _DRAINING_CODE
