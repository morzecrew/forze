"""The saga's pivot/compensation decision logic — shared by every driver.

A pure, ctx-free state machine over *structural* saga info (step kinds + names, no
callables), so an in-process executor and a external executor can drive identical
semantics. The driver performs the I/O (run an action / activity, run compensations); the
coordinator decides *compensate-in-reverse vs forward-incomplete* and builds the errors.
"""

from collections.abc import Sequence
from typing import final

import attrs

from forze.base.exceptions import CoreException, exc

from ..transaction import COMMIT_AMBIGUOUS_CODE
from .value_objects import SagaStepKind, validate_saga_order

# ----------------------- #

SAGA_STEP_AMBIGUOUS_CODE = "saga.step_ambiguous"
"""Error code of :meth:`SagaProgress.step_ambiguous_error` — an indeterminate saga.

Infrastructure-kind (an operator must act), but unlike its kind's default it must
never be blind-retried: the interrupted step may have committed, so a retry re-runs
the saga into a possible double-execution. Drivers whose retry semantics key on more
than the kind (the Temporal ``ApplicationError`` mapping) pin this code non-retryable
explicitly."""


def saga_step_outcome_unknown(error: BaseException) -> bool:
    """Whether a failed step's *commit outcome* is unknown (it may have committed).

    A cancellation landing at a step's transaction commit — an ordinary drain timeout —
    surfaces as a ``commit_ambiguous`` error. Reading it as a step *failure* would run
    the compensation pass against an outcome nobody knows: the step may be committed
    (leaving it standing while its predecessors are rolled back — split-brain) and the
    resulting ``saga.step_failed`` would assert a consistency that does not hold. Every
    driver must check this **before** its failure handling and raise
    :meth:`SagaProgress.step_ambiguous_error` instead, compensating nothing.
    """

    return isinstance(error, CoreException) and error.code == COMMIT_AMBIGUOUS_CODE


@final
@attrs.define(slots=True, kw_only=True)
class SagaProgress:
    """Tracks registered + completed steps and the pivot boundary, builds saga failures.

    Steps are registered incrementally via :meth:`register`, so a driver that declares
    steps one at a time and one that has them all up front share the exact same decision logic.
    """

    saga_name: str
    """Display name of the saga, used in validation and failure messages.

    Deliberately ``saga_name`` rather than ``name``: the coordinator also
    tracks per-*step* names, and the qualified field keeps the error-building
    code unambiguous. Shipped in v0.3.0, so renaming would also break custom
    drivers constructing :class:`SagaProgress` directly.
    """

    # ....................... #

    _kinds: list[SagaStepKind] = attrs.field(factory=list, init=False)
    _names: list[str] = attrs.field(factory=list, init=False)
    _completed: list[int] = attrs.field(factory=list, init=False)
    _committed: bool = attrs.field(default=False, init=False)

    # ....................... #

    @property
    def committed(self) -> bool:
        """Whether the pivot has succeeded — past here the saga must complete forward."""

        return self._committed

    # ....................... #

    def register(self, name: str, kind: SagaStepKind) -> int:
        """Append a step (validating the running order) and return its index."""

        self._kinds.append(kind)
        self._names.append(name)
        validate_saga_order(self._kinds, self._names, self.saga_name)

        return len(self._kinds) - 1

    # ....................... #

    def record_success(self, index: int) -> None:
        """Mark step *index* completed; cross the pivot if it is the pivot step."""

        self._completed.append(index)

        if self._kinds[index] is SagaStepKind.PIVOT:
            self._committed = True

    # ....................... #

    def steps_to_compensate(self) -> list[int]:
        """Completed step indices to compensate, in reverse completion order."""

        return list(reversed(self._completed))

    # ....................... #

    def step_failed_error(
        self,
        index: int,
        error: BaseException,
        comp_errors: Sequence[BaseException],
    ) -> CoreException:
        """Build the failure raised when a step fails *before* the pivot commits.

        The kind encodes the **saga outcome**, not the failing step's cause:

        - ``saga.step_failed`` is DOMAIN *by decision*: every compensation
          succeeded, so the system is consistent and "the process did not
          complete and was rolled back" is the saga's modeled business
          outcome — exactly what sagas exist to make a normal, handled
          result. DOMAIN egress exposes the details (saga/step/cause) to the
          caller and is non-retryable: a rolled-back saga must not be
          blind-retried by resilience policies; re-running it is a business
          decision. The step's original failure is not lost — drivers chain
          it as ``__cause__`` and ``details["cause"]`` carries its message.
          Deriving the wrapper's kind from the cause instead would make the
          saga's failure kind unpredictable to callers and require an
          arbitrary kind mapping for non-``CoreException`` causes.
        - ``saga.compensation_failed`` is INFRASTRUCTURE: the rollback itself
          failed, the system may be inconsistent, and an operator must act.
        """

        step_name = self._names[index]

        # NB: build details inline rather than via `.enrich()` — enrich does a lazy
        # import of `forze.base.serialization`, which the Temporal workflow sandbox
        # blocks (this coordinator runs inside the workflow under TemporalSaga).
        if comp_errors:
            return exc.infrastructure(
                f"Saga {self.saga_name!r} failed at step {step_name!r} and "
                f"compensation failed for {len(comp_errors)} step(s); manual "
                "intervention required.",
                code="saga.compensation_failed",
                details={
                    "saga": self.saga_name,
                    "step": step_name,
                    "cause": str(error),
                    "compensation_errors": [str(e) for e in comp_errors],
                },
            )

        # DOMAIN, deliberately — consistent state + modeled outcome (see docstring).
        return exc.domain(
            f"Saga {self.saga_name!r} failed at step {step_name!r}; completed steps "
            "were compensated.",
            code="saga.step_failed",
            details={
                "saga": self.saga_name,
                "step": step_name,
                "cause": str(error),
            },
        )

    # ....................... #

    def step_ambiguous_error(
        self,
        index: int,
        error: BaseException,
    ) -> CoreException:
        """Build the failure raised when a step's commit outcome is unknown.

        INFRASTRUCTURE, like ``saga.compensation_failed``: the system may be
        inconsistent and an operator must act. Nothing was compensated — with the
        step's outcome unknown, compensating the completed steps would roll them back
        around an effect that may be committed, and compensating the ambiguous step
        itself could undo work that never happened. Deliberately **not** DOMAIN: the
        consistency claim ``saga.step_failed`` carries ("completed steps were
        compensated") cannot be made here, and a durable journal must not record this
        saga as a cleanly rolled-back business outcome.
        """

        step_name = self._names[index]

        return exc.infrastructure(
            f"Saga {self.saga_name!r} was interrupted at step {step_name!r} and the "
            "step's commit outcome is unknown (it may have committed); completed "
            "steps were NOT compensated. Reconcile the step's effect manually "
            "before re-running.",
            code=SAGA_STEP_AMBIGUOUS_CODE,
            details={
                "saga": self.saga_name,
                "step": step_name,
                "cause": str(error),
            },
        )

    # ....................... #

    def forward_incomplete_error(
        self,
        index: int,
        error: BaseException,
    ) -> CoreException:
        """Build the failure raised when a step fails *after* the pivot commits."""

        step_name = self._names[index]

        return exc.infrastructure(
            f"Saga {self.saga_name!r} committed at its pivot but could not complete "
            f"forward at step {step_name!r}; it must be completed, not compensated.",
            code="saga.forward_incomplete",
            details={
                "saga": self.saga_name,
                "step": step_name,
                "cause": str(error),
            },
        )
