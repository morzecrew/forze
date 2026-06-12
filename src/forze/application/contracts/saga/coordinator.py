"""The saga's pivot/compensation decision logic — shared by every driver.

A pure, ctx-free state machine over *structural* saga info (step kinds + names, no
callables), so an in-process executor and a Temporal workflow can drive identical
semantics. The driver performs the I/O (run an action / activity, run compensations); the
coordinator decides *compensate-in-reverse vs forward-incomplete* and builds the errors.

Kept lean (only ``base.exceptions`` + same-package ``SagaStepKind``) to hold the
``contracts.saga`` boundary and stay safe to import inside the Temporal workflow sandbox.
"""

from typing import Sequence, final

import attrs

from forze.base.exceptions import CoreException, exc

from .value_objects import SagaStepKind, validate_saga_order

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True)
class SagaProgress:
    """Tracks registered + completed steps and the pivot boundary, builds saga failures.

    Steps are registered incrementally via :meth:`register`, so a driver that declares
    steps one at a time (the Temporal workflow helper) and one that has them all up front
    (the in-process executor) share the exact same decision logic.
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
