"""Workflow-side saga helper: drive the shared `SagaProgress` with Temporal activities.

Use *inside* a Temporal ``@workflow.run`` to get Forze's pivot/compensation semantics over
activity-shaped steps. Temporal owns durability, retries, timeouts, and resume (per-activity
``RetryPolicy``/timeouts and the workflow history); this helper contributes only the saga
semantics, via the same :class:`~forze.application.contracts.saga.SagaProgress` the
in-process executor uses — so both drivers stay in lock-step.

Imports no ``temporalio`` at module load (the activity calls are supplied by the caller as
thunks), so it is safe to import inside the workflow sandbox; the failure path lazily imports
``temporalio.exceptions`` to convert a saga :class:`CoreException` into an ``ApplicationError``
(see :func:`_as_application_error`).
"""

from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, final

import attrs

from forze.application.contracts.saga import (
    SAGA_STEP_AMBIGUOUS_CODE,
    SagaProgress,
    SagaStepKind,
    saga_step_outcome_unknown,
)
from forze.application.contracts.transaction import COMMIT_AMBIGUOUS_CODE
from forze.base.exceptions import CoreException, exception_egress_policy

from .execution._logger import logger

if TYPE_CHECKING:
    from temporalio.exceptions import ApplicationError

# ----------------------- #


def _as_application_error(error: CoreException) -> "ApplicationError":
    """Convert a saga :class:`CoreException` into a temporalio ``ApplicationError``.

    A non-``FailureError`` raised out of ``@workflow.run`` fails the *workflow task* (which
    Temporal retries forever — the workflow never reaches ``FAILED``); an ``ApplicationError``
    fails the *workflow*. ``non_retryable`` follows the framework's per-kind retryability policy,
    so a deterministic saga failure (e.g. ``saga.step_failed`` = ``domain``) is marked
    non-retryable while an infrastructure failure stays retryable — except
    ``saga.step_ambiguous``, infrastructure-kind but pinned non-retryable: the interrupted
    step may have committed, so a retry re-runs the saga into a possible double-execution
    (reconcile before re-running). Imported lazily so this module still loads without
    ``temporalio`` (the failure path only runs inside a workflow).
    """

    from temporalio.exceptions import ApplicationError

    non_retryable = (
        error.code == SAGA_STEP_AMBIGUOUS_CODE or not exception_egress_policy(error.kind).retryable
    )

    return ApplicationError(
        error.summary,
        type=error.code,
        non_retryable=non_retryable,
    )


_MAX_CAUSE_HOPS = 16
"""Bound on the failure-cause walk in :func:`_step_outcome_unknown` (chains are short;
the bound only guards against a pathological cycle)."""


def _step_outcome_unknown(error: BaseException) -> bool:
    """:func:`saga_step_outcome_unknown`, extended across Temporal's failure wrappers.

    An activity that dies at its transaction commit raises the ``commit_ambiguous``
    :class:`CoreException` *inside the activity*; the workflow receives it wrapped —
    an ``ActivityError`` whose cause is the ``ApplicationError`` the failure converter
    built. Two wrapped shapes carry the code: a converter that maps the code into the
    error ``type``, and the default converter's ``type="CoreException"`` whose message
    keeps the parenthesized code from ``CoreException.__str__``. Checked lazily so the
    module still imports without ``temporalio``.
    """

    from temporalio.exceptions import ApplicationError

    marker = f"({COMMIT_AMBIGUOUS_CODE})"
    cursor: BaseException | None = error

    for _ in range(_MAX_CAUSE_HOPS):
        if cursor is None:
            return False

        if saga_step_outcome_unknown(cursor):
            return True

        if isinstance(cursor, ApplicationError) and (
            cursor.type == COMMIT_AMBIGUOUS_CODE
            or (cursor.type == "CoreException" and marker in (cursor.message or ""))
        ):
            return True

        nxt: BaseException | None = cursor.__cause__

        if nxt is None:
            candidate = getattr(cursor, "cause", None)
            nxt = candidate if isinstance(candidate, BaseException) else None

        cursor = nxt

    return False


@final
@attrs.define(slots=True, kw_only=True)
class TemporalSaga:
    """Drives :class:`SagaProgress` from inside a Temporal workflow.

    Call :meth:`step` per step in declaration order, declaring each step's ``name`` and
    ``kind`` at the call site and passing a thunk that runs the step's activity (and, for a
    compensatable step, a thunk that runs its compensation activity). The helper assigns the
    index and validates the `compensatable* pivot? retryable*` order as steps are declared —
    there are no parallel kind/name lists to keep in sync. A failure *before* the pivot runs
    the pushed compensations in reverse and raises ``saga.step_failed`` /
    ``saga.compensation_failed``; a failure *after* the pivot raises ``saga.forward_incomplete``
    and runs no compensation.
    """

    name: str

    # ....................... #

    _progress: SagaProgress = attrs.field(
        init=False,
        default=attrs.Factory(
            lambda self: SagaProgress(saga_name=self.name),
            takes_self=True,
        ),
    )
    _compensations: dict[int, Callable[[], Awaitable[object]]] = attrs.field(
        factory=dict,
        init=False,
    )

    # ....................... #

    @property
    def committed(self) -> bool:
        """Whether the pivot has succeeded — past here the saga completes forward."""

        return self._progress.committed

    # ....................... #

    async def step[T](
        self,
        name: str,
        run: Callable[[], Awaitable[T]],
        *,
        kind: SagaStepKind = SagaStepKind.COMPENSATABLE,
        compensation: Callable[[], Awaitable[object]] | None = None,
    ) -> T:
        """Run step *name*; compensate (before the pivot) or fail forward (after it)."""

        index = self._progress.register(name, kind)

        try:
            result = await run()

        except Exception as error:
            # An ambiguous step commit (interrupted at the commit itself) is NOT a
            # step failure: the step may be committed, so compensating would
            # split-brain and ``saga.step_failed`` would falsely certify consistency.
            # Checked through Temporal's failure wrappers — an activity's
            # CoreException reaches the workflow as an ActivityError chain.
            if _step_outcome_unknown(error):
                raise _as_application_error(
                    self._progress.step_ambiguous_error(index, error)
                ) from error

            if self._progress.committed:
                raise _as_application_error(
                    self._progress.forward_incomplete_error(index, error)
                ) from error

            comp_errors = await self._compensate()
            raise _as_application_error(
                self._progress.step_failed_error(index, error, comp_errors)
            ) from error

        self._progress.record_success(index)

        if compensation is not None:
            self._compensations[index] = compensation

        return result

    # ....................... #

    async def _compensate(self) -> list[BaseException]:
        errors: list[BaseException] = []

        for index in self._progress.steps_to_compensate():
            compensation = self._compensations.get(index)

            if compensation is None:
                continue

            try:
                await compensation()

            except Exception as comp_error:
                logger.warning("Saga compensation step failed", step_index=index, exc_info=True)
                errors.append(comp_error)

        return errors
