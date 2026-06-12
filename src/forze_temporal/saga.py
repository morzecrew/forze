"""Workflow-side saga helper: drive the shared `SagaProgress` with Temporal activities.

Use *inside* a Temporal ``@workflow.run`` to get Forze's pivot/compensation semantics over
activity-shaped steps. Temporal owns durability, retries, timeouts, and resume (per-activity
``RetryPolicy``/timeouts and the workflow history); this helper contributes only the saga
semantics, via the same :class:`~forze.application.contracts.saga.SagaProgress` the
in-process executor uses — so both drivers stay in lock-step.

Imports no ``temporalio`` (the activity calls are supplied by the caller as thunks), so it is
safe to import inside the workflow sandbox.
"""

from typing import Awaitable, Callable, final

import attrs

from forze.application.contracts.saga import SagaProgress, SagaStepKind

# ----------------------- #


@final
@attrs.define(slots=True)
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

    name: str = attrs.field(kw_only=True)

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
            if self._progress.committed:
                raise self._progress.forward_incomplete_error(index, error) from error

            comp_errors = await self._compensate()
            raise self._progress.step_failed_error(index, error, comp_errors) from error

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

            except Exception as comp_error:  # noqa: BLE001 — best-effort; collect all
                errors.append(comp_error)

        return errors
