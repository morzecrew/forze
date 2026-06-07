"""In-process saga executor: per-step transactions, reverse compensation on failure."""

from typing import TYPE_CHECKING, Awaitable, Callable, final

import attrs

from forze.application.contracts.saga import (
    SagaDefinition,
    SagaProgress,
    SagaStep,
    SagaStepKind,
)
from forze.base.exceptions import exc

from ..resilience import resolve_resilience_executor
from ..tracing import record

if TYPE_CHECKING:
    from ..context import ExecutionContext

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True)
class InProcessSagaExecutor:
    """Runs a saga synchronously in the current process.

    Each step commits in its own transaction; a step failure compensates the completed
    steps in reverse. Not crash-resumable — a process crash mid-saga leaves committed
    steps un-compensated (use the durable adapter for resumable orchestration).
    """

    async def run[Ctx](
        self,
        ctx: ExecutionContext,
        definition: SagaDefinition[Ctx],
        initial: Ctx,
    ) -> Ctx:
        if ctx.tx_ctx.depth() != 0:
            raise exc.configuration(
                "A saga must run outside a transaction so each step commits "
                f"independently; saga {definition.name!r} was started inside an open "
                "transaction."
            )

        self._emit("saga_started", definition, None)

        progress = SagaProgress(saga_name=str(definition.name))

        for step in definition.steps:
            progress.register(str(step.name), step.kind)

        state = initial
        states: dict[int, Ctx] = {}  # context as of each completed step, by index

        for index, step in enumerate(definition.steps):
            try:
                state = await self._run_step(ctx, step, state)

            except Exception as error:
                self._emit("step_failed", definition, step)

                if progress.committed:
                    # Past the pivot: the saga is committed. Do NOT compensate — it must
                    # complete forward (manually / asynchronously) instead.
                    raise progress.forward_incomplete_error(index, error) from error

                comp_errors = await self._compensate(ctx, definition, progress, states)
                raise progress.step_failed_error(index, error, comp_errors) from error

            progress.record_success(index)
            states[index] = state
            self._emit("step_completed", definition, step)

            if step.kind is SagaStepKind.PIVOT:
                self._emit("saga_committed", definition, step)

        self._emit("saga_completed", definition, None)

        return state

    # ....................... #

    async def _run_step[Ctx](
        self,
        ctx: ExecutionContext,
        step: SagaStep[Ctx],
        state: Ctx,
    ) -> Ctx:
        async def _action() -> Ctx:
            if step.tx_route is not None:
                async with ctx.tx_ctx.scope(step.tx_route):
                    return await step.action(ctx, state)

            return await step.action(ctx, state)

        if step.retry_policy is not None:
            return await resolve_resilience_executor(ctx).run(
                _action, policy=step.retry_policy
            )

        return await _action()

    # ....................... #

    async def _compensate[Ctx](
        self,
        ctx: ExecutionContext,
        definition: SagaDefinition[Ctx],
        progress: SagaProgress,
        states: dict[int, Ctx],
    ) -> list[BaseException]:
        errors: list[BaseException] = []

        for index in progress.steps_to_compensate():
            step = definition.steps[index]
            compensation = step.compensation

            if compensation is None:
                continue

            try:
                await self._run_compensation(ctx, step, compensation, states[index])
                self._emit("compensated", definition, step)

            except Exception as comp_error:  # noqa: BLE001 — best-effort; collect all
                errors.append(comp_error)
                self._emit("compensation_failed", definition, step)

        return errors

    # ....................... #

    async def _run_compensation[Ctx](
        self,
        ctx: ExecutionContext,
        step: SagaStep[Ctx],
        compensation: Callable[[ExecutionContext, Ctx], Awaitable[None]],
        state: Ctx,
    ) -> None:
        async def _comp() -> None:
            if step.tx_route is not None:
                async with ctx.tx_ctx.scope(step.tx_route):
                    await compensation(ctx, state)

            else:
                await compensation(ctx, state)

        if step.compensation_policy is not None:
            await resolve_resilience_executor(ctx).run(
                _comp, policy=step.compensation_policy
            )

        else:
            await _comp()

    # ....................... #

    def _emit[Ctx](
        self,
        op: str,
        definition: SagaDefinition[Ctx],
        step: SagaStep[Ctx] | None,
    ) -> None:
        record(
            domain="saga",
            op=op,
            surface="saga_executor",
            route=str(step.name) if step is not None else None,
            phase=str(definition.name),
        )
