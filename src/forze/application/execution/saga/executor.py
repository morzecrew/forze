"""In-process saga executor: per-step transactions, reverse compensation on failure."""

from __future__ import annotations

from typing import TYPE_CHECKING, final

import attrs

from forze.application.contracts.saga import SagaStepKind
from forze.base.exceptions import CoreException, exc

from ..resilience import resolve_resilience_executor
from ..tracing import record

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from forze.application.contracts.saga import SagaDefinition, SagaStep

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

        state = initial
        completed: list[tuple[SagaStep[Ctx], Ctx]] = []
        committed = False

        for step in definition.steps:
            try:
                state = await self._run_step(ctx, step, state)

            except Exception as error:
                self._emit("step_failed", definition, step)

                if committed:
                    # Past the pivot: the saga is committed. Do NOT compensate — it must
                    # complete forward (manually / asynchronously) instead.
                    raise self._forward_incomplete(definition, step, error) from error

                comp_errors = await self._compensate(ctx, definition, completed)
                raise self._saga_error(definition, step, error, comp_errors) from error

            completed.append((step, state))
            self._emit("step_completed", definition, step)

            if step.kind is SagaStepKind.PIVOT:
                committed = True
                self._emit("saga_committed", definition, step)

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
        completed: list[tuple[SagaStep[Ctx], Ctx]],
    ) -> list[BaseException]:
        errors: list[BaseException] = []

        for step, state_at_completion in reversed(completed):
            compensation = step.compensation

            if compensation is None:
                continue

            try:
                await self._run_compensation(
                    ctx, step, compensation, state_at_completion
                )
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

    def _forward_incomplete[Ctx](
        self,
        definition: SagaDefinition[Ctx],
        step: SagaStep[Ctx],
        error: BaseException,
    ) -> CoreException:
        return exc.infrastructure(
            f"Saga {definition.name!r} committed at its pivot but could not complete "
            f"forward at step {step.name!r}; it must be completed, not compensated.",
            code="saga.forward_incomplete",
        ).enrich(
            cause={"error": str(error)},
            saga=str(definition.name),
            step=str(step.name),
        )

    # ....................... #

    def _saga_error[Ctx](
        self,
        definition: SagaDefinition[Ctx],
        step: SagaStep[Ctx],
        error: BaseException,
        comp_errors: list[BaseException],
    ) -> CoreException:
        if comp_errors:
            return exc.infrastructure(
                f"Saga {definition.name!r} failed at step {step.name!r} and "
                f"compensation failed for {len(comp_errors)} step(s); manual "
                "intervention required.",
                code="saga.compensation_failed",
            ).enrich(
                cause={
                    "error": str(error),
                    "compensation_errors": [str(e) for e in comp_errors],
                },
                saga=str(definition.name),
                step=str(step.name),
            )

        return exc.domain(
            f"Saga {definition.name!r} failed at step {step.name!r}; completed steps "
            "were compensated.",
            code="saga.step_failed",
        ).enrich(
            cause={"error": str(error)},
            saga=str(definition.name),
            step=str(step.name),
        )

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
