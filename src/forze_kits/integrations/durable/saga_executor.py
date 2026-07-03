"""Durable saga executor: crash-resumable sagas over the durable step journal.

Implements ``SagaExecutorPort`` by driving the shared ``SagaProgress`` coordinator through
a ``DurableFunctionStepPort`` — each step's action (and each compensation) is journaled, so
a process crash mid-saga resumes at the first un-journaled step / compensation instead of
leaving committed steps un-compensated. It depends only on the contract, so the same code
runs over the mock (tests), Postgres (self-hosted), or any step-port backend.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Awaitable, Callable, cast, final

import attrs
from pydantic import BaseModel

from forze.application.contracts.durable.function import require_durable_run
from forze.application.contracts.saga import (
    SagaDefinition,
    SagaProgress,
    SagaStep,
)
from forze.base.exceptions import exc

from ._resolve import resolve_durable_step
from .registry import DurableFunctionHandler

if TYPE_CHECKING:
    from forze.application.contracts.durable.function import DurableFunctionStepPort
    from forze.application.execution.context import ExecutionContext
    from forze.base.primitives import JsonDict

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True)
class DurableSagaExecutor:
    """A ``SagaExecutorPort`` that journals each saga step for crash-resumable orchestration.

    Must run inside a durable run (bound by the durable-function runner), so its steps
    memoize and the run is recoverable; invoked outside one it fails closed. The saga
    context must be a serializable ``pydantic.BaseModel`` — it is encoded before each step
    is journaled and decoded after, so a completed step replays its context on recovery.

    Wire it in via ``SagaDepsModule(executor=DurableSagaExecutor())`` and drive the saga as
    a durable function (see :func:`durable_saga_handler`).
    """

    async def run[Ctx](
        self,
        ctx: ExecutionContext,
        definition: SagaDefinition[Ctx],
        initial: Ctx,
    ) -> Ctx:
        require_durable_run()

        if not isinstance(initial, BaseModel):
            raise exc.configuration(
                "DurableSagaExecutor requires a serializable saga context "
                "(a pydantic BaseModel) so each step can be journaled; "
                f"saga {definition.name!r} was started with {type(initial).__name__}.",
            )

        if ctx.tx_ctx.depth() != 0:
            raise exc.configuration(
                "A saga must run outside a transaction so each step commits "
                f"independently; saga {definition.name!r} was started inside an open "
                "transaction.",
            )

        ctx_model = type(initial)
        step_port = resolve_durable_step(ctx)
        progress = SagaProgress(saga_name=str(definition.name))

        for step in definition.steps:
            progress.register(str(step.name), step.kind)

        state: Ctx = initial
        states: dict[int, Ctx] = {}  # context as of each completed step, by index

        for index, step in enumerate(definition.steps):
            try:
                state = cast(
                    "Ctx",
                    await self._run_step(ctx, step, state, step_port, ctx_model),
                )

            except Exception as error:
                if progress.committed:
                    # Past the pivot: complete forward (manually), never compensate.
                    raise progress.forward_incomplete_error(index, error) from error

                comp_errors = await self._compensate(
                    ctx, definition, progress, states, step_port
                )
                raise progress.step_failed_error(index, error, comp_errors) from error

            progress.record_success(index)
            states[index] = state

        return state

    # ....................... #

    async def _run_step[Ctx](
        self,
        ctx: ExecutionContext,
        step: SagaStep[Ctx],
        state: Ctx,
        step_port: DurableFunctionStepPort,
        ctx_model: type[BaseModel],
    ) -> BaseModel:
        async def _act() -> JsonDict:
            if step.tx_route is not None:
                async with ctx.tx_ctx.scope(step.tx_route):
                    new_state = await step.action(ctx, state)
            else:
                new_state = await step.action(ctx, state)

            return cast("BaseModel", new_state).model_dump(mode="json")

        async def _journaled() -> JsonDict:
            if step.retry_policy is not None:
                return await ctx.resilience().run(_act, policy=step.retry_policy)

            return await _act()

        # Journaled: a completed step returns its recorded context on replay and its action
        # is not re-run; a retry_policy retries transient failures before it journals.
        encoded = await step_port.run(str(step.name), _journaled)

        return ctx_model.model_validate(encoded)

    # ....................... #

    async def _compensate[Ctx](
        self,
        ctx: ExecutionContext,
        definition: SagaDefinition[Ctx],
        progress: SagaProgress,
        states: dict[int, Ctx],
        step_port: DurableFunctionStepPort,
    ) -> list[BaseException]:
        errors: list[BaseException] = []

        for index in progress.steps_to_compensate():
            step = definition.steps[index]
            compensation = step.compensation

            if compensation is None:
                continue

            try:
                await self._run_compensation(
                    ctx, step, compensation, states[index], step_port
                )

            except Exception as comp_error:  # noqa: BLE001 — best-effort; collect all
                errors.append(comp_error)

        return errors

    # ....................... #

    async def _run_compensation[Ctx](
        self,
        ctx: ExecutionContext,
        step: SagaStep[Ctx],
        compensation: Callable[[ExecutionContext, Ctx], Awaitable[None]],
        state: Ctx,
        step_port: DurableFunctionStepPort,
    ) -> None:
        async def _comp() -> JsonDict:
            if step.tx_route is not None:
                async with ctx.tx_ctx.scope(step.tx_route):
                    await compensation(ctx, state)
            else:
                await compensation(ctx, state)

            return {}  # compensation carries no result; the row marks it done for replay

        async def _journaled() -> JsonDict:
            if step.compensation_policy is not None:
                return await ctx.resilience().run(
                    _comp, policy=step.compensation_policy
                )

            return await _comp()

        # A distinct step id per compensation, so a crash during rollback resumes it in
        # reverse and skips already-compensated steps.
        await step_port.run(f"compensate:{step.name}", _journaled)


# ....................... #


def durable_saga_handler[Ctx: BaseModel](
    definition: SagaDefinition[Ctx],
    ctx_model: type[Ctx],
    *,
    executor: DurableSagaExecutor | None = None,
) -> DurableFunctionHandler:
    """Adapt a saga into a durable-function body for the registry (so it is recoverable).

    Register the result under the saga's name and enqueue it with the encoded initial
    context::

        registry.register(str(saga.name), durable_saga_handler(saga, OrderCtx))
        await runner.run_now(ctx, str(saga.name), initial.model_dump(mode="json"))
    """

    saga_executor = executor or DurableSagaExecutor()

    async def handler(
        ctx: ExecutionContext,
        input_json: JsonDict | None,
    ) -> JsonDict:
        if input_json is None:
            raise exc.validation(
                f"Durable saga {definition.name!r} requires an initial context input.",
            )

        initial = ctx_model.model_validate(input_json)
        result = await saga_executor.run(ctx, definition, initial)

        return result.model_dump(mode="json")

    return handler
