"""Durable saga executor: crash-resumable sagas over the durable step journal.

Implements ``SagaExecutorPort`` by driving the shared ``SagaProgress`` coordinator through
a ``DurableFunctionStepPort`` — each step's action (and each compensation) is journaled, so
a process crash mid-saga resumes at the first un-journaled step / compensation instead of
leaving committed steps un-compensated. A step that *fails* journals its failure too, so a
re-invocation of the same run re-raises it rather than re-running the action (a failed step's
body is not re-run on replay, like a completed one's — keep step bodies idempotent, as a body
can still run more than once if a worker is reclaimed mid-body). A failure classified as
retryable (infrastructure, throttled, concurrency) is retried in place with backoff before
it is journaled — compensation is irreversible business action, so it is reserved for
genuine failures and exhausted retries, never a one-off blip. It depends only on the
contract, so the same code runs over the mock (tests), Postgres (self-hosted), or any backend.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, cast, final

import attrs
from pydantic import BaseModel

from forze.application.contracts.durable.function import require_durable_run
from forze.application.contracts.saga import (
    SagaDefinition,
    SagaProgress,
    SagaStep,
)
from forze.base.exceptions import CoreException, exc, exception_egress_policy

from .._logger import logger
from ._resolve import resolve_durable_step
from .registry import DurableFunctionHandler

if TYPE_CHECKING:
    from forze.application.contracts.durable.function import DurableFunctionStepPort
    from forze.application.execution.context import ExecutionContext
    from forze.base.primitives import JsonDict

# ----------------------- #

_STEP_FAILURE_KEY = "__saga_step_failed__"
"""Journal sentinel. A step whose action raised records its failure message under this key
(instead of a state dict), so a replay re-raises the failure rather than re-running the
action — the failed body is not re-run on replay, like a completed step's."""


@final
class _JournaledStepFailure(Exception):
    """A step-action failure re-raised from the journal on replay (never re-runs the action).

    Carries the recorded message so the saga coordinator wraps it exactly as it wrapped the
    original failure; it is always caught inside :meth:`DurableSagaExecutor.run`.
    """


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class DurableSagaExecutor:
    """A ``SagaExecutorPort`` that journals each saga step for crash-resumable orchestration.

    Must run inside a durable run (bound by the durable-function runner), so its steps
    memoize and the run is recoverable; invoked outside one it fails closed. The saga
    context must be a serializable ``pydantic.BaseModel`` — it is encoded before each step
    is journaled and decoded after, so a completed step replays its context on recovery.

    A step action (or compensation) that fails with a *retryable* kind — infrastructure,
    throttled, concurrency — is retried in place with exponential backoff before its failure
    is journaled, so a one-off blip never triggers the compensation chain (an irreversible
    business action). The failed attempt's transaction rolled back, so a retry re-runs the
    action fresh — the same at-least-once caveat the journal already carries for a body
    interrupted before its result is recorded. A step with an explicit ``retry_policy``
    (or ``compensation_policy``) uses that named policy instead of these defaults.

    Wire it in via ``SagaDepsModule(executor=DurableSagaExecutor())`` and drive the saga as
    a durable function (see :func:`durable_saga_handler`).
    """

    retry_attempts: int = 2
    """Bounded in-place retries after a retryable-classified failure (``0`` disables them);
    on exhaustion the failure is journaled and the saga compensates."""

    retry_base_delay: float = 0.05
    """Initial backoff delay in seconds between in-place retries, doubled per retry."""

    def __attrs_post_init__(self) -> None:
        if self.retry_attempts < 0:
            raise exc.configuration(
                "Saga retry attempts must be non-negative (0 disables in-place retries)"
            )

        if self.retry_base_delay < 0:
            raise exc.configuration(
                "Saga retry base delay must be non-negative (0 retries immediately)"
            )

    # ....................... #

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

                comp_errors = await self._compensate(ctx, definition, progress, states, step_port)
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
            try:
                if step.retry_policy is not None:
                    return await ctx.resilience().run(_act, policy=step.retry_policy)

                return await self._retry_transient(str(step.name), _act)

            except Exception as error:
                # Record the failure as this step's journaled outcome so a re-invocation of
                # the same durable run (crash recovery / replay) re-raises it instead of
                # re-running the action's body. Only genuine failures reach here: a
                # retryable-classified one was already retried in place (by the step's own
                # policy or the executor's bounded default) and exhausted its attempts.
                return {_STEP_FAILURE_KEY: str(error)}

        # Journaled: a completed step returns its recorded context on replay and its action
        # is not re-run; a failed step records its failure and re-raises it on replay (never
        # re-running the action). Transient failures are retried before anything journals.
        encoded = await step_port.run(str(step.name), _journaled)

        if _STEP_FAILURE_KEY in encoded:
            raise _JournaledStepFailure(str(encoded[_STEP_FAILURE_KEY]))

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
                await self._run_compensation(ctx, step, compensation, states[index], step_port)

            except Exception as comp_error:
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
                return await ctx.resilience().run(_comp, policy=step.compensation_policy)

            # A compensation must eventually succeed, so a transient failure is retried in
            # place too — otherwise a blip would mark the rollback "compensation failed"
            # (manual intervention) when a retry would have kept it clean.
            return await self._retry_transient(f"compensate:{step.name}", _comp)

        # A distinct step id per compensation, so a crash during rollback resumes it in
        # reverse and skips already-compensated steps.
        await step_port.run(f"compensate:{step.name}", _journaled)

    # ....................... #

    async def _retry_transient(
        self,
        step_id: str,
        fn: Callable[[], Awaitable[JsonDict]],
    ) -> JsonDict:
        # Bounded in-place retry on retryable-classified kinds only (infrastructure,
        # throttled, concurrency); anything else — and the last exhausted attempt —
        # propagates to be journaled as the step's outcome.
        for attempt in range(self.retry_attempts):
            try:
                return await fn()

            except CoreException as error:
                if not exception_egress_policy(error.kind).retryable:
                    raise

                logger.warning(
                    "Durable saga step %s failed with a transient %s error; "
                    "retrying in place (%d/%d)",
                    step_id,
                    error.kind.value,
                    attempt + 1,
                    self.retry_attempts,
                    exc_info=True,
                )
                await asyncio.sleep(self.retry_base_delay * (2**attempt))

        return await fn()


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
