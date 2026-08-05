"""Cancelling a durable saga: the pivot decides, not the operator.

A saga is the one durable body where "stop" cannot mean the same thing everywhere, because a
saga has a point of no return:

- **before the pivot** stopping is safe *because* compensation exists — that is the entire
  bargain a saga makes — so the ask behaves as a step failure at the current position:
  completed steps roll back, journaled, and the run lands CANCELLED rather than FAILED
  (nothing misbehaved; somebody asked);
- **at or after the pivot** forward steps must complete. Honouring a cancel there would
  manufacture a FORWARD_INCOMPLETE by operator request — the state that means "committed and
  stuck, a human must finish it by hand" — which is strictly worse than the disease. The ask
  is recorded, refused, and the saga finishes on its own merits.

The refusal is deliberately visible (`cancel_refused_at`): an operator who pressed Stop and
watched the run complete anyway is owed the reason.

# covers: DurableSagaExecutor.run
# covers: DurableRunStorePort.refuse_cancel
"""

from __future__ import annotations

import asyncio
from datetime import timedelta

import pytest
from pydantic import BaseModel

from forze.application.contracts.durable.function import DurableRunStatus
from forze.application.contracts.saga import SagaDefinition, SagaStep, SagaStepKind
from forze.application.execution import ExecutionContext
from forze.base.exceptions import CoreException, ExceptionKind
from forze.testing import context_from_modules
from forze_kits.integrations.durable import (
    DurableFunctionRegistry,
    DurableFunctionRunner,
    durable_saga_handler,
    resolve_durable_run_admin,
    resolve_durable_run_store,
)
from forze_mock import MockDepsModule, MockState

# ----------------------- #

_FAST_LEASE = timedelta(milliseconds=60)
"""Short enough that the heartbeat (``lease_for / 2``) observes an ask in ~30 ms."""


class OrderCtx(BaseModel):
    trail: list[str] = []


# ....................... #


def _step(
    name: str,
    effects: list[str],
    *,
    kind: SagaStepKind = SagaStepKind.COMPENSATABLE,
    stop: bool = False,
    on_stop: object = None,
) -> SagaStep[OrderCtx]:
    """A journaled saga step; with *stop* it asks the run to cancel and then blocks.

    Asking from inside the step is what makes this deterministic: the request lands while
    this exact step is mid-action, so the test controls *where* in the saga the cancellation
    is observed rather than racing the heartbeat.
    """

    async def action(_ctx: ExecutionContext, state: OrderCtx) -> OrderCtx:
        effects.append(f"do:{name}")

        if stop:
            await on_stop()  # type: ignore[operator]
            await asyncio.sleep(1.0)  # the heartbeat tears us down here

        return OrderCtx(trail=[*state.trail, name])

    async def compensation(_ctx: ExecutionContext, _state: OrderCtx) -> None:
        effects.append(f"undo:{name}")

    return SagaStep(
        name=name,
        action=action,
        compensation=compensation,
        kind=kind,
        tx_route="mock",
        # A post-pivot step must already declare this — the contract refuses to build the
        # saga otherwise. That existing requirement is what makes the refusal path legal:
        # re-running the interrupted step is safe by construction, not by hope.
        idempotent=kind is SagaStepKind.RETRYABLE,
    )


# ....................... #


async def _drive(
    ctx: ExecutionContext,
    saga: SagaDefinition[OrderCtx],
    run_id_holder: dict[str, str],
) -> tuple[DurableFunctionRunner, str]:
    """Register *saga* as a durable function and enqueue one run of it."""

    registry = DurableFunctionRegistry()
    registry.register(str(saga.name), durable_saga_handler(saga, OrderCtx))

    runner = DurableFunctionRunner(
        registry=registry,
        lease_for=_FAST_LEASE,
        heartbeat_divisor=2,
    )

    store = resolve_durable_run_store(ctx)
    record = await store.enqueue(
        str(saga.name),
        input_json=OrderCtx().model_dump(mode="json"),
        idempotency_key="k",
    )
    run_id_holder["run_id"] = record.run_id

    return runner, record.run_id


# ....................... #


class TestCancelBeforeThePivot:
    async def test_completed_steps_compensate_and_the_run_lands_cancelled(self) -> None:
        state = MockState()
        ctx = context_from_modules(MockDepsModule(state=state))
        admin = resolve_durable_run_admin(ctx)
        store = resolve_durable_run_store(ctx)

        effects: list[str] = []
        holder: dict[str, str] = {}

        async def ask_to_stop() -> None:
            assert await admin.request_cancel(holder["run_id"]) is True

        saga: SagaDefinition[OrderCtx] = SagaDefinition(
            name="order",
            steps=(
                _step("reserve", effects),
                _step("charge", effects, stop=True, on_stop=ask_to_stop),
                _step("ship", effects, kind=SagaStepKind.PIVOT),
            ),
        )

        runner, run_id = await _drive(ctx, saga, holder)
        result = await runner.run_now(ctx, "order", idempotency_key="k")

        # ``reserve`` completed and is rolled back; ``charge`` never completed, so it has
        # nothing to undo; ``ship`` — the pivot — was never reached.
        assert effects == ["do:reserve", "do:charge", "undo:reserve"]

        assert result.status is DurableRunStatus.CANCELLED
        assert result.cancel_requested_at is not None
        assert result.cancel_refused_at is None  # nothing to refuse before the pivot

        # CANCELLED, not FAILED: the distinction is the whole point — a rolled-back saga
        # that an operator asked for should not read as a defect on a dashboard.
        assert result.status is not DurableRunStatus.FAILED

        # The compensation is journaled like any step, so a crash mid-rollback resumes it
        # rather than re-running an undo.
        assert f"{run_id}:compensate:reserve" in state.durable_step_memo


class TestCancelAfterThePivot:
    async def test_the_ask_is_refused_and_the_saga_completes_forward(self) -> None:
        ctx = context_from_modules(MockDepsModule())
        admin = resolve_durable_run_admin(ctx)

        effects: list[str] = []
        holder: dict[str, str] = {}

        async def ask_to_stop() -> None:
            # Recorded, and the caller is told so — the refusal happens downstream, at
            # observation time, not here.
            assert await admin.request_cancel(holder["run_id"]) is True

        saga: SagaDefinition[OrderCtx] = SagaDefinition(
            name="order",
            steps=(
                _step("reserve", effects),
                _step("charge", effects, kind=SagaStepKind.PIVOT),
                _step(
                    "ship",
                    effects,
                    kind=SagaStepKind.RETRYABLE,
                    stop=True,
                    on_stop=ask_to_stop,
                ),
            ),
        )

        runner, _ = await _drive(ctx, saga, holder)
        result = await runner.run_now(ctx, "order", idempotency_key="k")

        # ``ship`` was interrupted pre-commit, so it journaled nothing and simply re-ran —
        # the same replay a crash would have caused. Nothing was compensated: past the pivot
        # there is no rollback to run.
        assert effects == ["do:reserve", "do:charge", "do:ship", "do:ship"]
        assert "undo:reserve" not in effects

        assert result.status is DurableRunStatus.COMPLETED
        assert result.output_json == {"trail": ["reserve", "charge", "ship"]}

        # Both stamps: asked, and declined. A run carrying only the first would leave an
        # operator unable to tell a refusal from a request that was silently lost.
        assert result.cancel_requested_at is not None
        assert result.cancel_refused_at is not None

    async def test_a_refusal_spends_the_ask_so_a_later_stop_is_not_swallowed(self) -> None:
        # The hazard in absorbing a cancellation: if the ask stayed "live" after being
        # refused, the body would keep eating every subsequent cancel — a drain or a
        # deadline would be swallowed by a stale reading of an operator's old request. Here
        # the deadline fires after the refusal and must still stop the run.
        ctx = context_from_modules(MockDepsModule())
        admin = resolve_durable_run_admin(ctx)

        effects: list[str] = []
        holder: dict[str, str] = {}
        asked = {"done": False}

        async def ask_once() -> None:
            if not asked["done"]:
                asked["done"] = True
                await admin.request_cancel(holder["run_id"])

        async def hang(_ctx: ExecutionContext, state: OrderCtx) -> OrderCtx:
            effects.append("do:ship")
            await ask_once()
            await asyncio.sleep(5.0)  # refused once, then held past the cap

            return state  # pragma: no cover — the deadline stops us first

        saga: SagaDefinition[OrderCtx] = SagaDefinition(
            name="order",
            steps=(
                _step("charge", effects, kind=SagaStepKind.PIVOT),
                SagaStep(
                    name="ship",
                    action=hang,
                    compensation=None,
                    kind=SagaStepKind.RETRYABLE,
                    tx_route="mock",
                    idempotent=True,
                ),
            ),
        )

        registry = DurableFunctionRegistry()
        registry.register("order", durable_saga_handler(saga, OrderCtx))
        runner = DurableFunctionRunner(
            registry=registry,
            lease_for=_FAST_LEASE,
            heartbeat_divisor=2,
            max_run_duration=timedelta(milliseconds=250),
        )

        store = resolve_durable_run_store(ctx)
        record = await store.enqueue(
            "order",
            input_json=OrderCtx().model_dump(mode="json"),
            idempotency_key="k",
        )
        holder["run_id"] = record.run_id

        # A deadline is a genuine error, so it still surfaces to the caller — unlike a
        # cancel, which returns the record. That difference is the tell that the refused
        # ask was not silently reused to classify this stop.
        with pytest.raises(CoreException, match="max_run_duration") as caught:
            await runner.run_now(ctx, "order", idempotency_key="k")

        assert caught.value.kind is ExceptionKind.TIMEOUT

        result = await store.load(record.run_id)
        assert result is not None
        assert result.status is DurableRunStatus.TIMED_OUT
        assert "max_run_duration" in (result.error or "")

        # And the refusal is still on the record as the fact it is.
        assert result.cancel_requested_at is not None
        assert result.cancel_refused_at is not None
