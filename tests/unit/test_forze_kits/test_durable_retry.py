"""Run-level retry: a run whose body fails with a retryable error goes back to ``PENDING``.

The run keeps its ``run_id``, so the next claim replays what it journaled; its ``attempts``
are the budget, and the backoff is its ``available_at``. Only a ``CoreException`` of a kind
the strategy names is retried, never once the budget is spent, and never while an
operator's cancel is pending.

# covers: DurableFunctionRunner.run_now
# covers: DurableFunctionRunner.recover
# covers: DurableFunctionRegistry.register
# covers: DurableRunStorePort.mark_retry
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Any

import pytest
from pydantic import BaseModel

from forze.application.contracts.durable.function import DurableRunStatus, current_durable_run
from forze.application.contracts.resilience import BackoffStrategy, RetryBudget, RetryStrategy
from forze.application.contracts.saga import (
    SAGA_STEP_AMBIGUOUS_CODE,
    SagaDefinition,
    SagaStep,
    SagaStepKind,
)
from forze.application.execution import ExecutionContext
from forze.base.exceptions import CoreException, ExceptionKind, exc
from forze.base.primitives import utcnow
from forze_kits.integrations.durable import (
    DurableFunctionRegistry,
    DurableFunctionRunner,
    durable_saga_handler,
    resolve_durable_run_admin,
    resolve_durable_run_store,
    resolve_durable_step,
)
from forze_mock import MockDepsModule, MockDurableRunStore, MockState
from tests.support.execution_context import context_from_modules

# ----------------------- #


class _Order(BaseModel):
    trail: list[str] = []


def _strategy(
    max_attempts: int = 3,
    *,
    base: timedelta = timedelta(milliseconds=1),
    retry_on: frozenset[ExceptionKind] = frozenset({ExceptionKind.INFRASTRUCTURE}),
) -> RetryStrategy:
    return RetryStrategy(
        max_attempts=max_attempts,
        backoff=BackoffStrategy(base=base, max=base * 1000, jitter="none"),
        retry_on=retry_on,
    )


async def _drain(runner: DurableFunctionRunner, ctx: ExecutionContext, run_id: str) -> Any:
    """Recover until the run lands in a terminal state, as a recovery loop would."""

    store = resolve_durable_run_store(ctx)

    for _ in range(20):
        record = await store.load(run_id)
        assert record is not None

        if record.status is not DurableRunStatus.PENDING:
            return record

        await asyncio.sleep(0.01)
        await runner.recover(ctx)

    raise AssertionError("the run never landed")  # pragma: no cover


# ....................... #


class TestRunRetry:
    async def test_a_run_failing_twice_then_succeeding_completes_on_its_own(self) -> None:
        ctx = context_from_modules(MockDepsModule())
        calls = {"body": 0, "step": 0}

        async def flaky(ctx: ExecutionContext, input_json: dict | None) -> dict:
            calls["body"] += 1

            async def work() -> dict:
                calls["step"] += 1
                return {"v": 1}

            await resolve_durable_step(ctx).run("s1", work)

            if calls["body"] < 3:
                raise exc.infrastructure("upstream blip")

            return {"ok": True}

        registry = DurableFunctionRegistry()
        registry.register("flaky", flaky)
        runner = DurableFunctionRunner(registry=registry, retry=_strategy(3))

        first = await runner.run_now(ctx, "flaky", idempotency_key="k")

        # Not raised at the caller: the run is waiting for its retry, with the failure kept.
        assert first.status is DurableRunStatus.PENDING
        assert first.error is not None and first.error.endswith("upstream blip")
        assert first.available_at is not None

        final = await _drain(runner, ctx, first.run_id)

        assert final.status is DurableRunStatus.COMPLETED
        assert final.output_json == {"ok": True}
        assert final.attempts == 3
        assert calls == {"body": 3, "step": 1}  # the journaled step replayed

    async def test_the_last_attempt_lands_failed(self) -> None:
        ctx = context_from_modules(MockDepsModule())

        async def broken(ctx: ExecutionContext, input_json: dict | None) -> dict:
            raise exc.infrastructure("down")

        registry = DurableFunctionRegistry()
        registry.register("broken", broken)
        runner = DurableFunctionRunner(registry=registry, retry=_strategy(2))

        first = await runner.run_now(ctx, "broken")
        final = await _drain(runner, ctx, first.run_id)

        assert final.status is DurableRunStatus.FAILED
        assert final.error is not None and final.error.endswith("down")
        assert final.attempts == 2

    @pytest.mark.parametrize(
        "error",
        [
            exc.validation("bad input"),
            exc.throttled("slow down"),
            ValueError("not a core exception"),
        ],
        ids=["not-retryable", "not-in-retry-on", "not-core"],
    )
    async def test_an_error_the_strategy_does_not_name_lands_failed_at_once(
        self, error: Exception
    ) -> None:
        ctx = context_from_modules(MockDepsModule())

        async def fails(ctx: ExecutionContext, input_json: dict | None) -> dict:
            raise error

        registry = DurableFunctionRegistry()
        registry.register("fails", fails)
        runner = DurableFunctionRunner(registry=registry, retry=_strategy(3))
        store = resolve_durable_run_store(ctx)
        record = await store.enqueue("fails", input_json=None, idempotency_key="k")

        with pytest.raises(type(error)):
            await runner.run_now(ctx, "fails", idempotency_key="k")

        landed = await store.load(record.run_id)
        assert landed is not None
        assert (landed.status, landed.attempts) == (DurableRunStatus.FAILED, 1)

    async def test_without_a_strategy_a_retryable_failure_lands_failed(self) -> None:
        ctx = context_from_modules(MockDepsModule())

        async def broken(ctx: ExecutionContext, input_json: dict | None) -> dict:
            raise exc.infrastructure("down")

        registry = DurableFunctionRegistry()
        registry.register("broken", broken)

        with pytest.raises(CoreException, match="down"):
            await DurableFunctionRunner(registry=registry).run_now(ctx, "broken")

    async def test_the_backoff_holds_the_run_out_of_the_scan(self) -> None:
        ctx = context_from_modules(MockDepsModule())

        async def broken(ctx: ExecutionContext, input_json: dict | None) -> dict:
            raise exc.infrastructure("down")

        registry = DurableFunctionRegistry()
        registry.register("broken", broken)
        runner = DurableFunctionRunner(
            registry=registry, retry=_strategy(3, base=timedelta(minutes=10))
        )

        before = utcnow()
        first = await runner.run_now(ctx, "broken")

        assert first.available_at is not None
        assert timedelta(minutes=10) <= first.available_at - before < timedelta(minutes=11)
        assert await runner.recover(ctx) == 0

    async def test_a_throttled_run_is_due_no_sooner_than_its_retry_after(self) -> None:
        ctx = context_from_modules(MockDepsModule())

        async def throttled(ctx: ExecutionContext, input_json: dict | None) -> dict:
            raise exc.throttled("slow down", details={"retry_after": 600})

        registry = DurableFunctionRegistry()
        registry.register("throttled", throttled)
        runner = DurableFunctionRunner(
            registry=registry,
            retry=_strategy(3, retry_on=frozenset({ExceptionKind.THROTTLED})),
        )

        before = utcnow()
        first = await runner.run_now(ctx, "throttled")

        # The backoff alone (1 ms) would be due at once; the upstream asked for 10 minutes.
        assert first.available_at is not None
        assert first.available_at - before >= timedelta(minutes=10)
        assert await runner.recover(ctx) == 0

    # Infinite; finite but past what a timedelta holds; within a timedelta, past year 9999.
    @pytest.mark.parametrize("hint", [10**400, 1e15, 3e11], ids=["inf", "timedelta", "year"])
    async def test_a_retry_after_no_datetime_can_hold_fails_the_run(self, hint: float) -> None:
        ctx = context_from_modules(MockDepsModule())

        async def throttled(ctx: ExecutionContext, input_json: dict | None) -> dict:
            raise exc.throttled("slow down", details={"retry_after": hint})

        registry = DurableFunctionRegistry()
        registry.register("throttled", throttled)
        runner = DurableFunctionRunner(
            registry=registry,
            retry=_strategy(3, retry_on=frozenset({ExceptionKind.THROTTLED})),
        )

        store = resolve_durable_run_store(ctx)
        record = await store.enqueue("throttled", input_json=None, idempotency_key="k")

        # The throttle itself, not an OverflowError from scheduling the retry.
        with pytest.raises(CoreException) as ei:
            await runner.run_now(ctx, "throttled", idempotency_key="k")

        assert ei.value.kind is ExceptionKind.THROTTLED
        landed = await store.load(record.run_id)
        assert landed is not None
        assert landed.status is DurableRunStatus.FAILED

    async def test_a_function_registered_with_a_strategy_uses_it(self) -> None:
        ctx = context_from_modules(MockDepsModule())

        async def broken(ctx: ExecutionContext, input_json: dict | None) -> dict:
            raise exc.infrastructure("down")

        registry = DurableFunctionRegistry()
        registry.register("retried", broken, retry=_strategy(2))
        registry.register("opted-out", broken, retry=_strategy(1))
        runner = DurableFunctionRunner(registry=registry, retry=_strategy(5))

        retried = await runner.run_now(ctx, "retried")
        assert retried.status is DurableRunStatus.PENDING
        assert (await _drain(runner, ctx, retried.run_id)).attempts == 2

        with pytest.raises(CoreException, match="down"):
            await runner.run_now(ctx, "opted-out")

    async def test_a_pending_cancel_outranks_the_retry(self) -> None:
        ctx = context_from_modules(MockDepsModule())
        admin = resolve_durable_run_admin(ctx)

        async def converts(ctx: ExecutionContext, input_json: dict | None) -> dict:
            run = current_durable_run()
            assert run is not None
            await admin.request_cancel(run.run_id)

            try:
                await asyncio.sleep(5)

            except asyncio.CancelledError:
                # A body that turns the cancel into a retryable error of its own.
                raise exc.infrastructure("rollback interrupted") from None

            return {}  # pragma: no cover — the cancel stops us first

        registry = DurableFunctionRegistry()
        registry.register("converts", converts)
        runner = DurableFunctionRunner(
            registry=registry,
            retry=_strategy(3),
            lease_for=timedelta(milliseconds=60),
        )

        with pytest.raises(CoreException, match="rollback interrupted"):
            await runner.run_now(ctx, "converts", idempotency_key="k")

        landed = await resolve_durable_run_store(ctx).enqueue(
            "converts", input_json=None, idempotency_key="k"
        )
        assert landed.status is DurableRunStatus.FAILED

    async def test_a_cancel_recorded_since_the_last_heartbeat_outranks_the_retry(self) -> None:
        # The default lease beats only every 100 s, so the ask is on the row alone.
        ctx = context_from_modules(MockDepsModule())
        admin = resolve_durable_run_admin(ctx)

        async def asks_then_fails(ctx: ExecutionContext, input_json: dict | None) -> dict:
            run = current_durable_run()
            assert run is not None
            await admin.request_cancel(run.run_id)

            raise exc.infrastructure("down")

        registry = DurableFunctionRegistry()
        registry.register("fn", asks_then_fails)
        runner = DurableFunctionRunner(registry=registry, retry=_strategy(3))
        store = resolve_durable_run_store(ctx)
        record = await store.enqueue("fn", input_json=None, idempotency_key="k")

        with pytest.raises(CoreException, match="down"):
            await runner.run_now(ctx, "fn", idempotency_key="k")

        landed = await store.load(record.run_id)
        assert landed is not None
        assert landed.status is DurableRunStatus.FAILED
        assert landed.cancel_requested_at is not None

    async def test_a_cancel_asked_during_the_wait_lands_when_the_retry_is_due(self) -> None:
        ctx = context_from_modules(MockDepsModule())
        calls = {"body": 0}

        async def broken(ctx: ExecutionContext, input_json: dict | None) -> dict:
            calls["body"] += 1
            raise exc.infrastructure("down")

        registry = DurableFunctionRegistry()
        registry.register("fn", broken)
        runner = DurableFunctionRunner(registry=registry, retry=_strategy(3))
        store = resolve_durable_run_store(ctx)

        waiting = await runner.run_now(ctx, "fn")

        assert await resolve_durable_run_admin(ctx).request_cancel(waiting.run_id) is True
        stamped = await store.load(waiting.run_id)
        assert stamped is not None and stamped.status is DurableRunStatus.PENDING

        final = await _drain(runner, ctx, waiting.run_id)

        assert (final.status, final.attempts) == (DurableRunStatus.CANCELLED, 2)
        assert calls["body"] == 1  # the due claim landed the ask without running the body

    @pytest.mark.parametrize(
        ("code", "status"),
        [
            (SAGA_STEP_AMBIGUOUS_CODE, DurableRunStatus.FAILED),
            ("saga.compensation_failed", DurableRunStatus.FAILED),
            ("saga.forward_incomplete", DurableRunStatus.FORWARD_INCOMPLETE),
        ],
        ids=["step-ambiguous", "compensation-failed", "forward-incomplete"],
    )
    async def test_a_saga_left_for_an_operator_is_not_retried(
        self, code: str, status: DurableRunStatus
    ) -> None:
        ctx = context_from_modules(MockDepsModule())

        async def needs_operator(ctx: ExecutionContext, input_json: dict | None) -> dict:
            raise exc.infrastructure("reconcile by hand", code=code)

        registry = DurableFunctionRegistry()
        registry.register("saga", needs_operator)
        runner = DurableFunctionRunner(registry=registry, retry=_strategy(3))
        store = resolve_durable_run_store(ctx)
        record = await store.enqueue("saga", input_json=None, idempotency_key="k")

        with pytest.raises(CoreException, match="reconcile by hand"):
            await runner.run_now(ctx, "saga", idempotency_key="k")

        landed = await store.load(record.run_id)
        assert landed is not None
        assert (landed.status, landed.attempts) == (status, 1)

    async def test_a_saga_that_rolled_back_is_not_retried(self) -> None:
        # Its failed step is journaled, so running it again would only fail the same way.
        ctx = context_from_modules(MockDepsModule(state=MockState()))
        calls = {"ship": 0, "refund": 0}

        async def charge(ctx: ExecutionContext, state: _Order) -> _Order:
            return _Order(trail=[*state.trail, "charged"])

        async def refund(ctx: ExecutionContext, state: _Order) -> None:
            calls["refund"] += 1

        async def ship(ctx: ExecutionContext, state: _Order) -> _Order:
            calls["ship"] += 1
            raise exc.validation("address rejected")

        async def confirm(ctx: ExecutionContext, state: _Order) -> _Order:
            return state  # pragma: no cover — the saga fails before it

        saga: SagaDefinition[_Order] = SagaDefinition(
            name="order",
            steps=(
                SagaStep(name="charge", action=charge, compensation=refund, tx_route="mock"),
                SagaStep(name="ship", action=ship, compensation=refund, tx_route="mock"),
                SagaStep(name="confirm", action=confirm, kind=SagaStepKind.PIVOT, tx_route="mock"),
            ),
        )
        registry = DurableFunctionRegistry()
        registry.register("order", durable_saga_handler(saga, _Order))
        runner = DurableFunctionRunner(registry=registry, retry=_strategy(3))
        store = resolve_durable_run_store(ctx)
        record = await store.enqueue(
            "order", input_json=_Order().model_dump(mode="json"), idempotency_key="k"
        )

        with pytest.raises(CoreException) as caught:
            await runner.run_now(
                ctx, "order", _Order().model_dump(mode="json"), idempotency_key="k"
            )

        assert caught.value.code == "saga.step_failed"

        landed = await store.load(record.run_id)
        assert landed is not None
        assert (landed.status, landed.attempts) == (DurableRunStatus.FAILED, 1)
        assert calls == {"ship": 1, "refund": 1}

    async def test_a_crash_reclaim_spends_an_attempt(self) -> None:
        state = MockState()
        ctx = context_from_modules(MockDepsModule(state=state))
        store = resolve_durable_run_store(ctx)

        async def broken(ctx: ExecutionContext, input_json: dict | None) -> dict:
            raise exc.infrastructure("down")

        registry = DurableFunctionRegistry()
        registry.register("fn", broken)
        runner = DurableFunctionRunner(registry=registry, retry=_strategy(2))

        # A worker claimed the run and died holding it.
        record = await store.enqueue("fn", input_json=None)
        assert await store.begin(record.run_id, lease_for=timedelta(minutes=5)) is not None
        state.durable_runs[record.run_id]["leased_until"] = utcnow() - timedelta(hours=1)

        assert await runner.recover(ctx) == 1

        landed = await store.load(record.run_id)
        assert landed is not None
        assert (landed.status, landed.attempts) == (DurableRunStatus.FAILED, 2)

    async def test_the_retry_is_fenced_on_the_claim(self, monkeypatch: pytest.MonkeyPatch) -> None:
        ctx = context_from_modules(MockDepsModule())
        fences: list[int | None] = []
        original = MockDurableRunStore.mark_retry

        async def recording(self: MockDurableRunStore, run_id: str, **kwargs: Any) -> None:
            fences.append(kwargs.get("fence"))
            await original(self, run_id, **kwargs)

        monkeypatch.setattr(MockDurableRunStore, "mark_retry", recording)

        async def broken(ctx: ExecutionContext, input_json: dict | None) -> dict:
            raise exc.infrastructure("down")

        registry = DurableFunctionRegistry()
        registry.register("fn", broken)
        await DurableFunctionRunner(registry=registry, retry=_strategy(3)).run_now(ctx, "fn")

        assert fences == [1]

    async def test_a_resubmit_during_the_backoff_does_not_run_the_body(self) -> None:
        ctx = context_from_modules(MockDepsModule())
        calls = {"body": 0}

        async def broken(ctx: ExecutionContext, input_json: dict | None) -> dict:
            calls["body"] += 1
            raise exc.infrastructure("down")

        registry = DurableFunctionRegistry()
        registry.register("fn", broken)
        runner = DurableFunctionRunner(
            registry=registry, retry=_strategy(3, base=timedelta(minutes=10))
        )

        first = await runner.run_now(ctx, "fn", idempotency_key="k")
        again = await runner.run_now(ctx, "fn", idempotency_key="k")

        assert again.run_id == first.run_id
        assert (again.status, again.attempts) == (DurableRunStatus.PENDING, 1)
        assert calls["body"] == 1

    async def test_a_resubmit_of_a_scheduled_run_still_runs_it_at_once(self) -> None:
        ctx = context_from_modules(MockDepsModule())
        calls = {"body": 0}

        async def body(ctx: ExecutionContext, input_json: dict | None) -> dict:
            calls["body"] += 1
            return {"ok": True}

        registry = DurableFunctionRegistry()
        registry.register("fn", body)
        runner = DurableFunctionRunner(registry=registry, retry=_strategy(3))

        scheduled = await runner.enqueue(
            ctx, "fn", idempotency_key="k", run_at=utcnow() + timedelta(hours=1)
        )
        ran = await runner.run_now(ctx, "fn", idempotency_key="k")

        assert ran.run_id == scheduled.run_id
        assert ran.status is DurableRunStatus.COMPLETED
        assert calls["body"] == 1

    async def test_a_strategy_with_a_budget_is_refused(self) -> None:
        budgeted = RetryStrategy(
            max_attempts=3,
            backoff=BackoffStrategy(base=timedelta(seconds=1), max=timedelta(seconds=10)),
            retry_on=frozenset({ExceptionKind.INFRASTRUCTURE}),
            budget=RetryBudget(ratio=0.1),
        )

        with pytest.raises(CoreException, match="budget") as on_runner:
            DurableFunctionRunner(registry=DurableFunctionRegistry(), retry=budgeted)

        async def body(ctx: ExecutionContext, input_json: dict | None) -> dict:
            return {}  # pragma: no cover

        with pytest.raises(CoreException, match="budget") as on_registry:
            DurableFunctionRegistry().register("fn", body, retry=budgeted)

        assert on_runner.value.kind is on_registry.value.kind is ExceptionKind.CONFIGURATION


# ....................... #


class TestRequeueFailed:
    async def test_a_requeued_failed_run_reruns_only_what_it_did_not_journal(self) -> None:
        ctx = context_from_modules(MockDepsModule())
        calls = {"s1": 0, "s2": 0}
        broken = {"on": True}

        async def body(ctx: ExecutionContext, input_json: dict | None) -> dict:
            step = resolve_durable_step(ctx)

            async def one() -> dict:
                calls["s1"] += 1
                return {"v": 1}

            async def two() -> dict:
                calls["s2"] += 1

                if broken["on"]:
                    raise exc.internal("bug")

                return {"v": 2}

            await step.run("s1", one)
            await step.run("s2", two)

            return {"ok": True}

        registry = DurableFunctionRegistry()
        registry.register("fn", body)
        runner = DurableFunctionRunner(registry=registry)
        store = resolve_durable_run_store(ctx)
        record = await store.enqueue("fn", input_json=None, idempotency_key="k")

        with pytest.raises(CoreException, match="bug"):
            await runner.run_now(ctx, "fn", idempotency_key="k")

        broken["on"] = False  # the bug is fixed

        assert await resolve_durable_run_admin(ctx).requeue_failed(run_ids=[record.run_id]) == 1
        assert await runner.recover(ctx) == 1

        final = await store.load(record.run_id)
        assert final is not None
        assert (final.status, final.attempts) == (DurableRunStatus.COMPLETED, 2)
        assert calls == {"s1": 1, "s2": 2}

    async def test_a_requeued_saga_that_rolled_back_fails_the_same_way(self) -> None:
        # Its failed forward step is journaled: the requeue replays the failure, it does not
        # run the step again. Such work needs a new run.
        ctx = context_from_modules(MockDepsModule(state=MockState()))
        calls = {"ship": 0}

        async def charge(ctx: ExecutionContext, state: _Order) -> _Order:
            return _Order(trail=[*state.trail, "charged"])

        async def refund(ctx: ExecutionContext, state: _Order) -> None:
            return None

        async def ship(ctx: ExecutionContext, state: _Order) -> _Order:
            calls["ship"] += 1
            raise exc.validation("address rejected")

        record, runner = await _saga_run(ctx, charge=charge, refund=refund, ship=ship)

        assert await resolve_durable_run_admin(ctx).requeue_failed(run_ids=[record.run_id]) == 1
        assert await runner.recover(ctx) == 1

        final = await resolve_durable_run_store(ctx).load(record.run_id)
        assert final is not None
        assert (final.status, final.attempts) == (DurableRunStatus.FAILED, 2)
        assert calls["ship"] == 1

    async def test_a_requeued_saga_whose_rollback_failed_reruns_only_that_compensation(
        self,
    ) -> None:
        ctx = context_from_modules(MockDepsModule(state=MockState()))
        calls = {"refund": 0, "ship": 0}
        refund_down = {"on": True}

        async def charge(ctx: ExecutionContext, state: _Order) -> _Order:
            return _Order(trail=[*state.trail, "charged"])

        async def refund(ctx: ExecutionContext, state: _Order) -> None:
            calls["refund"] += 1

            if refund_down["on"]:
                raise exc.infrastructure("refund api down")

        async def ship(ctx: ExecutionContext, state: _Order) -> _Order:
            calls["ship"] += 1
            raise exc.validation("address rejected")

        record, runner = await _saga_run(ctx, charge=charge, refund=refund, ship=ship)
        failed = await resolve_durable_run_store(ctx).load(record.run_id)
        assert failed is not None and failed.status is DurableRunStatus.FAILED
        tried = calls["refund"]  # retried in place by the saga executor, then given up

        refund_down["on"] = False
        assert await resolve_durable_run_admin(ctx).requeue_failed(run_ids=[record.run_id]) == 1
        assert await runner.recover(ctx) == 1

        final = await resolve_durable_run_store(ctx).load(record.run_id)
        assert final is not None
        assert final.status is DurableRunStatus.FAILED  # rolled back cleanly this time
        assert final.error is not None and "compensated" in final.error
        assert calls == {"refund": tried + 1, "ship": 1}


async def _saga_run(
    ctx: ExecutionContext,
    *,
    charge: Any,
    refund: Any,
    ship: Any,
) -> tuple[Any, DurableFunctionRunner]:
    """Run a three-step saga whose second step fails, and return its record and runner."""

    async def confirm(ctx: ExecutionContext, state: _Order) -> _Order:
        return state  # pragma: no cover — the saga fails before it

    async def nothing(ctx: ExecutionContext, state: _Order) -> None:
        return None

    saga: SagaDefinition[_Order] = SagaDefinition(
        name="order",
        steps=(
            SagaStep(name="charge", action=charge, compensation=refund, tx_route="mock"),
            SagaStep(name="ship", action=ship, compensation=nothing, tx_route="mock"),
            SagaStep(name="confirm", action=confirm, kind=SagaStepKind.PIVOT, tx_route="mock"),
        ),
    )
    registry = DurableFunctionRegistry()
    registry.register("order", durable_saga_handler(saga, _Order))
    runner = DurableFunctionRunner(registry=registry)
    store = resolve_durable_run_store(ctx)
    payload = _Order().model_dump(mode="json")
    record = await store.enqueue("order", input_json=payload, idempotency_key="k")

    with pytest.raises(CoreException):
        await runner.run_now(ctx, "order", payload, idempotency_key="k")

    return record, runner
