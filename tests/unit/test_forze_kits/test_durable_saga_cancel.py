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
from statistics import median
from time import perf_counter

import pytest
from pydantic import BaseModel

from forze.application.contracts.durable.function import DurableRunStatus
from forze.application.contracts.saga import SagaDefinition, SagaStep, SagaStepKind
from forze.application.execution import ExecutionContext
from forze.base.exceptions import CoreException, ExceptionKind
from forze.base.primitives import utcnow
from forze.testing import context_from_modules
from forze_kits.integrations.durable import (
    DurableFunctionRegistry,
    DurableFunctionRunner,
    durable_saga_handler,
    resolve_durable_run_admin,
    resolve_durable_run_store,
)
from forze_mock import MockDepsModule, MockState
from forze_mock.adapters.durable import MockDurableRunStore

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
    delay: float = 0.0,
    comp_delay: float = 0.0,
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

        if delay:
            # Outlive at least one heartbeat, so a tick that *would* misread a persisted
            # cancel stamp gets the chance to.
            await asyncio.sleep(delay)

        return OrderCtx(trail=[*state.trail, name])

    async def compensation(_ctx: ExecutionContext, _state: OrderCtx) -> None:
        if comp_delay:
            await asyncio.sleep(comp_delay)

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


    async def test_a_rollback_cut_short_by_a_second_cancel_is_not_reported_clean(self) -> None:
        # The operator's ask is *consumed* by the decision to compensate. If it stayed live
        # through the rollback, a second cancellation arriving mid-compensation (a drain, or
        # the deadline watchdog here) would still be attributed to it — and the run would be
        # reported CANCELLED, i.e. "completed steps were compensated", over a rollback that
        # was abandoned halfway.
        ctx = context_from_modules(MockDepsModule())
        admin = resolve_durable_run_admin(ctx)
        store = resolve_durable_run_store(ctx)

        effects: list[str] = []
        holder: dict[str, str] = {}

        async def ask_to_stop() -> None:
            assert await admin.request_cancel(holder["run_id"]) is True

        saga: SagaDefinition[OrderCtx] = SagaDefinition(
            name="order",
            steps=(
                # Its compensation outlives the deadline below, so the rollback is still
                # running when the second cancellation lands.
                _step("reserve", effects, comp_delay=5.0),
                _step("charge", effects, stop=True, on_stop=ask_to_stop),
                _step("ship", effects, kind=SagaStepKind.PIVOT),
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

        record = await store.enqueue(
            "order", input_json=OrderCtx().model_dump(mode="json"), idempotency_key="k"
        )
        holder["run_id"] = record.run_id

        await runner.recover(ctx)

        landed = await store.load(record.run_id)
        assert landed is not None

        # An interrupted rollback is an operator problem, not a clean stop. The summary names
        # the failing step and the rollback's state; which compensation was cut short rides
        # in the error's details and the warning log, as it does for any compensation failure.
        assert landed.status is not DurableRunStatus.CANCELLED
        assert landed.status is DurableRunStatus.FAILED
        assert "compensation failed" in (landed.error or "")
        assert "manual intervention required" in (landed.error or "")

        # And the rollback really was cut short — ``reserve`` never undid anything.
        assert "undo:reserve" not in effects

    async def test_a_lease_lost_during_rollback_leaves_the_run_for_recovery(self) -> None:
        # Converting an interrupted rollback into ``saga.compensation_failed`` must not
        # outrank a lease loss. A renewal *error* (a DB blip) is treated as lease loss while
        # the fence is still valid — so a terminal write would actually land, marking a run
        # FAILED that recovery should have replayed, with the rollback half-done.
        state = MockState()
        ctx = context_from_modules(MockDepsModule(state=state))
        store = resolve_durable_run_store(ctx)

        effects: list[str] = []
        holder: dict[str, str] = {}
        renewals = {"n": 0}

        real_renew = MockDurableRunStore.renew

        async def flaky_renew(self, run_id, *, lease_for, fence):  # type: ignore[no-untyped-def]
            renewals["n"] += 1

            if renewals["n"] == 1:
                # First beat: report the ask, so the saga starts compensating.
                await self.request_cancel(run_id)

                return await real_renew(self, run_id, lease_for=lease_for, fence=fence)

            raise RuntimeError("the database blinked")  # → treated as lease loss

        saga: SagaDefinition[OrderCtx] = SagaDefinition(
            name="order",
            steps=(
                _step("reserve", effects, comp_delay=5.0),
                _step("charge", effects, delay=5.0),
                _step("ship", effects, kind=SagaStepKind.PIVOT),
            ),
        )

        registry = DurableFunctionRegistry()
        registry.register("order", durable_saga_handler(saga, OrderCtx))
        runner = DurableFunctionRunner(
            registry=registry, lease_for=_FAST_LEASE, heartbeat_divisor=2
        )

        record = await store.enqueue(
            "order", input_json=OrderCtx().model_dump(mode="json")
        )
        holder["run_id"] = record.run_id

        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(MockDurableRunStore, "renew", flaky_renew)
            assert await runner.recover(ctx) == 1

        landed = await store.load(record.run_id)
        assert landed is not None

        # No terminal write from a holder that cannot prove it still owns the run: it stays
        # RUNNING for the recovery scan to reclaim and replay.
        assert landed.status is DurableRunStatus.RUNNING
        assert landed.status is not DurableRunStatus.FAILED
        assert landed.error is None

    async def test_the_un_compensated_tally_counts_only_steps_that_have_a_rollback(
        self,
    ) -> None:
        # The interruption message tells an operator how much is left un-rolled-back, so it
        # must count steps that actually *have* a compensation. ``audit`` completed but has
        # none, and it sits later in the reverse rollback order — counted naively it would
        # report two steps outstanding when only one can ever be undone.
        ctx = context_from_modules(MockDepsModule())
        admin = resolve_durable_run_admin(ctx)

        effects: list[str] = []
        holder: dict[str, str] = {}

        async def ask_to_stop() -> None:
            assert await admin.request_cancel(holder["run_id"]) is True

        async def audit(_ctx: ExecutionContext, state: OrderCtx) -> OrderCtx:
            effects.append("do:audit")
            return OrderCtx(trail=[*state.trail, "audit"])

        saga: SagaDefinition[OrderCtx] = SagaDefinition(
            name="order",
            steps=(
                # Completes first, so it is rolled back *last* — and has nothing to roll back.
                SagaStep(
                    name="audit",
                    action=audit,
                    compensation=None,
                    kind=SagaStepKind.COMPENSATABLE,
                    tx_route="mock",
                ),
                _step("reserve", effects, comp_delay=5.0),
                _step("charge", effects, stop=True, on_stop=ask_to_stop),
                _step("ship", effects, kind=SagaStepKind.PIVOT),
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
            "order", input_json=OrderCtx().model_dump(mode="json"), idempotency_key="k"
        )
        holder["run_id"] = record.run_id

        with pytest.raises(CoreException, match="compensation failed") as caught:
            await runner.run_now(ctx, "order", idempotency_key="k")

        reported = " ".join(caught.value.details["compensation_errors"])  # type: ignore[index]
        assert "1 step(s) are left un-compensated" in reported
        assert "2 step(s)" not in reported

    async def test_a_cancel_whose_rollback_failed_lands_failed_not_cancelled(self) -> None:
        # The branch that only runs when the bad thing happens, and therefore the one most
        # likely to be wrong and never noticed. A cancel is normally a clean, non-alarming
        # outcome — but if the *compensation* fails, the system may be inconsistent, and
        # reporting "cancelled, completed steps were compensated" over a half-rolled-back
        # saga would tell an operator the exact opposite of the truth.
        ctx = context_from_modules(MockDepsModule())
        admin = resolve_durable_run_admin(ctx)
        store = resolve_durable_run_store(ctx)

        effects: list[str] = []
        holder: dict[str, str] = {}

        async def ask_to_stop() -> None:
            assert await admin.request_cancel(holder["run_id"]) is True

        async def reserve(_ctx: ExecutionContext, state: OrderCtx) -> OrderCtx:
            effects.append("do:reserve")
            return OrderCtx(trail=[*state.trail, "reserve"])

        async def failing_undo(_ctx: ExecutionContext, _state: OrderCtx) -> None:
            effects.append("undo:reserve:failed")
            raise RuntimeError("the refund gateway is down")

        saga: SagaDefinition[OrderCtx] = SagaDefinition(
            name="order",
            steps=(
                SagaStep(
                    name="reserve",
                    action=reserve,
                    compensation=failing_undo,
                    kind=SagaStepKind.COMPENSATABLE,
                    tx_route="mock",
                ),
                _step("charge", effects, stop=True, on_stop=ask_to_stop),
            ),
        )

        runner, run_id = await _drive(ctx, saga, holder)

        # A failed rollback is a genuine error, so it reaches the caller — unlike a clean
        # cancel, which returns the record.
        with pytest.raises(CoreException, match="compensation failed") as caught:
            await runner.run_now(ctx, "order", idempotency_key="k")

        assert caught.value.code == "saga.compensation_failed"
        assert caught.value.kind is ExceptionKind.INFRASTRUCTURE
        assert effects == ["do:reserve", "do:charge", "undo:reserve:failed"]

        landed = await store.load(run_id)
        assert landed is not None
        assert landed.status is DurableRunStatus.FAILED
        assert landed.status is not DurableRunStatus.CANCELLED  # the whole point
        assert "compensation failed" in (landed.error or "")


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

    async def test_a_failed_refusal_stamp_cannot_destroy_the_runs_real_outcome(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # The refusal stamp is written in the runner's ``finally``, which makes it dangerous
        # out of proportion to its importance: an error there would replace whatever outcome
        # is propagating and skip the telemetry after it. The stamp is *advisory* — losing it
        # costs an operator one piece of context; losing the run's actual result costs them
        # the incident.
        ctx = context_from_modules(MockDepsModule())
        admin = resolve_durable_run_admin(ctx)
        store = resolve_durable_run_store(ctx)

        async def boom(*_args: object, **_kwargs: object) -> None:
            raise RuntimeError("the run store is having a bad day")

        monkeypatch.setattr(MockDurableRunStore, "refuse_cancel", boom)

        effects: list[str] = []
        holder: dict[str, str] = {}

        async def ask_to_stop() -> None:
            await admin.request_cancel(holder["run_id"])

        saga: SagaDefinition[OrderCtx] = SagaDefinition(
            name="order",
            steps=(
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

        runner, run_id = await _drive(ctx, saga, holder)

        # The saga still completes forward and the caller still gets its record — the
        # bookkeeping failure is swallowed, not promoted into the run's result.
        result = await runner.run_now(ctx, "order", idempotency_key="k")

        assert result.status is DurableRunStatus.COMPLETED
        assert result.output_json == {"trail": ["charge", "ship"]}

        # Only the advisory stamp is lost, and the ask itself is still on the record.
        landed = await store.load(run_id)
        assert landed is not None
        assert landed.cancel_requested_at is not None
        assert landed.cancel_refused_at is None

    async def test_the_refusal_is_persisted_while_the_saga_is_still_going_forward(
        self,
    ) -> None:
        # A refusal written only in the runner's ``finally`` is invisible for as long as the
        # saga takes to complete forward — minutes, potentially. Die in that window and the
        # row carries an ask with no refusal, which recovery reads as an ordinary
        # cancel-stamped run and lands CANCELLED, abandoning a saga past its pivot. So the
        # heartbeat writes it: here the body itself reads the row mid-flight and must already
        # see the stamp, long before the run finishes.
        ctx = context_from_modules(MockDepsModule())
        admin = resolve_durable_run_admin(ctx)
        store = resolve_durable_run_store(ctx)

        effects: list[str] = []
        holder: dict[str, str] = {}
        seen = {"stamped_mid_run": False}
        calls = {"n": 0}
        writes = {"n": 0}

        real_refuse = MockDurableRunStore.refuse_cancel

        async def counted_refuse(self, run_id, *, fence=None):  # type: ignore[no-untyped-def]
            writes["n"] += 1

            return await real_refuse(self, run_id, fence=fence)

        async def ship(_ctx: ExecutionContext, state: OrderCtx) -> OrderCtx:
            calls["n"] += 1
            effects.append("do:ship")

            if calls["n"] == 1:
                assert await admin.request_cancel(holder["run_id"]) is True
                await asyncio.sleep(1.0)  # cancelled here; the saga refuses and re-runs us

            # Second pass — still going forward. Give the heartbeat a beat, then look.
            await asyncio.sleep(0.15)
            mid = await store.load(holder["run_id"])
            seen["stamped_mid_run"] = mid is not None and mid.cancel_refused_at is not None

            return OrderCtx(trail=[*state.trail, "ship"])

        saga: SagaDefinition[OrderCtx] = SagaDefinition(
            name="order",
            steps=(
                _step("charge", effects, kind=SagaStepKind.PIVOT),
                SagaStep(
                    name="ship",
                    action=ship,
                    compensation=None,
                    kind=SagaStepKind.RETRYABLE,
                    tx_route="mock",
                    idempotent=True,
                ),
            ),
        )

        runner, run_id = await _drive(ctx, saga, holder)

        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(MockDurableRunStore, "refuse_cancel", counted_refuse)
            result = await runner.run_now(ctx, "order", idempotency_key="k")

        assert result.status is DurableRunStatus.COMPLETED
        assert seen["stamped_mid_run"] is True  # persisted before the run ended, not after

        # Exactly once: the heartbeat wrote it, so the runner's ``finally`` backstop stands
        # down. The two writers share one flag rather than both paying for the stamp.
        assert writes["n"] == 1

        landed = await store.load(run_id)
        assert landed is not None
        assert landed.cancel_refused_at is not None

    async def test_a_slow_refusal_write_does_not_stretch_the_renewal_cadence(self) -> None:
        # The renewal loop has no spare capacity: at ``heartbeat_divisor=2`` one sleep plus
        # one renewal already spend the whole lease. A refusal write bolted on top would push
        # the next renewal past expiry — so a still-running post-pivot saga would lose its
        # lease to a second worker, which is the double-execution the heartbeat exists to
        # prevent, caused by the bookkeeping added to make cancellation safer.
        ctx = context_from_modules(MockDepsModule())
        admin = resolve_durable_run_admin(ctx)
        store = resolve_durable_run_store(ctx)

        effects: list[str] = []
        holder: dict[str, str] = {}
        renewals: list[float] = []
        calls = {"n": 0}

        real_renew = MockDurableRunStore.renew
        real_refuse = MockDurableRunStore.refuse_cancel

        async def timed_renew(self, run_id, *, lease_for, fence):  # type: ignore[no-untyped-def]
            renewals.append(perf_counter())

            return await real_renew(self, run_id, lease_for=lease_for, fence=fence)

        async def slow_refuse(self, run_id, *, fence=None):  # type: ignore[no-untyped-def]
            # Most of a heartbeat interval, but inside the write's own timeout so it lands.
            await asyncio.sleep(0.08)

            return await real_refuse(self, run_id, fence=fence)

        async def ship(_ctx: ExecutionContext, state: OrderCtx) -> OrderCtx:
            calls["n"] += 1
            effects.append("do:ship")

            if calls["n"] == 1:
                assert await admin.request_cancel(holder["run_id"]) is True
                await asyncio.sleep(1.0)  # cancelled; the saga refuses and re-runs us

            await asyncio.sleep(0.6)  # keep going forward across several beats

            return OrderCtx(trail=[*state.trail, "ship"])

        saga: SagaDefinition[OrderCtx] = SagaDefinition(
            name="order",
            steps=(
                _step("charge", effects, kind=SagaStepKind.PIVOT),
                SagaStep(
                    name="ship",
                    action=ship,
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
            lease_for=timedelta(milliseconds=200),  # heartbeat every 100 ms
            heartbeat_divisor=2,
        )

        record = await store.enqueue(
            "order", input_json=OrderCtx().model_dump(mode="json"), idempotency_key="k"
        )
        holder["run_id"] = record.run_id

        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(MockDurableRunStore, "renew", timed_renew)
            patch.setattr(MockDurableRunStore, "refuse_cancel", slow_refuse)
            result = await runner.run_now(ctx, "order", idempotency_key="k")

        assert result.status is DurableRunStatus.COMPLETED

        # Judged against the loop's own median, not a wall-clock bound: under load (a
        # coverage run, a busy CI box) every beat stretches together, so an absolute
        # threshold measures the machine rather than the code. The defect is *one* beat
        # standing out — uncompensated it carries the 80 ms write on top of its 100 ms sleep
        # and runs ~1.8x its neighbours, closing on the 200 ms lease it exists to protect.
        gaps = [b - a for a, b in zip(renewals, renewals[1:], strict=False)]
        assert len(gaps) >= 3, f"too few renewals to judge the cadence: {renewals}"

        typical = median(gaps)
        assert max(gaps) < typical * 1.4, f"one beat outran its neighbours: {gaps}"

    async def test_refusal_is_recorded_even_when_the_step_swallows_the_cancel(self) -> None:
        # The refusal branch in `_advance` only runs when the CancelledError escapes the step
        # action. A post-pivot step that catches its own cancellation completes normally, so
        # the saga goes forward past the pivot with an ask outstanding and — without the
        # outcome-level check — nothing is stamped. The operator is then left with a record
        # that says "asked" and completed, unable to tell a refusal from a lost request.
        ctx = context_from_modules(MockDepsModule())
        admin = resolve_durable_run_admin(ctx)
        store = resolve_durable_run_store(ctx)

        effects: list[str] = []
        holder: dict[str, str] = {}

        async def swallowing_ship(_ctx: ExecutionContext, state: OrderCtx) -> OrderCtx:
            effects.append("do:ship")
            assert await admin.request_cancel(holder["run_id"]) is True

            try:
                await asyncio.sleep(1.0)  # the heartbeat cancels us here...

            except asyncio.CancelledError:
                effects.append("swallowed")  # ...and the step simply carries on

            return OrderCtx(trail=[*state.trail, "ship"])

        saga: SagaDefinition[OrderCtx] = SagaDefinition(
            name="order",
            steps=(
                _step("charge", effects, kind=SagaStepKind.PIVOT),
                SagaStep(
                    name="ship",
                    action=swallowing_ship,
                    compensation=None,
                    kind=SagaStepKind.RETRYABLE,
                    tx_route="mock",
                    idempotent=True,
                ),
            ),
        )

        runner, run_id = await _drive(ctx, saga, holder)
        result = await runner.run_now(ctx, "order", idempotency_key="k")

        assert effects == ["do:charge", "do:ship", "swallowed"]
        assert result.status is DurableRunStatus.COMPLETED

        landed = await store.load(run_id)
        assert landed is not None
        assert landed.cancel_requested_at is not None
        assert landed.cancel_refused_at is not None  # the ask was declined, and it says so

    async def test_a_refused_run_reclaimed_by_recovery_completes_forward(self) -> None:
        # The ask stays on the row after a refusal. If recovery treated that row the way it
        # treats any other cancel-stamped run — land CANCELLED, never invoke the body — a
        # saga that had already committed at its pivot would be abandoned there and reported
        # as "cancelled, nothing wrong". Past the pivot the only legal outcomes are forward
        # completion or FORWARD_INCOMPLETE; cancellation is not among them.
        state = MockState()
        ctx = context_from_modules(MockDepsModule(state=state))
        admin = resolve_durable_run_admin(ctx)
        store = resolve_durable_run_store(ctx)

        effects: list[str] = []
        registry = DurableFunctionRegistry()

        saga: SagaDefinition[OrderCtx] = SagaDefinition(
            name="order",
            steps=(
                _step("charge", effects, kind=SagaStepKind.PIVOT),
                # Slow enough to outlive a heartbeat tick: the ask is still on the row, so a
                # runner that did not start this attempt's signal spent would read it back,
                # cancel the body, and re-run the step — visible below as a duplicate effect.
                _step("ship", effects, kind=SagaStepKind.RETRYABLE, delay=0.1),
            ),
        )
        registry.register("order", durable_saga_handler(saga, OrderCtx))
        runner = DurableFunctionRunner(
            registry=registry, lease_for=_FAST_LEASE, heartbeat_divisor=2
        )

        # A previous attempt got past the pivot, was asked to stop, refused — and then lost
        # its worker before it could finish going forward.
        record = await store.enqueue(
            "order", input_json=OrderCtx().model_dump(mode="json")
        )
        claimed = await store.begin(record.run_id, lease_for=_FAST_LEASE)
        assert claimed is not None

        assert await admin.request_cancel(record.run_id) is True
        await store.refuse_cancel(record.run_id, fence=claimed.attempts)
        state.durable_runs[record.run_id]["leased_until"] = utcnow() - timedelta(hours=1)

        assert await runner.recover(ctx) == 1

        # The body ran and the saga finished going forward, rather than being landed
        # CANCELLED on the strength of a stamp it had already declined.
        assert effects == ["do:charge", "do:ship"]

        landed = await store.load(record.run_id)
        assert landed is not None
        assert landed.status is DurableRunStatus.COMPLETED
        assert landed.status is not DurableRunStatus.CANCELLED
        assert landed.cancel_refused_at is not None  # the refusal still stands on the record

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
