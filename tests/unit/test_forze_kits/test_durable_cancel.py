"""Durable run control: an operator can ask a self-hosted run to stop, and it does.

The self-hosted function tier had no answer to "stop this run" — the workflow tier's
`cancel`/`terminate` had no counterpart, so a product's Stop button had nowhere to land. This
is the battery for the mechanism that closes it, and most of it is about the ways a cancel
can go *wrong* rather than the happy path:

- the **ask** is unfenced (anyone may ask) but the **landing** is fenced (only the current
  claim holder may write the terminal state), so a stale worker cannot cancel a run out from
  under its new owner — the property that makes this safe on more than one replica;
- cancel racing `complete` produces exactly one terminal state, never a torn one;
- a run whose holder *died* carrying the stamp is landed by recovery **without invoking the
  body** — the one thing a recovery scan must never do to work an operator already stopped;
- a body that ignores the ask is still bounded by the deadline watchdog, and because the ask
  was recorded first the run lands CANCELLED rather than TIMED_OUT.

Cancellation is cooperative and this file does not pretend otherwise: every "the body
stopped" assertion here is really "the body stopped at its next await point".

# covers: DurableRunAdminPort.request_cancel
# covers: DurableRunStorePort.mark_cancelled
# covers: DurableRunStorePort.renew
# covers: DurableFunctionRunner.request_cancel
"""

from __future__ import annotations

import asyncio
from datetime import timedelta

import pytest

from forze.application.contracts.durable.function import (
    DurableRunAdminDepKey,
    DurableRunPage,
    DurableRunStatus,
    durable_run_control_capabilities,
)
from forze.application.execution import Deps, ExecutionContext
from forze.base.exceptions import CoreException
from forze.base.primitives import utcnow
from forze.testing import context_from_modules
from forze_kits.integrations.durable import (
    DurableFunctionRegistry,
    DurableFunctionRunner,
    resolve_durable_run_admin,
    resolve_durable_run_store,
    resolve_durable_step,
)
from forze_mock import MockDepsModule, MockDurableRunStore, MockState

# ----------------------- #

_FAST_LEASE = timedelta(milliseconds=60)
"""A lease short enough that the heartbeat (``lease_for / 2``) observes a cancel in ~30 ms."""


def _runner(registry: DurableFunctionRegistry, **overrides: object) -> DurableFunctionRunner:
    """A runner whose heartbeat fires fast enough for a test to watch it work."""

    return DurableFunctionRunner(
        registry=registry,
        lease_for=_FAST_LEASE,
        heartbeat_divisor=2,
        **overrides,  # type: ignore[arg-type]
    )


# ....................... #


class TestCancelPending:
    async def test_pending_run_lands_cancelled_without_ever_running(self) -> None:
        # Nothing is executing, so there is no holder to wait for: the ask and the landing
        # are the same instant, and the recovery scan must never pick the run up again.
        state = MockState()
        ctx = context_from_modules(MockDepsModule(state=state))
        store = resolve_durable_run_store(ctx)
        admin = resolve_durable_run_admin(ctx)

        ran = {"body": False}
        registry = DurableFunctionRegistry()

        async def body(ctx: ExecutionContext, input_json: dict | None) -> dict:
            ran["body"] = True
            return {"ok": True}

        registry.register("fn", body)
        runner = _runner(registry)

        record = await runner.enqueue(ctx, "fn")
        assert await admin.request_cancel(record.run_id) is True

        cancelled = await store.load(record.run_id)
        assert cancelled is not None
        assert cancelled.status is DurableRunStatus.CANCELLED
        assert cancelled.cancel_requested_at is not None

        # The scanner steps over it: a cancelled run is terminal, so it neither claims a
        # recovery slot nor gets its body invoked.
        assert await store.claim_abandoned(limit=10, lease_for=_FAST_LEASE) == []
        assert await runner.recover(ctx) == 0
        assert ran["body"] is False

    async def test_delayed_run_cancelled_before_it_is_due(self) -> None:
        # A run parked on ``available_at`` is still PENDING, so it stops the same way — and
        # never becomes claimable once its due time passes.
        ctx = context_from_modules(MockDepsModule())
        store = resolve_durable_run_store(ctx)
        admin = resolve_durable_run_admin(ctx)

        record = await store.enqueue(
            "fn", input_json=None, available_at=utcnow() - timedelta(minutes=1)
        )

        assert await admin.request_cancel(record.run_id) is True
        assert await store.claim_abandoned(limit=10, lease_for=_FAST_LEASE) == []


class TestCancelRunning:
    async def test_running_body_stops_at_its_next_await_and_lands_cancelled(self) -> None:
        state = MockState()
        ctx = context_from_modules(MockDepsModule(state=state))
        store = resolve_durable_run_store(ctx)
        admin = resolve_durable_run_admin(ctx)

        after_cancel = {"count": 0}
        registry = DurableFunctionRegistry()

        # Enqueued up front (converged on by idempotency key) so the body knows the run id
        # it is about to have cancelled.
        record = await store.enqueue("fn", input_json=None, idempotency_key="k")
        run_id = record.run_id

        async def body(ctx: ExecutionContext, input_json: dict | None) -> dict:
            step = resolve_durable_step(ctx)

            async def charge() -> dict:
                return {"charged": True}

            await step.run("charge", charge)  # journaled BEFORE anyone asks to stop

            assert await admin.request_cancel(run_id) is True

            # Long enough for the heartbeat to see the stamp and tear the body down.
            await asyncio.sleep(1.0)
            after_cancel["count"] += 1  # must NOT run
            return {"ok": True}

        registry.register("fn", body)
        runner = _runner(registry)

        result = await runner.run_now(ctx, "fn", idempotency_key="k")

        assert after_cancel["count"] == 0
        assert result.status is DurableRunStatus.CANCELLED
        assert result.cancel_requested_at is not None
        assert result.cancel_refused_at is None

        # The journal survives the cancel: a manual re-enqueue replays the charged step
        # rather than charging twice. Cancelling is not a rollback.
        assert state.durable_step_memo[f"{run_id}:charge"] == {"charged": True}

    async def test_cancel_is_not_a_failure(self) -> None:
        # ``run_now`` hands back the CANCELLED record instead of raising: the caller did not
        # cause an error, and a cancel that surfaces as an exception gets logged as one.
        ctx = context_from_modules(MockDepsModule())
        store = resolve_durable_run_store(ctx)
        admin = resolve_durable_run_admin(ctx)

        record = await store.enqueue("fn", input_json=None, idempotency_key="k")
        registry = DurableFunctionRegistry()

        async def body(ctx: ExecutionContext, input_json: dict | None) -> dict:
            await admin.request_cancel(record.run_id)
            await asyncio.sleep(1.0)
            return {"ok": True}

        registry.register("fn", body)

        result = await _runner(registry).run_now(ctx, "fn", idempotency_key="k")

        assert result.status is DurableRunStatus.CANCELLED
        assert result.error is None  # no failure message: nothing failed


class TestCancelIsIdempotent:
    async def test_double_cancel_changes_nothing_and_terminal_runs_refuse(self) -> None:
        ctx = context_from_modules(MockDepsModule())
        store = resolve_durable_run_store(ctx)
        admin = resolve_durable_run_admin(ctx)

        record = await store.enqueue("fn", input_json=None)
        claimed = await store.begin(record.run_id, lease_for=timedelta(minutes=5))
        assert claimed is not None

        assert await admin.request_cancel(record.run_id) is True
        first = await store.load(record.run_id)
        assert first is not None and first.cancel_requested_at is not None

        # Asking again is a genuine no-op — including the timestamp, which must not creep
        # forward every time somebody leans on the button.
        assert await admin.request_cancel(record.run_id) is True
        second = await store.load(record.run_id)
        assert second is not None
        assert second.cancel_requested_at == first.cancel_requested_at
        assert second.status is DurableRunStatus.RUNNING  # still the holder's to land

        await store.mark_cancelled(record.run_id, fence=claimed.attempts)

        # Terminal now: there is nothing left to stop, and the ask reports so.
        assert await admin.request_cancel(record.run_id) is False

    async def test_cancelling_an_unknown_run_reports_nothing_happened(self) -> None:
        ctx = context_from_modules(MockDepsModule())
        admin = resolve_durable_run_admin(ctx)

        assert await admin.request_cancel("no-such-run") is False


class TestFencing:
    async def test_cancel_racing_complete_yields_exactly_one_terminal(self) -> None:
        # Both outcomes are legal — an operator asking while a body is finishing is a real
        # race with no right answer. What is NOT legal is a torn run: a status from one
        # writer and an output from the other.
        ctx = context_from_modules(MockDepsModule())
        store = resolve_durable_run_store(ctx)
        admin = resolve_durable_run_admin(ctx)

        record = await store.enqueue("fn", input_json=None)
        claimed = await store.begin(record.run_id, lease_for=timedelta(minutes=5))
        assert claimed is not None

        await admin.request_cancel(record.run_id)
        await store.complete(record.run_id, output_json={"ok": True}, fence=claimed.attempts)

        # ``complete`` got there first and holds the run; the late cancel landing is a no-op
        # rather than an overwrite that would erase a result the caller already saw.
        await store.mark_cancelled(record.run_id, fence=claimed.attempts)

        loaded = await store.load(record.run_id)
        assert loaded is not None
        assert loaded.status is DurableRunStatus.COMPLETED
        assert loaded.output_json == {"ok": True}

    async def test_cancel_landing_first_wins_and_a_late_complete_is_a_noop(self) -> None:
        ctx = context_from_modules(MockDepsModule())
        store = resolve_durable_run_store(ctx)
        admin = resolve_durable_run_admin(ctx)

        record = await store.enqueue("fn", input_json=None)
        claimed = await store.begin(record.run_id, lease_for=timedelta(minutes=5))
        assert claimed is not None

        await admin.request_cancel(record.run_id)
        await store.mark_cancelled(record.run_id, fence=claimed.attempts)
        await store.complete(record.run_id, output_json={"ok": True}, fence=claimed.attempts)

        loaded = await store.load(record.run_id)
        assert loaded is not None
        assert loaded.status is DurableRunStatus.CANCELLED
        assert loaded.output_json is None

    async def test_a_stale_worker_cannot_cancel_the_new_owners_run(self) -> None:
        # The reason the ask is unfenced but the landing is not: worker A is still alive and
        # believes it holds the run. Its cancel landing must be inert, or a zombie replica
        # could terminate work the current owner is midway through.
        state = MockState()
        store = MockDurableRunStore(state=state)

        record = await store.enqueue("fn", input_json=None)
        worker_a = await store.begin(record.run_id, lease_for=timedelta(minutes=5))
        assert worker_a is not None and worker_a.attempts == 1

        state.durable_runs[record.run_id]["leased_until"] = utcnow() - timedelta(hours=1)
        reclaimed = await store.claim_abandoned(limit=10, lease_for=timedelta(minutes=5))
        worker_b = next(r for r in reclaimed if r.run_id == record.run_id)
        assert worker_b.attempts == 2

        assert await store.request_cancel(record.run_id) is True

        # A's renewal already told it to stop, and its cancel landing is refused too.
        assert (await store.renew(record.run_id, lease_for=_FAST_LEASE, fence=1)).held is False
        await store.mark_cancelled(record.run_id, fence=worker_a.attempts)

        still_running = await store.load(record.run_id)
        assert still_running is not None
        assert still_running.status is DurableRunStatus.RUNNING

        # B — the current holder — sees the ask on its own heartbeat and lands it.
        renewal = await store.renew(record.run_id, lease_for=_FAST_LEASE, fence=2)
        assert renewal.held is True
        assert renewal.cancel_requested is True

        await store.mark_cancelled(record.run_id, fence=worker_b.attempts)
        landed = await store.load(record.run_id)
        assert landed is not None
        assert landed.status is DurableRunStatus.CANCELLED


class TestDeadHolder:
    async def test_recovery_lands_a_cancelled_run_without_invoking_the_body(self) -> None:
        # The holder crashed after the ask was recorded. Recovery inherits the run — and
        # must NOT re-invoke work an operator already stopped, which is exactly what the
        # ordinary recovery path would otherwise do.
        state = MockState()
        ctx = context_from_modules(MockDepsModule(state=state))
        store = resolve_durable_run_store(ctx)
        admin = resolve_durable_run_admin(ctx)

        invoked = {"body": False}
        registry = DurableFunctionRegistry()

        async def body(ctx: ExecutionContext, input_json: dict | None) -> dict:
            invoked["body"] = True
            return {"ok": True}

        registry.register("fn", body)
        runner = _runner(registry)

        record = await store.enqueue("fn", input_json=None)
        claimed = await store.begin(record.run_id, lease_for=timedelta(minutes=5))
        assert claimed is not None

        assert await admin.request_cancel(record.run_id) is True

        # The holder dies with the stamp down: its lease lapses with the run still RUNNING.
        state.durable_runs[record.run_id]["leased_until"] = utcnow() - timedelta(hours=1)

        assert await runner.recover(ctx) == 1

        assert invoked["body"] is False
        landed = await store.load(record.run_id)
        assert landed is not None
        assert landed.status is DurableRunStatus.CANCELLED

        # Terminal for good: a second sweep finds nothing, so the run cannot ping-pong
        # between claim and cancel forever.
        assert await runner.recover(ctx) == 0


class TestCancelComposesWithTheDeadline:
    async def test_a_body_that_ignores_the_ask_is_still_bounded_and_lands_cancelled(
        self,
    ) -> None:
        # Cooperative means a body *can* refuse to notice. The deadline watchdog is the
        # backstop, and the two watchdogs compose: the ask was recorded first, so the run
        # lands CANCELLED (the operator's reason) rather than TIMED_OUT (the cap's).
        ctx = context_from_modules(MockDepsModule())
        store = resolve_durable_run_store(ctx)
        admin = resolve_durable_run_admin(ctx)

        swallowed = {"count": 0}
        record = await store.enqueue("fn", input_json=None, idempotency_key="k")
        registry = DurableFunctionRegistry()

        async def stubborn(ctx: ExecutionContext, input_json: dict | None) -> dict:
            await admin.request_cancel(record.run_id)

            try:
                await asyncio.sleep(5.0)

            except asyncio.CancelledError:
                swallowed["count"] += 1  # ignores the operator's ask and carries on

            await asyncio.sleep(5.0)

            return {"ok": True}  # pragma: no cover — the deadline stops us first

        registry.register("fn", stubborn)
        runner = _runner(registry, max_run_duration=timedelta(milliseconds=200))

        result = await runner.run_now(ctx, "fn", idempotency_key="k")

        assert swallowed["count"] == 1  # the ask was delivered, and ignored
        assert result.status is DurableRunStatus.CANCELLED
        assert result.status is not DurableRunStatus.TIMED_OUT


class _ListOnlyAdmin:
    """A durable-run admin port that can list but cannot reach a running body.

    Stands in for an engine-backed tier whose platform exposes no cancellation mechanism. It
    still *has* ``request_cancel`` — the method is on the port — which is exactly why the
    capability, and not the method's presence, is what the gate consults.
    """

    async def list_runs(self, **kwargs: object) -> DurableRunPage:
        return DurableRunPage(records=[])

    async def request_cancel(self, run_id: str) -> bool:
        raise AssertionError("must not be reached — the gate refuses first")


# ....................... #


def _list_only_admin_module() -> Deps:
    """Wire :class:`_ListOnlyAdmin` over the mock, which registers only as a fallback."""

    return Deps.plain({DurableRunAdminDepKey: lambda _ctx: _ListOnlyAdmin()})


# ....................... #


class TestOrderingAgainstTheDeadline:
    async def test_an_ask_recorded_before_the_deadline_wins_even_if_unobserved(self) -> None:
        # The local signal only learns about an ask on a heartbeat *tick*. Here the heartbeat
        # never gets one — the lease is long, the cap is short — so the ask is recorded, the
        # deadline fires, and nothing in-process knows a cancel was ever requested. The
        # contract orders these by when the ask was *recorded*, so the row has to be
        # consulted; otherwise an operator who pressed Stop sees TIMED_OUT.
        ctx = context_from_modules(MockDepsModule())
        store = resolve_durable_run_store(ctx)
        admin = resolve_durable_run_admin(ctx)

        record = await store.enqueue("fn", input_json=None, idempotency_key="k")
        registry = DurableFunctionRegistry()

        async def body(ctx: ExecutionContext, input_json: dict | None) -> dict:
            assert await admin.request_cancel(record.run_id) is True
            await asyncio.sleep(5.0)

            return {"ok": True}  # pragma: no cover — the deadline stops us first

        registry.register("fn", body)

        runner = DurableFunctionRunner(
            registry=registry,
            lease_for=timedelta(seconds=30),  # heartbeat at 15 s: it will never tick
            heartbeat_divisor=2,
            max_run_duration=timedelta(milliseconds=100),
        )

        result = await runner.run_now(ctx, "fn", idempotency_key="k")

        assert result.status is DurableRunStatus.CANCELLED
        assert result.status is not DurableRunStatus.TIMED_OUT
        assert result.error is None  # a cancel, not a deadline failure

    async def test_a_failed_re_read_still_lands_a_timeout(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # The re-read is best effort, and its failure path only runs when the store is
        # unreachable — the moment it must not make things worse. If it let the read error
        # escape, a store blip during a deadline would replace TIMED_OUT with an unrelated
        # infrastructure error and leave the run's terminal state unwritten.
        from forze_mock.adapters.durable import MockDurableRunStore

        ctx = context_from_modules(MockDepsModule())
        store = resolve_durable_run_store(ctx)

        registry = DurableFunctionRegistry()

        async def body(ctx: ExecutionContext, input_json: dict | None) -> dict:
            await asyncio.sleep(5.0)

            return {"ok": True}  # pragma: no cover — the deadline stops us first

        registry.register("fn", body)

        runner = DurableFunctionRunner(
            registry=registry,
            lease_for=timedelta(seconds=30),  # the heartbeat never ticks
            heartbeat_divisor=2,
            max_run_duration=timedelta(milliseconds=100),
        )

        record = await store.enqueue("fn", input_json=None)

        async def unreachable(*_args: object, **_kwargs: object) -> None:
            raise RuntimeError("the run store is unreachable")

        with monkeypatch.context() as patch:
            patch.setattr(MockDurableRunStore, "load", unreachable)
            # ``recover`` records the outcome itself, so the read failure has to be absorbed
            # inside the deadline path rather than surfacing here.
            assert await runner.recover(ctx) == 1

        landed = await store.load(record.run_id)
        assert landed is not None
        assert landed.status is DurableRunStatus.TIMED_OUT
        assert "max_run_duration" in (landed.error or "")

    async def test_a_deadline_with_no_ask_on_the_record_is_still_a_timeout(self) -> None:
        # The positive control for the check above: the extra read must not turn every
        # deadline into a cancellation.
        ctx = context_from_modules(MockDepsModule())
        store = resolve_durable_run_store(ctx)

        record = await store.enqueue("fn", input_json=None, idempotency_key="k")
        registry = DurableFunctionRegistry()

        async def body(ctx: ExecutionContext, input_json: dict | None) -> dict:
            await asyncio.sleep(5.0)

            return {"ok": True}  # pragma: no cover — the deadline stops us first

        registry.register("fn", body)

        runner = DurableFunctionRunner(
            registry=registry,
            lease_for=timedelta(seconds=30),
            heartbeat_divisor=2,
            max_run_duration=timedelta(milliseconds=100),
        )

        with pytest.raises(CoreException, match="max_run_duration"):
            await runner.run_now(ctx, "fn", idempotency_key="k")

        landed = await store.load(record.run_id)
        assert landed is not None
        assert landed.status is DurableRunStatus.TIMED_OUT


class TestCapabilityGate:
    async def test_request_cancel_fails_closed_on_a_port_that_cannot_cancel(self) -> None:
        # A backend that cannot reach a running body must say so. Accepting the request and
        # dropping it is the failure mode this gate exists to prevent: a Stop button that
        # reports success and does nothing.
        ctx = context_from_modules(_list_only_admin_module, MockDepsModule())

        assert durable_run_control_capabilities(_ListOnlyAdmin()).supports_cancel is False

        with pytest.raises(CoreException, match="does not support cancellation"):
            await _runner(DurableFunctionRegistry()).request_cancel(ctx, "r1")

    async def test_the_self_hosted_store_advertises_cancel_support(self) -> None:
        ctx = context_from_modules(MockDepsModule())
        admin = resolve_durable_run_admin(ctx)

        assert durable_run_control_capabilities(admin).supports_cancel is True

    async def test_the_gate_lets_a_capable_backend_through(self) -> None:
        ctx = context_from_modules(MockDepsModule())
        runner = _runner(DurableFunctionRegistry())

        record = await resolve_durable_run_store(ctx).enqueue("fn", input_json=None)

        assert await runner.request_cancel(ctx, record.run_id) is True
