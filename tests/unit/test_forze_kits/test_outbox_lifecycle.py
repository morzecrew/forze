"""Unit tests for outbox background relay lifecycle."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID, uuid4

import pytest
from pydantic import BaseModel

from forze.application.contracts.execution import LifecycleStep
from forze.application.contracts.outbox import (
    OutboxRelayResult,
    OutboxSpec,
    OutboxStatus,
)
from forze.application.contracts.pubsub import PubSubSpec
from forze.application.contracts.queue import QueueSpec
from forze.application.execution import DepsRegistry, ExecutionRuntime
from forze.base.exceptions import CoreException
from forze.base.serialization import PydanticModelCodec
from forze_kits.integrations.outbox import (
    OutboxRelay,
    outbox_relay_background_lifecycle_step,
)
from forze_kits.integrations.outbox.lifecycle import _OutboxRelayBackgroundStartup
from forze_mock import MockDepsModule, MockStateDepKey
from forze_mock.adapters.outbox import MockOutboxRow


class _Payload(BaseModel):
    x: int


@pytest.mark.asyncio
async def test_background_lifecycle_starts_and_stops_task() -> None:
    codec = PydanticModelCodec(_Payload)
    outbox_spec = OutboxSpec(name="events", codec=codec)
    queue_spec = QueueSpec(name="jobs", codec=codec)
    step = outbox_relay_background_lifecycle_step(
        outbox_spec=outbox_spec,
        queue_spec=queue_spec,
        interval=timedelta(hours=1),
        reclaim_stale_after=None,
    )

    relay_mock = AsyncMock()
    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(MockDepsModule()).freeze())

    with patch.object(OutboxRelay, "to_queue", relay_mock):
        async with runtime.scope():
            ctx = runtime.get_context()
            await step.startup(ctx)
            startup = step.startup
            assert isinstance(startup, _OutboxRelayBackgroundStartup)
            assert startup.task is not None
            await asyncio.sleep(0.05)
            await step.shutdown(ctx)

    relay_mock.assert_called()
    assert startup.task.done()


# ----------------------- #
# Drain-until-empty tick behavior


def _startup(**overrides: Any) -> _OutboxRelayBackgroundStartup:
    codec = PydanticModelCodec(_Payload)
    kwargs: dict[str, Any] = {
        "outbox_spec": OutboxSpec(name="events", codec=codec),
        "transport": "queue",
        "queue_spec": QueueSpec(name="jobs", codec=codec),
        "stream_spec": None,
        "pubsub_spec": None,
        "interval": timedelta(hours=1),
        "reclaim_stale_after": timedelta(minutes=5),
        "limit": 10,
        "max_attempts": 5,
        "retry_base_delay": timedelta(seconds=1),
        "retry_max_backoff": timedelta(minutes=5),
        "max_batches_per_tick": 100,
    }
    kwargs.update(overrides)
    return _OutboxRelayBackgroundStartup(**kwargs)


def _result(claimed: int) -> OutboxRelayResult:
    return OutboxRelayResult(claimed=claimed, published=claimed)


async def _run_tick(
    startup: _OutboxRelayBackgroundStartup,
    relay_mock: AsyncMock,
) -> None:
    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(MockDepsModule()).freeze())

    with patch.object(OutboxRelay, "to_queue", relay_mock):
        async with runtime.scope():
            await startup._relay_once(runtime.get_context())


@pytest.mark.asyncio
async def test_relay_once_drains_backlog_until_short_claim() -> None:
    startup = _startup(limit=10)
    relay_mock = AsyncMock(side_effect=[_result(10), _result(10), _result(10), _result(3)])

    await _run_tick(startup, relay_mock)

    # Backlog of 3 full batches drains in one tick: 3 full claims + 1 short.
    assert relay_mock.await_count == 4


@pytest.mark.asyncio
async def test_relay_once_respects_max_batches_per_tick_cap() -> None:
    startup = _startup(limit=10, max_batches_per_tick=5)
    relay_mock = AsyncMock(return_value=_result(10))

    await _run_tick(startup, relay_mock)

    assert relay_mock.await_count == 5


@pytest.mark.asyncio
async def test_relay_once_empty_backlog_claims_exactly_once() -> None:
    startup = _startup(limit=10)
    relay_mock = AsyncMock(return_value=_result(0))

    await _run_tick(startup, relay_mock)

    assert relay_mock.await_count == 1


@pytest.mark.asyncio
async def test_relay_once_reclaims_only_with_first_batch() -> None:
    reclaim = timedelta(minutes=5)
    startup = _startup(limit=10, reclaim_stale_after=reclaim)
    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(MockDepsModule()).freeze())

    # autospec passes ``self`` → each batch builds a fresh OutboxRelay carrying that
    # batch's reclaim policy (first batch reclaims, the rest do not).
    with patch.object(OutboxRelay, "to_queue", autospec=True) as relay_mock:
        relay_mock.side_effect = [_result(10), _result(10), _result(0)]
        async with runtime.scope():
            await startup._relay_once(runtime.get_context())

    reclaims = [call.args[0].reclaim_stale_after for call in relay_mock.call_args_list]
    assert reclaims == [reclaim, None, None]


@pytest.mark.asyncio
async def test_relay_once_failing_batch_is_logged_and_tick_continues() -> None:
    startup = _startup(limit=10)
    relay_mock = AsyncMock(side_effect=[RuntimeError("boom"), _result(0)])
    logger_mock = MagicMock()

    with patch("forze_kits.integrations.outbox.lifecycle.logger", logger_mock):
        await _run_tick(startup, relay_mock)

    assert relay_mock.await_count == 2
    logger_mock.exception.assert_called_once()


def test_lifecycle_step_rejects_invalid_options() -> None:
    codec = PydanticModelCodec(_Payload)
    outbox_spec = OutboxSpec(name="events", codec=codec)
    queue_spec = QueueSpec(name="jobs", codec=codec)

    with pytest.raises(CoreException, match="max_attempts"):
        outbox_relay_background_lifecycle_step(
            outbox_spec=outbox_spec,
            queue_spec=queue_spec,
            max_attempts=0,
        )

    with pytest.raises(CoreException, match="retry_base_delay"):
        outbox_relay_background_lifecycle_step(
            outbox_spec=outbox_spec,
            queue_spec=queue_spec,
            retry_base_delay=timedelta(0),
        )

    with pytest.raises(CoreException, match="retry_max_backoff"):
        outbox_relay_background_lifecycle_step(
            outbox_spec=outbox_spec,
            queue_spec=queue_spec,
            retry_base_delay=timedelta(seconds=10),
            retry_max_backoff=timedelta(seconds=1),
        )

    with pytest.raises(CoreException, match="batches per tick"):
        outbox_relay_background_lifecycle_step(
            outbox_spec=outbox_spec,
            queue_spec=queue_spec,
            max_batches_per_tick=0,
        )


# ----------------------- #
# Tenant-sharded drain (namespace-tier outbox)


_T1 = UUID("11111111-1111-1111-1111-111111111111")
_T2 = UUID("22222222-2222-2222-2222-222222222222")


async def _drain_capturing_tenants(startup: _OutboxRelayBackgroundStartup) -> list[UUID | None]:
    """Run one drain tick with ``to_queue`` mocked to record the bound tenant per pass."""

    seen: list[UUID | None] = []

    async def _capture(
        self: Any, ctx: Any, queue_spec: Any, *, limit: Any = None
    ) -> OutboxRelayResult:
        tenant = ctx.inv_ctx.get_tenant()
        seen.append(tenant.tenant_id if tenant is not None else None)
        return _result(0)

    frozen = list(startup.tenants()) if startup.tenants is not None else None
    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(MockDepsModule()).freeze())
    with patch.object(OutboxRelay, "to_queue", autospec=True, side_effect=_capture):
        async with runtime.scope():
            await startup._drain_tick(runtime.get_context(), frozen)

    return seen


@pytest.mark.asyncio
async def test_drain_tick_relays_each_assigned_tenant_bound() -> None:
    seen = await _drain_capturing_tenants(_startup(tenants=lambda: [_T1, _T2]))

    assert seen == [_T1, _T2]  # one pass per assigned tenant, each bound, in shard order


@pytest.mark.asyncio
async def test_drain_tick_without_tenants_runs_one_global_pass() -> None:
    seen = await _drain_capturing_tenants(_startup(tenants=None))

    assert seen == [None]  # tenant-global outbox: a single unbound pass


@pytest.mark.asyncio
async def test_drain_tick_isolates_a_failing_tenant() -> None:
    startup = _startup(tenants=lambda: [_T1, _T2])
    seen: list[UUID] = []

    async def _once(self: Any, ctx: Any) -> None:
        tenant = ctx.inv_ctx.get_tenant().tenant_id
        seen.append(tenant)
        if tenant == _T1:
            raise RuntimeError("boom")

    logger_mock = MagicMock()
    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(MockDepsModule()).freeze())

    with patch.object(
        _OutboxRelayBackgroundStartup, "_relay_once", autospec=True, side_effect=_once
    ):
        with patch("forze_kits.integrations.outbox.lifecycle.logger", logger_mock):
            async with runtime.scope():
                await startup._drain_tick(runtime.get_context(), [_T1, _T2])

    assert seen == [_T1, _T2]  # T1 failed but T2 still drained this tick
    logger_mock.exception.assert_called_once()  # the failing tenant was logged


# ----------------------- #
# Shutdown drain (opt-in)
#
# Default shutdown cancels the relay task, so rows staged just before teardown sit
# ``pending`` until some later process claims them. ``drain_on_shutdown`` publishes what is
# still claimable instead. The drain must never be worse than that default, which is what
# most of these tests pin: it burns exactly one delivery attempt per row (so it cannot
# dead-letter a backlog the next process would have delivered), it stops instead of
# hammering a dead backend, and it is bounded structurally rather than only by a clock.


_ENQUEUE = "forze_mock.adapters.queue.MockQueueAdapter.enqueue"

_T0 = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)


def _outbox_row(*, index: int = 0) -> MockOutboxRow:
    return MockOutboxRow(
        id=uuid4(),
        outbox_route="events",
        event_id=uuid4(),
        event_type="job.requested",
        payload={"x": index},
        status=OutboxStatus.PENDING,
        tenant_id=None,
        execution_id=None,
        correlation_id=None,
        causation_id=None,
        occurred_at=_T0,
        created_at=_T0 + timedelta(microseconds=index),
        attempts=0,
    )


def _drain_step(**overrides: Any) -> LifecycleStep:
    codec = PydanticModelCodec(_Payload)
    kwargs: dict[str, Any] = {
        "outbox_spec": OutboxSpec(name="events", codec=codec),
        "queue_spec": QueueSpec(name="jobs", codec=codec),
        "interval": timedelta(hours=1),
        "reclaim_stale_after": None,
        "drain_on_shutdown": True,
        "requires": ("db",),
    }
    kwargs.update(overrides)
    return outbox_relay_background_lifecycle_step(**kwargs)


async def _scope_staging_rows_before_shutdown(
    step: LifecycleStep,
    rows: list[MockOutboxRow],
) -> None:
    """Start the relay on an empty route, stage *rows*, then shut down.

    Staging only *after* the first tick has run and parked on its hour-long sleep is what
    isolates the shutdown path: whatever publishes these rows can only be the drain.
    """

    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(MockDepsModule()).freeze())

    async with runtime.scope():
        ctx = runtime.get_context()
        state = ctx.deps.provide(MockStateDepKey)

        await step.startup(ctx)
        await asyncio.sleep(0.05)

        state.outbox_rows["events"] = list(rows)
        await step.shutdown(ctx)


async def _drain_directly(
    startup: _OutboxRelayBackgroundStartup,
    *,
    budget: float = 60.0,
    **patch_kwargs: Any,
) -> Any:
    """Run just the drain (no poll loop) against a patched transport; return its mock."""

    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(MockDepsModule()).freeze())

    with patch.object(OutboxRelay, "to_queue", **patch_kwargs) as relay_mock:
        async with runtime.scope():
            deadline = asyncio.get_running_loop().time() + budget
            await startup.drain_for_shutdown(runtime.get_context(), deadline=deadline)

    return relay_mock


@pytest.mark.asyncio
async def test_shutdown_drain_publishes_rows_staged_after_the_last_tick() -> None:
    rows = [_outbox_row(index=i) for i in range(3)]

    await _scope_staging_rows_before_shutdown(_drain_step(), rows)

    assert [row.status for row in rows] == [OutboxStatus.PUBLISHED] * 3


@pytest.mark.asyncio
async def test_shutdown_without_drain_leaves_rows_pending() -> None:
    rows = [_outbox_row(index=i) for i in range(3)]

    # The default: cancel the task and let the next process pick the rows up. This pair of
    # tests is the whole feature — the drain is exactly the difference between them.
    await _scope_staging_rows_before_shutdown(
        _drain_step(drain_on_shutdown=False, requires=()), rows
    )

    assert [row.status for row in rows] == [OutboxStatus.PENDING] * 3


@pytest.mark.asyncio
async def test_shutdown_drain_burns_at_most_one_attempt_per_row() -> None:
    # Destination down. A drain that kept going would re-claim the rows it just rescheduled
    # (they sort back to the head of the queue by ``created_at``) and burn every attempt —
    # dead-lettering a backlog the next process would have delivered. ``max_attempts=2``
    # makes that visible: a second attempt would park these rows ``failed``.
    rows = [_outbox_row(index=i) for i in range(2)]
    step = _drain_step(
        limit=2,  # a full claim, so the pass cannot end as "drained" on the first batch
        max_attempts=2,
        retry_base_delay=timedelta(microseconds=1),  # rescheduled rows are claimable again at once
        retry_max_backoff=timedelta(seconds=1),
    )

    with patch(_ENQUEUE, AsyncMock(side_effect=RuntimeError("broker down"))):
        await _scope_staging_rows_before_shutdown(step, rows)

    assert [row.attempts for row in rows] == [1, 1]
    assert [row.status for row in rows] == [OutboxStatus.PENDING] * 2  # never dead-lettered


@pytest.mark.asyncio
async def test_shutdown_drain_stops_on_a_failing_batch_instead_of_spinning() -> None:
    # The steady tick tolerates a failing batch because the next tick retries it. The drain
    # has no next tick, so it must not hammer a dead backend ``max_batches_per_tick`` times
    # with no backoff.
    relay_mock = await _drain_directly(
        _startup(drain_on_shutdown=True, max_batches_per_tick=100),
        new=AsyncMock(side_effect=RuntimeError("db down")),
    )

    assert relay_mock.await_count == 1


@pytest.mark.asyncio
async def test_shutdown_drain_is_bounded_by_the_batch_cap() -> None:
    # A time-only bound is not a bound: under simulation the virtual clock does not advance
    # while the loop has ready work, so a drain that only watched a deadline would never
    # stop. The structural cap is what guarantees termination.
    relay_mock = await _drain_directly(
        _startup(limit=10, max_batches_per_tick=5, drain_on_shutdown=True),
        new=AsyncMock(return_value=_result(10)),  # always a full claim: never "drained"
    )

    assert relay_mock.await_count == 5


@pytest.mark.asyncio
async def test_shutdown_drain_on_an_empty_outbox_costs_one_claim_and_skips_reclaim() -> None:
    # The 99% case: shutdown must not get measurably slower. Reclaiming rows abandoned by
    # some *other* dead process is not this teardown's business either.
    relay_mock = await _drain_directly(
        _startup(reclaim_stale_after=timedelta(minutes=5), drain_on_shutdown=True),
        autospec=True,
        return_value=_result(0),
    )

    assert relay_mock.await_count == 1
    assert relay_mock.call_args_list[0].args[0].reclaim_stale_after is None


@pytest.mark.asyncio
async def test_shutdown_drain_covers_each_assigned_tenant_bound() -> None:
    startup = _startup(tenants=lambda: [_T1, _T2], drain_on_shutdown=True)
    startup.tenant_shard = [_T1, _T2]  # normally frozen at startup
    seen: list[UUID | None] = []

    async def _capture(
        self: Any, ctx: Any, queue_spec: Any, *, limit: Any = None
    ) -> OutboxRelayResult:
        tenant = ctx.inv_ctx.get_tenant()
        seen.append(tenant.tenant_id if tenant is not None else None)
        return _result(0)

    await _drain_directly(startup, autospec=True, side_effect=_capture)

    assert seen == [_T1, _T2]


@pytest.mark.asyncio
async def test_double_shutdown_is_a_no_op() -> None:
    # A CancelledError escaping this hook would abort teardown of *every* remaining
    # lifecycle step — the runner reads ``task.exception()``, and that re-raises it.
    step = _drain_step()
    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(MockDepsModule()).freeze())

    async with runtime.scope():
        ctx = runtime.get_context()
        await step.startup(ctx)
        await asyncio.sleep(0.05)
        await step.shutdown(ctx)
        await step.shutdown(ctx)


@pytest.mark.asyncio
async def test_relay_runs_again_after_a_previous_scope_shut_it_down() -> None:
    # The stop signal must not outlive its startup: a reused event would leave the next
    # loop stopped before its first tick, and the relay would silently never run again.
    step = _drain_step()
    relay_mock = AsyncMock(return_value=_result(0))
    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(MockDepsModule()).freeze())

    with patch.object(OutboxRelay, "to_queue", relay_mock):
        async with runtime.scope():
            ctx = runtime.get_context()

            await step.startup(ctx)
            await asyncio.sleep(0.05)
            await step.shutdown(ctx)
            after_first = relay_mock.await_count

            await step.startup(ctx)
            await asyncio.sleep(0.05)
            await step.shutdown(ctx)

    assert after_first >= 1
    assert relay_mock.await_count > after_first  # the second loop actually ticked


def test_drain_needs_no_ordering_dependency() -> None:
    # It used to: the drain touches the database during teardown, and lifecycle shutdown runs
    # in reverse wave order, so without an edge the pool could close underneath it. The runtime
    # now stops every registered loop *before* teardown begins, so the client is open by
    # construction and the app no longer has to know any of this.
    codec = PydanticModelCodec(_Payload)

    step = outbox_relay_background_lifecycle_step(
        outbox_spec=OutboxSpec(name="events", codec=codec),
        queue_spec=QueueSpec(name="jobs", codec=codec),
        drain_on_shutdown=True,
    )

    assert step.requires == ()


@pytest.mark.asyncio
async def test_the_drain_runs_at_most_once_per_startup() -> None:
    # `stop` is asked twice by design — by the runtime before teardown, and by this step's own
    # shutdown hook. The drain must not be the part that repeats: a second pass would re-claim
    # the rows the first just rescheduled (they return to the head of the claim order once
    # their backoff elapses) and burn a second delivery attempt on each.
    rows = [_outbox_row(index=i) for i in range(2)]
    step = _drain_step(
        limit=2,
        max_attempts=2,  # a second attempt would park these rows `failed`
        retry_base_delay=timedelta(microseconds=1),
        retry_max_backoff=timedelta(seconds=1),
    )

    with patch(_ENQUEUE, AsyncMock(side_effect=RuntimeError("broker down"))):
        await _scope_staging_rows_before_shutdown(step, rows)

    assert [row.attempts for row in rows] == [1, 1]
    assert [row.status for row in rows] == [OutboxStatus.PENDING] * 2


def test_drain_is_rejected_for_a_pubsub_destination() -> None:
    # Pubsub is at-most-once past the broker, so draining while subscribers are going away
    # turns a delayed delivery into a lost one. Leaving the rows pending is strictly safer.
    codec = PydanticModelCodec(_Payload)

    with pytest.raises(CoreException, match="pubsub"):
        outbox_relay_background_lifecycle_step(
            outbox_spec=OutboxSpec(name="events", codec=codec),
            transport="pubsub",
            pubsub_spec=PubSubSpec(name="topic", codec=codec),
            drain_on_shutdown=True,
            requires=("db",),
        )


def test_drain_step_carries_its_ordering_edge() -> None:
    step = _drain_step(requires=("postgres_client",))

    assert step.requires == ("postgres_client",)


# ....................... #
# The quiesce-time flush


@pytest.mark.asyncio
async def test_flush_publishes_rows_with_drain_on_shutdown_off() -> None:
    # The quiesce order is stop → flush: stopping the loop ends its ticks, and with
    # ``drain_on_shutdown`` at its default (off) nothing else would ever move the
    # backlog the sweep then polls. ``flush`` publishes what is claimable regardless
    # of that setting — the pair of asserts is the whole fix.
    startup = _startup()  # drain_on_shutdown defaults off; interval=1h never ticks
    rows = [_outbox_row(index=i) for i in range(3)]
    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(MockDepsModule()).freeze())

    async with runtime.scope():
        ctx = runtime.get_context()
        await startup(ctx)
        await asyncio.sleep(0.02)

        ctx.deps.provide(MockStateDepKey).outbox_rows["events"] = rows
        deadline = asyncio.get_running_loop().time() + 5.0

        await startup.stop(deadline=deadline)
        assert [row.status for row in rows] == [OutboxStatus.PENDING] * 3  # stop alone: frozen

        await startup.flush(deadline=deadline)
        assert [row.status for row in rows] == [OutboxStatus.PUBLISHED] * 3


@pytest.mark.asyncio
async def test_flush_runs_at_most_once_and_shares_the_drain_guard() -> None:
    # The flush and the shutdown drain share the once-per-startup guard: whichever
    # runs first claims it, so a later ``drain_on_shutdown`` teardown cannot re-claim
    # (and burn a second delivery attempt on) rows the flush just rescheduled.
    startup = _startup()
    rows = [_outbox_row(index=0)]
    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(MockDepsModule()).freeze())

    async with runtime.scope():
        ctx = runtime.get_context()
        await startup(ctx)
        await asyncio.sleep(0.02)

        ctx.deps.provide(MockStateDepKey).outbox_rows["events"] = rows
        deadline = asyncio.get_running_loop().time() + 5.0
        await startup.stop(deadline=deadline)
        await startup.flush(deadline=deadline)

        with patch.object(OutboxRelay, "to_queue", AsyncMock()) as second:
            await startup.flush(deadline=deadline)  # guarded: no second claim
            await startup.stop(deadline=deadline)  # nor from a later teardown stop

    second.assert_not_called()


@pytest.mark.asyncio
async def test_flush_skips_a_pubsub_destination() -> None:
    # Pubsub is at-most-once past the broker — the same reason ``drain_on_shutdown``
    # refuses it at wiring. The flush leaves the rows pending; the quiesce plane then
    # reports residual honestly instead of losing a delayed delivery.
    codec = PydanticModelCodec(_Payload)
    startup = _startup(
        transport="pubsub",
        queue_spec=None,
        pubsub_spec=PubSubSpec(name="topic", codec=codec),
    )
    rows = [_outbox_row(index=0)]
    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(MockDepsModule()).freeze())

    async with runtime.scope():
        ctx = runtime.get_context()
        await startup(ctx)
        await asyncio.sleep(0.02)

        ctx.deps.provide(MockStateDepKey).outbox_rows["events"] = rows
        deadline = asyncio.get_running_loop().time() + 5.0
        await startup.stop(deadline=deadline)
        await startup.flush(deadline=deadline)

    assert [row.status for row in rows] == [OutboxStatus.PENDING]


@pytest.mark.asyncio
async def test_a_timed_out_flush_leaves_the_shutdown_drain_armed() -> None:
    # A flush cut by the quiesce budget did NOT drain: were the once-guard claimed
    # anyway, the later drain_on_shutdown teardown would be a silent no-op and the
    # untouched backlog would strand for another process. The retry is safe: rows
    # the cut pass rescheduled sit out their backoff, so a near-immediate second
    # pass claims only what was never attempted.
    startup = _startup(drain_on_shutdown=True)
    rows = [_outbox_row(index=i) for i in range(2)]
    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(MockDepsModule()).freeze())

    async def _stall(*_args: Any, **_kwargs: Any) -> Any:
        await asyncio.sleep(0.5)  # far past the flush deadline below
        return _result(0)

    async with runtime.scope():
        ctx = runtime.get_context()
        await startup(ctx)
        await asyncio.sleep(0.02)

        ctx.deps.provide(MockStateDepKey).outbox_rows["events"] = rows
        clock = asyncio.get_running_loop()

        with patch.object(OutboxRelay, "to_queue", _stall):
            await startup.control.stop(deadline=clock.time() + 1.0)
            await startup.flush(deadline=clock.time() + 0.05)  # cut mid-drain

        assert startup.drained is False  # the guard re-armed
        assert [row.status for row in rows] == [OutboxStatus.PENDING] * 2

        # the teardown drain, with its own fresh budget, retries the remainder
        await startup.stop(deadline=clock.time() + 5.0)

    assert [row.status for row in rows] == [OutboxStatus.PUBLISHED] * 2


@pytest.mark.asyncio
async def test_a_timed_out_stop_drain_leaves_the_second_ask_armed() -> None:
    # The same re-arm on the stop side: a tight first budget (quiesce's stop_all
    # under a short sweep timeout) cuts the drain; the second ask — the step's own
    # shutdown hook, with a fresh budget — must retry instead of skipping.
    startup = _startup(drain_on_shutdown=True, shutdown_drain_timeout=timedelta(seconds=5))
    rows = [_outbox_row(index=0)]
    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(MockDepsModule()).freeze())

    async def _stall(*_args: Any, **_kwargs: Any) -> Any:
        await asyncio.sleep(0.5)
        return _result(0)

    async with runtime.scope():
        ctx = runtime.get_context()
        await startup(ctx)
        await asyncio.sleep(0.02)

        ctx.deps.provide(MockStateDepKey).outbox_rows["events"] = rows
        clock = asyncio.get_running_loop()

        with patch.object(OutboxRelay, "to_queue", _stall):
            assert await startup.stop(deadline=clock.time() + 0.05) is False  # cut

        assert startup.drained is False

        await startup.stop(deadline=clock.time() + 5.0)

    assert [row.status for row in rows] == [OutboxStatus.PUBLISHED]


@pytest.mark.asyncio
async def test_a_cancelled_drain_leaves_the_next_ask_armed() -> None:
    # ``stop_all`` cancels straggler stops when its grace elapses (and a torn
    # quiesce cancels its flush): the cancellation lands inside the drain, which
    # must re-arm the once-guard like the timeout path — holding it would make the
    # teardown's fresh-budget ask a silent no-op and strand the claimable backlog.
    startup = _startup(drain_on_shutdown=True)
    rows = [_outbox_row(index=0)]
    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(MockDepsModule()).freeze())

    entered = asyncio.Event()

    async def _stall(*_args: Any, **_kwargs: Any) -> Any:
        entered.set()
        await asyncio.sleep(30)  # parked until the cancel arrives
        return _result(0)

    async with runtime.scope():
        ctx = runtime.get_context()
        await startup(ctx)
        await asyncio.sleep(0.02)

        ctx.deps.provide(MockStateDepKey).outbox_rows["events"] = rows
        clock = asyncio.get_running_loop()

        with patch.object(OutboxRelay, "to_queue", _stall):
            stopping = asyncio.create_task(startup.stop(deadline=clock.time() + 30.0))
            await entered.wait()  # the drain is mid-batch
            stopping.cancel()

            with pytest.raises(asyncio.CancelledError):
                await stopping

        assert startup.drained is False  # the guard re-armed

        # the teardown ask, with its own fresh budget, retries and drains
        await startup.stop(deadline=clock.time() + 5.0)

    assert [row.status for row in rows] == [OutboxStatus.PUBLISHED]
