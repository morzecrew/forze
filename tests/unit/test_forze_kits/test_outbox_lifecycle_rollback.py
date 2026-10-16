"""Unit tests for the background relay on a store whose failed passes roll back.

When a pass runs in one transaction, its failure makes the whole batch claimable again at
once. The step must end that pass instead of running the next batch, and back off over
consecutive failed ticks, or it republishes the batch on every pass.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import AsyncMock, MagicMock, PropertyMock, patch
from uuid import UUID, uuid4

import pytest
from pydantic import BaseModel

from forze.application.contracts.outbox import OutboxRelayResult, OutboxSpec, OutboxStatus
from forze.application.contracts.queue import QueueSpec
from forze.application.execution import DepsRegistry, ExecutionRuntime
from forze.application.execution.background import BackgroundLoopControl
from forze.base.exceptions import exc
from forze.base.serialization import PydanticModelCodec
from forze_kits.integrations.outbox import OutboxRelay
from forze_kits.integrations.outbox.lifecycle import _OutboxRelayBackgroundStartup, _RelayPass
from forze_mock import MockDepsModule, MockStateDepKey
from forze_mock.adapters import MockState
from forze_mock.adapters.outbox import MockOutboxRow, MockOutboxStore

# ----------------------- #

_T1 = UUID("11111111-1111-1111-1111-111111111111")
_T2 = UUID("22222222-2222-2222-2222-222222222222")
_LOGGER = "forze_kits.integrations.outbox.lifecycle.logger"
_T0 = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)


class _Payload(BaseModel):
    x: int


def _startup(*, passes_roll_back: bool, **overrides: Any) -> _OutboxRelayBackgroundStartup:
    codec = PydanticModelCodec(_Payload)
    kwargs: dict[str, Any] = {
        "outbox_spec": OutboxSpec(name="events", codec=codec),
        "transport": "queue",
        "queue_spec": QueueSpec(name="jobs", codec=codec),
        "stream_spec": None,
        "pubsub_spec": None,
        "interval": timedelta(seconds=1),
        "jitter": 0.0,
        "reclaim_stale_after": None,
        "limit": 10,
        "max_attempts": 5,
        "retry_base_delay": timedelta(seconds=1),
        "retry_max_backoff": timedelta(seconds=3),
        "max_batches_per_tick": 100,
    }
    kwargs.update(overrides)
    startup = _OutboxRelayBackgroundStartup(**kwargs)
    startup.passes_roll_back = passes_roll_back
    return startup


def _seed(state: MockState, count: int) -> None:
    state.outbox_rows.setdefault("events", []).extend(
        MockOutboxRow(
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
        )
        for index in range(count)
    )


def _runtime() -> ExecutionRuntime:
    return ExecutionRuntime(deps=DepsRegistry.from_modules(MockDepsModule()).freeze())


def _full(claimed: int = 10) -> OutboxRelayResult:
    return OutboxRelayResult(claimed=claimed, published=claimed)


async def _sleeps(
    startup: _OutboxRelayBackgroundStartup, relay: AsyncMock, *, ticks: int
) -> list[tuple[str, float | None]]:
    """Run the loop for *ticks* ticks and return how it waited after each one.

    ``("backoff", delay)`` is the wake-blind wait after a failed tick; ``("tick", None)`` is
    the ordinary wait that a wake cuts short.
    """

    waits: list[tuple[str, float | None]] = []

    async def backoff(_control: Any, delay: float) -> bool:
        waits.append(("backoff", delay))
        return len(waits) >= ticks

    async def tick(_self: Any) -> bool:
        waits.append(("tick", None))
        return len(waits) >= ticks

    with (
        patch.object(OutboxRelay, "to_queue", relay),
        patch.object(BackgroundLoopControl, "sleep_or_stop", backoff),
        patch.object(_OutboxRelayBackgroundStartup, "_sleep_or_stop", tick),
        patch(_LOGGER, MagicMock()),
    ):
        runtime = _runtime()

        async with runtime.scope():
            await startup._loop(runtime.get_context())

    return waits


# ....................... #


class TestRollingBackPassFailure:
    async def test_a_failed_batch_ends_the_pass_instead_of_reclaiming_the_batch(self) -> None:
        startup = _startup(passes_roll_back=True)
        relay = AsyncMock(side_effect=RuntimeError("mark failed"))
        runtime = _runtime()

        with patch.object(OutboxRelay, "to_queue", relay), patch(_LOGGER, MagicMock()):
            async with runtime.scope():
                passed = await startup._relay_once(runtime.get_context())

        assert relay.await_count == 1
        assert passed.stop == "error"

    async def test_without_rollback_a_failed_batch_still_runs_the_next(self) -> None:
        startup = _startup(passes_roll_back=False, max_batches_per_tick=3)
        relay = AsyncMock(side_effect=RuntimeError("mark failed"))
        runtime = _runtime()

        with patch.object(OutboxRelay, "to_queue", relay), patch(_LOGGER, MagicMock()):
            async with runtime.scope():
                passed = await startup._relay_once(runtime.get_context())

        assert relay.await_count == 3
        assert passed.stop == "cap"

    async def test_without_rollback_a_failing_route_keeps_the_ordinary_tick(self) -> None:
        startup = _startup(passes_roll_back=False, max_batches_per_tick=2)
        relay = AsyncMock(side_effect=RuntimeError("mark failed"))

        waits = await _sleeps(startup, relay, ticks=3)

        assert waits == [("tick", None)] * 3
        assert relay.await_count == 6
        assert startup.backoffs == {}


# ....................... #


class TestPerTenantBackoff:
    async def test_a_failing_tenant_backs_off_alone_while_the_shard_keeps_its_cadence(
        self,
    ) -> None:
        # T1's key is disabled for good; T2 is healthy. The ticks come back to back, as a
        # storm of wakes would bring them.
        startup = _startup(passes_roll_back=True)
        startup.tenant_shard = [_T1, _T2]
        attempts: dict[UUID, int] = {_T1: 0, _T2: 0}

        async def once(_self: Any, ctx: Any, **_: Any) -> _RelayPass:
            tenant = ctx.inv_ctx.get_tenant().tenant_id
            attempts[tenant] += 1

            if tenant == _T1:
                raise exc.configuration("tenant key disabled")

            return _RelayPass(stop="drained")

        with patch.object(_OutboxRelayBackgroundStartup, "_relay_once", once):
            waits = await _sleeps(startup, AsyncMock(), ticks=6)

        assert waits == [("tick", None)] * 6  # the loop never waits on T1's behalf
        assert attempts == {_T1: 1, _T2: 6}  # T1 sits out its backoff, wakes or not
        assert startup.backoffs[_T1][0] == 1
        assert _T2 not in startup.backoffs

    async def test_a_tenant_due_again_is_retried_and_its_wait_doubles_to_the_cap(self) -> None:
        startup = _startup(passes_roll_back=True)
        boom = RuntimeError("mark failed")
        relay = AsyncMock(side_effect=[boom, boom, boom, _full(0)])
        runtime = _runtime()
        waits: list[float] = []

        with patch.object(OutboxRelay, "to_queue", relay), patch(_LOGGER, MagicMock()):
            async with runtime.scope():
                ctx = runtime.get_context()

                for _ in range(4):
                    await startup._drain_tick(ctx, None)

                    if None in startup.backoffs:
                        failures, retry_at = startup.backoffs[None]
                        waits.append(round(retry_at - asyncio.get_running_loop().time(), 1))
                        # Let it fall due without waiting it out.
                        startup.backoffs[None] = (failures, 0.0)

        # interval 1 s, doubling, capped at retry_max_backoff (3 s); then a pass commits.
        assert waits == [1.0, 2.0, 3.0]
        assert relay.await_count == 4
        assert startup.backoffs == {}

    @pytest.mark.parametrize("failures", [1, 50])
    async def test_a_wait_never_drops_below_the_interval_less_its_jitter(
        self, failures: int
    ) -> None:
        startup = _startup(
            passes_roll_back=True,
            interval=timedelta(seconds=10),
            jitter=0.2,
            retry_max_backoff=timedelta(seconds=1),
        )
        startup.backoffs[_T1] = (failures - 1, 0.0)
        before = asyncio.get_running_loop().time()

        startup._note_pass(_T1, failed=True)

        # Shortened by the jitter, so the first tick at or after 10 s runs the pass.
        assert 8.0 <= startup.backoffs[_T1][1] - before <= 8.1

    async def test_a_raising_tenant_backs_off_but_the_shard_still_drains(self) -> None:
        startup = _startup(passes_roll_back=True)
        relay = AsyncMock(side_effect=[exc.configuration("tenant key disabled"), _full(0)])
        runtime = _runtime()

        with patch.object(OutboxRelay, "to_queue", relay), patch(_LOGGER, MagicMock()):
            async with runtime.scope():
                await startup._drain_tick(runtime.get_context(), [_T1, _T2])

        assert relay.await_count == 2
        assert list(startup.backoffs) == [_T1]


# ....................... #


class TestRollingBackDrain:
    @pytest.mark.parametrize("roll_back", [True, False])
    async def test_a_drain_keeps_a_fifth_of_its_budget_to_commit_when_passes_roll_back(
        self, roll_back: bool
    ) -> None:
        startup = _startup(passes_roll_back=roll_back, shutdown_drain_timeout=timedelta(seconds=5))
        seen: list[float | None] = []

        async def capture(self: OutboxRelay, *_: Any, **__: Any) -> OutboxRelayResult:
            seen.append(self.publish_until)
            return _full(0)

        runtime = _runtime()

        with patch.object(OutboxRelay, "to_queue", autospec=True, side_effect=capture):
            async with runtime.scope():
                deadline = asyncio.get_running_loop().time() + 5
                await startup._relay_once(
                    runtime.get_context(), strict=True, reclaim=False, deadline=deadline
                )

        # A retain batch cut early would strand its claimed rows, so it keeps today's bound.
        if roll_back:
            [publish_until] = seen
            assert publish_until == pytest.approx(deadline - 1.0, abs=0.05)
        else:
            assert seen == [None]

    async def test_a_drain_left_with_little_budget_still_publishes(self) -> None:
        # The runtime left half a second of a 5 s drain setting, and the destination is fast:
        # the share kept back must come out of the half second, not out of the 5 s.
        startup = _startup(passes_roll_back=True, shutdown_drain_timeout=timedelta(seconds=5))
        runtime = _runtime()

        async with runtime.scope():
            state = runtime.get_context().deps.provide(MockStateDepKey)
            _seed(state, 3)
            passed = await startup._relay_once(
                runtime.get_context(),
                strict=True,
                reclaim=False,
                deadline=asyncio.get_running_loop().time() + 0.5,
            )

        assert passed.published == 3

    async def test_a_drain_opens_no_batch_once_publishing_is_over(self) -> None:
        # A batch opened past the cutoff can publish nothing, yet claims a full batch, so the
        # drain would lock and release the same pending rows until its cap or deadline.
        startup = _startup(passes_roll_back=True, max_batches_per_tick=100)
        starts: list[float] = []
        cutoffs: list[float | None] = []

        async def batch(self: OutboxRelay, *_: Any, **__: Any) -> OutboxRelayResult:
            starts.append(asyncio.get_running_loop().time())
            cutoffs.append(self.publish_until)
            await asyncio.sleep(0.05)
            return _full(10)  # a backlog: every claim comes back full

        runtime = _runtime()

        with patch.object(OutboxRelay, "to_queue", autospec=True, side_effect=batch):
            async with runtime.scope():
                passed = await startup._relay_once(
                    runtime.get_context(),
                    strict=True,
                    reclaim=False,
                    deadline=asyncio.get_running_loop().time() + 1.0,
                )

        cutoff = cutoffs[0]
        assert cutoff is not None
        assert [start for start in starts if start >= cutoff] == []
        assert passed.stop == "budget"


# ....................... #


class TestRollbackModeAtStartup:
    @pytest.mark.parametrize("transactional", [True, False])
    async def test_the_step_reads_the_mode_from_the_store(self, transactional: bool) -> None:
        startup = _startup(passes_roll_back=not transactional, interval=timedelta(hours=1))
        runtime = _runtime()

        with (
            patch.object(
                MockOutboxStore,
                "transactional_passes",
                new_callable=PropertyMock,
                return_value=transactional,
            ),
            patch.object(OutboxRelay, "to_queue", AsyncMock(return_value=_full(0))),
        ):
            async with runtime.scope():
                ctx = runtime.get_context()
                await startup(ctx)
                await startup.stop(deadline=float("inf"))

        assert startup.passes_roll_back is transactional

    async def test_a_truthy_stand_in_is_not_taken_for_rolling_back(self) -> None:
        # A MagicMock(spec=OutboxQueryPort) answers the attribute with a truthy MagicMock.
        startup = _startup(passes_roll_back=True, interval=timedelta(hours=1))
        runtime = _runtime()

        with (
            patch.object(
                MockOutboxStore,
                "transactional_passes",
                new_callable=PropertyMock,
                return_value=MagicMock(),
            ),
            patch.object(OutboxRelay, "to_queue", AsyncMock(return_value=_full(0))),
        ):
            async with runtime.scope():
                await startup(runtime.get_context())
                await startup.stop(deadline=float("inf"))

        assert startup.passes_roll_back is False
