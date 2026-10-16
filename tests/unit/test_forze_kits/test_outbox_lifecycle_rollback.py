"""Unit tests for the background relay on a store whose failed passes roll back.

When a pass runs in one transaction, its failure makes the whole batch claimable again at
once. The step must end that pass instead of running the next batch, and back off over
consecutive failed ticks, or it republishes the batch on every pass.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any
from unittest.mock import AsyncMock, MagicMock, PropertyMock, patch
from uuid import UUID

import pytest
from pydantic import BaseModel

from forze.application.contracts.outbox import OutboxRelayResult, OutboxSpec
from forze.application.contracts.queue import QueueSpec
from forze.application.execution import DepsRegistry, ExecutionRuntime
from forze.application.execution.background import BackgroundLoopControl
from forze.base.exceptions import exc
from forze.base.serialization import PydanticModelCodec
from forze_kits.integrations.outbox import OutboxRelay
from forze_kits.integrations.outbox.lifecycle import _OutboxRelayBackgroundStartup
from forze_mock import MockDepsModule
from forze_mock.adapters.outbox import MockOutboxStore

# ----------------------- #

_T1 = UUID("11111111-1111-1111-1111-111111111111")
_T2 = UUID("22222222-2222-2222-2222-222222222222")
_LOGGER = "forze_kits.integrations.outbox.lifecycle.logger"


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

    async def test_consecutive_failed_ticks_back_off_and_a_committed_tick_resets(self) -> None:
        startup = _startup(passes_roll_back=True)
        boom = RuntimeError("mark failed")
        relay = AsyncMock(side_effect=[boom, boom, boom, _full(0), boom])

        waits = await _sleeps(startup, relay, ticks=5)

        # 1 s, then doubling, capped at retry_max_backoff (3 s); none of them wake-cut.
        assert waits == [
            ("backoff", 1.0),
            ("backoff", 2.0),
            ("backoff", 3.0),
            ("tick", None),
            ("backoff", 1.0),
        ]

    async def test_without_rollback_a_failing_route_keeps_the_ordinary_tick(self) -> None:
        startup = _startup(passes_roll_back=False, max_batches_per_tick=2)
        relay = AsyncMock(side_effect=RuntimeError("mark failed"))

        waits = await _sleeps(startup, relay, ticks=3)

        assert waits == [("tick", None)] * 3

    async def test_a_backoff_never_drops_below_the_interval(self) -> None:
        startup = _startup(
            passes_roll_back=True,
            interval=timedelta(seconds=10),
            retry_max_backoff=timedelta(seconds=1),
        )

        assert startup._failed_tick_backoff(1) == 10.0
        assert startup._failed_tick_backoff(50) == 10.0

    async def test_a_raising_tenant_fails_the_tick_but_the_shard_still_drains(self) -> None:
        startup = _startup(passes_roll_back=True)
        relay = AsyncMock(side_effect=[exc.configuration("tenant key disabled"), _full(0)])
        runtime = _runtime()

        with patch.object(OutboxRelay, "to_queue", relay), patch(_LOGGER, MagicMock()):
            async with runtime.scope():
                tick = await startup._drain_tick(runtime.get_context(), [_T1, _T2])

        assert relay.await_count == 2
        assert tick.failed


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
