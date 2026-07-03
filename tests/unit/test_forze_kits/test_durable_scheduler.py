"""P7 recurring schedules: cron next-fire (skip-missed) + the durable scheduler.

# covers: DurableScheduler.put
# covers: DurableScheduler.remove
# covers: DurableScheduler.tick
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from forze.application.integrations.durable import next_cron_fire, validate_cron
from forze.base.exceptions import CoreException
from tests.support.execution_context import context_from_modules

from forze_kits.integrations.durable import (
    DurableScheduler,
    resolve_durable_run_store,
    resolve_durable_schedule_store,
)
from forze_mock import MockDepsModule, MockState

# ----------------------- #

UTC = timezone.utc


def _runs_for(state: MockState, name: str) -> list[dict]:
    return [d for d in state.durable_runs.values() if d["name"] == name]


# ....................... #


class TestCronNextFire:
    def test_next_fire_is_first_occurrence_strictly_after(self) -> None:
        got = next_cron_fire("*/5 * * * *", after=datetime(2026, 1, 1, 0, 2, tzinfo=UTC))
        assert got == datetime(2026, 1, 1, 0, 5, tzinfo=UTC)

    def test_skip_missed_jumps_to_first_future_occurrence(self) -> None:
        # Daily 03:00; a very late base lands on the next future 03:00, not a backfill.
        got = next_cron_fire("0 3 * * *", after=datetime(2026, 1, 5, 12, tzinfo=UTC))
        assert got == datetime(2026, 1, 6, 3, tzinfo=UTC)

    def test_timezone_is_evaluated_then_normalised_to_utc(self) -> None:
        # 03:00 in Moscow (UTC+3) = 00:00 UTC; strictly after 00:00 UTC → next day.
        got = next_cron_fire(
            "0 3 * * *", after=datetime(2026, 1, 1, 0, 0, tzinfo=UTC), tz="Europe/Moscow"
        )
        assert got == datetime(2026, 1, 2, 0, 0, tzinfo=UTC)

    def test_validate_rejects_bad_expression_and_tz(self) -> None:
        with pytest.raises(CoreException, match="cron"):
            validate_cron("not a cron")

        with pytest.raises(CoreException, match="timezone"):
            validate_cron("* * * * *", tz="Not/AZone")

    def test_rejects_a_naive_after(self) -> None:
        # A naive datetime would be read in the host timezone (environment-dependent).
        with pytest.raises(CoreException, match="timezone-aware"):
            next_cron_fire("* * * * *", after=datetime(2026, 1, 1, 0, 0))


class TestDurableScheduler:
    async def test_put_computes_first_fire(self) -> None:
        ctx = context_from_modules(MockDepsModule())
        record = await DurableScheduler().put(
            ctx,
            "every-minute",
            "fn",
            "* * * * *",
            now=datetime(2026, 1, 1, 0, 0, 30, tzinfo=UTC),
        )
        assert record.next_fire_at == datetime(2026, 1, 1, 0, 1, tzinfo=UTC)

    async def test_remove_unregisters_a_schedule(self) -> None:
        state = MockState()
        ctx = context_from_modules(MockDepsModule(state=state))
        scheduler = DurableScheduler()
        put_at = datetime(2026, 1, 1, 0, 0, 30, tzinfo=UTC)

        await scheduler.put(ctx, "s", "fn", "* * * * *", now=put_at)

        assert await scheduler.remove(ctx, "s") is True  # removed
        assert await resolve_durable_schedule_store(ctx).load("s") is None
        assert await scheduler.remove(ctx, "s") is False  # idempotent no-op

        # A removed schedule never fires again.
        assert await scheduler.tick(ctx, now=datetime(2026, 1, 1, 0, 5, tzinfo=UTC)) == 0
        assert _runs_for(state, "fn") == []

    async def test_tick_fires_a_due_schedule_and_advances(self) -> None:
        state = MockState()
        ctx = context_from_modules(MockDepsModule(state=state))
        scheduler = DurableScheduler()

        await scheduler.put(
            ctx,
            "s",
            "fn",
            "* * * * *",
            input_json={"k": 1},
            now=datetime(2026, 1, 1, 0, 0, 30, tzinfo=UTC),
        )

        # Before due → nothing fires.
        assert await scheduler.tick(ctx, now=datetime(2026, 1, 1, 0, 0, 45, tzinfo=UTC)) == 0
        assert _runs_for(state, "fn") == []

        # At/after due → one run enqueued, schedule advanced to the next occurrence.
        assert await scheduler.tick(ctx, now=datetime(2026, 1, 1, 0, 1, 5, tzinfo=UTC)) == 1

        runs = _runs_for(state, "fn")
        assert len(runs) == 1
        assert runs[0]["input"] == {"k": 1}

        reloaded = await resolve_durable_schedule_store(ctx).load("s")
        assert reloaded is not None
        assert reloaded.next_fire_at == datetime(2026, 1, 1, 0, 2, tzinfo=UTC)

    async def test_skip_missed_fires_once_no_backfill(self) -> None:
        state = MockState()
        ctx = context_from_modules(MockDepsModule(state=state))
        scheduler = DurableScheduler()

        await scheduler.put(
            ctx, "daily", "fn", "0 3 * * *", now=datetime(2026, 1, 1, 2, tzinfo=UTC)
        )

        # Wake 4 days late (4 fires missed) → fire ONCE, advance to the next future 03:00.
        fired = await scheduler.tick(ctx, now=datetime(2026, 1, 5, 12, tzinfo=UTC))
        assert fired == 1
        assert len(_runs_for(state, "fn")) == 1

        reloaded = await resolve_durable_schedule_store(ctx).load("daily")
        assert reloaded is not None
        assert reloaded.next_fire_at == datetime(2026, 1, 6, 3, tzinfo=UTC)

    async def test_second_tick_does_not_refire(self) -> None:
        state = MockState()
        ctx = context_from_modules(MockDepsModule(state=state))
        scheduler = DurableScheduler()

        await scheduler.put(
            ctx, "s", "fn", "* * * * *", now=datetime(2026, 1, 1, 0, 0, 30, tzinfo=UTC)
        )

        due = datetime(2026, 1, 1, 0, 1, 5, tzinfo=UTC)
        assert await scheduler.tick(ctx, now=due) == 1
        assert await scheduler.tick(ctx, now=due) == 0  # already advanced past `due`
        assert len(_runs_for(state, "fn")) == 1

    async def test_fire_is_idempotent_for_the_same_instant(self) -> None:
        # The run key is {schedule_id}:{fire_epoch}; re-enqueuing the same fired instant
        # converges on one run (the crash-between-enqueue-and-advance safety net).
        state = MockState()
        ctx = context_from_modules(MockDepsModule(state=state))
        scheduler = DurableScheduler()
        store = resolve_durable_run_store(ctx)

        record = await scheduler.put(
            ctx, "s", "fn", "* * * * *", now=datetime(2026, 1, 1, 0, 0, 30, tzinfo=UTC)
        )
        fire_epoch = int(record.next_fire_at.timestamp())

        # Pre-enqueue the exact run the next tick will produce.
        await store.enqueue("fn", input_json=None, idempotency_key=f"s:{fire_epoch}")
        await scheduler.tick(ctx, now=datetime(2026, 1, 1, 0, 1, 5, tzinfo=UTC))

        assert len(_runs_for(state, "fn")) == 1  # converged, not duplicated

    async def test_disabled_schedule_is_not_fired(self) -> None:
        state = MockState()
        ctx = context_from_modules(MockDepsModule(state=state))
        scheduler = DurableScheduler()

        await scheduler.put(
            ctx,
            "off",
            "fn",
            "* * * * *",
            enabled=False,
            now=datetime(2026, 1, 1, 0, 0, 30, tzinfo=UTC),
        )

        fired = await scheduler.tick(ctx, now=datetime(2026, 1, 1, 1, tzinfo=UTC))
        assert fired == 0
        assert _runs_for(state, "fn") == []

    async def test_invalid_cron_rejected_at_put(self) -> None:
        ctx = context_from_modules(MockDepsModule())
        with pytest.raises(CoreException, match="cron"):
            await DurableScheduler().put(
                ctx, "bad", "fn", "nonsense", now=datetime(2026, 1, 1, tzinfo=UTC)
            )
