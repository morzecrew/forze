"""Cancel's two write races, under every forced schedule, against the mock.

The battery itself lives in :mod:`tests.support.durable_cancel_races` because the identical
orderings are replayed against real Postgres in the conformance leg — fence logic is exactly
where a mock is most tempting to believe and least safe to.

# covers: DurableRunStorePort.mark_cancelled
# covers: DurableRunAdminPort.request_cancel
"""

from __future__ import annotations

from datetime import timedelta

from forze.application.contracts.durable.function import DurableRunStorePort
from forze.base.primitives import utcnow
from forze_mock import MockDurableRunStore, MockState
from tests.support.durable_cancel_races import (
    run_cancel_vs_complete_race,
    run_stale_holder_race,
)

# ----------------------- #


async def _expire_lease(store: DurableRunStorePort, run_id: str) -> None:
    state = getattr(store, "state")
    state.durable_runs[run_id]["leased_until"] = utcnow() - timedelta(hours=1)


# ....................... #


class TestCancelRaces:
    async def test_cancel_racing_complete_never_tears_under_any_ordering(self) -> None:
        winners = await run_cancel_vs_complete_race(
            lambda: MockDurableRunStore(state=MockState())
        )

        # All six orderings ran, and each resolved to a single legal terminal (the battery
        # asserts the untorn property per ordering; this pins the coverage claim itself, so
        # a battery that silently stopped enumerating cannot pass quietly).
        assert len(winners) == 6

        # First-terminal-write-wins is the rule, and it is visible in the outcomes: whichever
        # of cancel/complete is applied first is the one that survives.
        assert "ask->cancel->complete=cancelled" in winners
        assert "ask->complete->cancel=completed" in winners
        assert "complete->ask->cancel=completed" in winners

    async def test_a_stale_worker_never_wins_under_any_ordering(self) -> None:
        outcomes = await run_stale_holder_race(
            lambda: MockDurableRunStore(state=MockState()),
            _expire_lease,
        )

        assert len(outcomes) == 24  # every arrival order of the four competing writes
        assert all(o.endswith("=cancelled") for o in outcomes)
