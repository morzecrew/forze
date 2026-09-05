"""Differential conformance: the mock durable journal behaves like the Mongo one.

The same three scenarios the Postgres leg runs (`tests/support/durable_conformance`), driven
against real Mongo and against the in-memory mock, asserting identical observable outcomes —
so "passed on the mock" means "matches this engine too", and the oracle DST simulates
against is not quietly modelling a store nobody deploys.

Mongo is where the comparison earns the most. Postgres expresses claim-and-fence in SQL the
database enforces for it — `FOR UPDATE SKIP LOCKED`, one `UPDATE … CASE` for the whole
cancel transition. Mongo has neither, so the same rules are re-expressed as a candidate
read plus a token-stamped `update_many`, and as two ordered statements. Two engines, one
rule, written twice: reading the code proves nothing here and only running both does.

# covers: DurableRunStorePort.enqueue
# covers: DurableRunStorePort.begin
# covers: DurableRunStorePort.claim_abandoned
# covers: DurableRunStorePort.renew
# covers: DurableFunctionStepPort.run
# covers: DurableRunAdminPort.request_cancel
# covers: DurableRunAdminPort.list_runs
# covers: DurableRunStorePort.mark_cancelled
# covers: DurableRunStorePort.refuse_cancel
# covers: DurableRunStorePort.mark_timed_out
"""

from __future__ import annotations

from datetime import timedelta
from typing import cast
from uuid import uuid4

import pytest
import pytest_asyncio

from forze.application.contracts.durable.function import DurableRunStorePort
from forze.base.primitives import utcnow
from forze_mock import (
    MockDurableFunctionStepAdapter,
    MockDurableRunStore,
    MockDurableScheduleStore,
    MockState,
)
from forze_mongo.adapters.durable import (
    MongoDurableFunctionStepAdapter,
    MongoDurableRunStore,
    MongoDurableScheduleStore,
)
from forze_mongo.execution.deps.configs import (
    MongoDurableRunConfig,
    MongoDurableScheduleConfig,
    MongoDurableStepConfig,
)
from forze_mongo.kernel.client import MongoClient
from tests.support.durable_cancel_races import (
    run_cancel_vs_complete_race,
    run_stale_holder_race,
)
from tests.support.durable_conformance import (
    run_claim_scenario,
    run_control_scenario,
    run_lifecycle_scenario,
    run_list_scenario,
    run_schedule_scenario,
)

# ----------------------- #

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


@pytest_asyncio.fixture
async def run_collection(mongo_client: MongoClient) -> tuple[str, str]:
    """A run collection carrying the one index the store needs for correctness.

    The partial unique index on ``idempotency_key`` is what makes two simultaneous enqueues
    of one key converge on a single run; partial because a run without a key must not
    collide with every other keyless run.
    """

    db_name = (await mongo_client.db()).name
    name = f"durable_run_{uuid4().hex[:8]}"
    coll = await mongo_client.collection(name, db_name=db_name)
    await coll.create_index(
        [("idempotency_key", 1)],
        unique=True,
        partialFilterExpression={"idempotency_key": {"$type": "string"}},
    )

    return db_name, name


@pytest_asyncio.fixture
async def step_collection(mongo_client: MongoClient) -> tuple[str, str]:
    """A step journal, which needs no index: its dedup key is the document ``_id``."""

    db_name = (await mongo_client.db()).name

    return db_name, f"durable_step_{uuid4().hex[:8]}"


def _mongo_store(client: MongoClient, collection: tuple[str, str]) -> MongoDurableRunStore:
    return MongoDurableRunStore(
        client=client,
        config=MongoDurableRunConfig(collection=collection),
    )


# ....................... #


class TestDurableMockVsMongo:
    async def test_mock_matches_mongo_for_the_durable_lifecycle(
        self,
        mongo_client: MongoClient,
        run_collection: tuple[str, str],
        step_collection: tuple[str, str],
    ) -> None:
        mock_state = MockState()
        mock_out = await run_lifecycle_scenario(
            MockDurableRunStore(state=mock_state),
            lambda: MockDurableFunctionStepAdapter(state=mock_state),
        )

        mongo_out = await run_lifecycle_scenario(
            _mongo_store(mongo_client, run_collection),
            lambda: MongoDurableFunctionStepAdapter(
                client=mongo_client,
                config=MongoDurableStepConfig(collection=step_collection),
            ),
        )

        assert mock_out == mongo_out
        assert mock_out["step_ran_once"] == 1
        assert mock_out["final_status"] == "completed"

    async def test_mock_matches_mongo_for_list_runs(
        self, mongo_client: MongoClient, run_collection: tuple[str, str]
    ) -> None:
        mock_out = await run_list_scenario(MockDurableRunStore(state=MockState()))
        mongo_out = await run_list_scenario(_mongo_store(mongo_client, run_collection))

        assert mock_out == mongo_out
        # Anchor the shared expectation (newest-first, keyset paging, filters), so a matching
        # pair of wrong answers still fails.
        assert mock_out["all_names_newest_first"] == ["fn4", "fn3", "fn2", "fn1", "fn0"]
        assert mock_out["page1_names"] == ["fn4", "fn3"]
        assert mock_out["page2_names"] == ["fn2", "fn1"]
        assert mock_out["completed_names"] == ["fn2"]
        assert mock_out["by_name_count"] == 1

    async def test_mock_matches_mongo_for_run_control(
        self, mongo_client: MongoClient, run_collection: tuple[str, str]
    ) -> None:
        mock_out = await run_control_scenario(MockDurableRunStore(state=MockState()))
        mongo_out = await run_control_scenario(_mongo_store(mongo_client, run_collection))

        assert mock_out == mongo_out

        assert mock_out["pending_ask"] is True
        assert mock_out["pending_status"] == "cancelled"
        assert mock_out["pending_not_reclaimed"] is True
        assert mock_out["terminal_ask"] is False

        assert mock_out["running_ask"] is True
        assert mock_out["running_ask_again"] is True
        assert mock_out["running_still_running"] is True
        assert mock_out["ask_instant_is_stable"] is True
        assert (mock_out["renewal_held"], mock_out["renewal_carries_cancel"]) == (True, True)

        # The fence is the whole safety story on more than one replica.
        assert mock_out["stale_renewal_held"] is False
        assert mock_out["stale_renewal_cancel"] is False
        assert mock_out["stale_landing_ignored"] is True
        assert mock_out["stale_refusal_ignored"] is True

        assert mock_out["running_final_status"] == "cancelled"
        assert mock_out["late_complete_status"] == "cancelled"
        assert mock_out["late_complete_output"] is None

        assert mock_out["refused_status"] == "completed"
        assert mock_out["refused_while_running"] is True
        assert (mock_out["refused_asked"], mock_out["refused_stamped"]) == (True, True)

        # Not guarded on RUNNING, unlike every other write on this port — and that is the
        # ordering the runner produces, since the stamp goes down in a ``finally``.
        assert mock_out["late_refusal_status"] == "completed"
        assert mock_out["late_refusal_stamped"] is True

        assert mock_out["timed_out_status"] == "timed_out"
        assert mock_out["timed_out_error"] == "cap exceeded"

        assert mock_out["cancelled_names"] == ["cancel-pending", "cancel-running"]
        assert mock_out["timed_out_names"] == ["timed-out"]
        assert mock_out["supports_cancel"] is True

    async def test_mock_matches_mongo_under_every_forced_race_schedule(
        self, mongo_client: MongoClient, run_collection: tuple[str, str]
    ) -> None:
        # Comparing the *winner per ordering* — not merely that each side picked something
        # legal — is what catches a store resolving a tie the other way: both engines pass a
        # per-ordering legality check while disagreeing about who won, and the mock oracle
        # would then be proving a rule Mongo does not follow.
        db_name, coll_name = run_collection

        async def mongo_expire(store: DurableRunStorePort, run_id: str) -> None:
            coll = await mongo_client.collection(coll_name, db_name=db_name)
            await mongo_client.update_one(
                coll,
                {"_id": run_id},
                {"$set": {"leased_until": utcnow() - timedelta(hours=1)}},
            )

        async def mock_expire(store: DurableRunStorePort, run_id: str) -> None:
            state = cast(MockDurableRunStore, store).state
            state.durable_runs[run_id]["leased_until"] = utcnow() - timedelta(hours=1)

        mock_state = MockState()

        assert await run_cancel_vs_complete_race(
            lambda: MockDurableRunStore(state=mock_state)
        ) == await run_cancel_vs_complete_race(
            lambda: _mongo_store(mongo_client, run_collection)
        )

        assert await run_stale_holder_race(
            lambda: MockDurableRunStore(state=mock_state), mock_expire
        ) == await run_stale_holder_race(
            lambda: _mongo_store(mongo_client, run_collection), mongo_expire
        )

    async def test_mock_matches_mongo_for_the_recovery_scan(
        self, mongo_client: MongoClient, run_collection: tuple[str, str]
    ) -> None:
        mock_out = await run_claim_scenario(MockDurableRunStore(state=MockState()))
        mongo_out = await run_claim_scenario(_mongo_store(mongo_client, run_collection))

        assert mock_out == mongo_out

        # Anchors, because "both scanners took the same run" is a pair of matching wrong
        # answers away from passing: the second scan must come back empty while the first
        # holder's lease is live, and the holder's fence must still be the one it was given.
        assert mock_out["first_scan_names"] == ["claimable"]
        assert mock_out["delayed_not_claimed"] is True
        assert mock_out["second_scan_names"] == []
        assert mock_out["holder_still_holds"] is True
        assert mock_out["begin_while_running"] is True
        assert mock_out["zero_limit_scan"] == []

    async def test_mock_matches_mongo_for_schedules(self, mongo_client: MongoClient) -> None:
        db_name = (await mongo_client.db()).name
        collection = (db_name, f"durable_schedule_{uuid4().hex[:8]}")

        mock_out = await run_schedule_scenario(MockDurableScheduleStore(state=MockState()))
        mongo_out = await run_schedule_scenario(
            MongoDurableScheduleStore(
                client=mongo_client,
                config=MongoDurableScheduleConfig(collection=collection),
            )
        )

        assert mock_out == mongo_out

        assert mock_out["due_ids"] == ["nightly"]
        # One due instant, one advance: the loser is told it advanced nothing.
        assert (mock_out["advanced"], mock_out["advanced_again"]) == (True, False)
        assert mock_out["not_due_after_advance"] == []
        assert mock_out["reput_cron"] == "*/5 * * * *"
        assert mock_out["paused_not_due"] == []
        assert (mock_out["deleted"], mock_out["delete_again"]) == (True, False)
        assert mock_out["zero_limit_due"] == []
        assert mock_out["load_after_delete"] is None
