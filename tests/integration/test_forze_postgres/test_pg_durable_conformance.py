"""Differential conformance: the mock durable journal behaves like the Postgres one.

Runs one identical durable scenario (enqueue → idempotent re-submit → claim → journal a
step + replay → complete → refuse-reclaim) against the in-memory mock and against real
Postgres, and asserts the observable outcomes are identical — so "passed on the mock" means
"matches the self-hosted engine". JSON-native step results are used so the round-trip is
byte-identical on both (the JSON-projection divergence is documented, not exercised here).

Run **control** gets its own scenario, because it is where a mock is most likely to lie.
Cancellation is fence logic — an unfenced ask, a fenced landing, a `CASE` that transitions
PENDING but not RUNNING — expressed once in Python dict mutation and once in a single
`UPDATE ... RETURNING`. Two implementations of one rule is exactly the shape where reading
the code proves nothing and only running both does.

# covers: DurableRunStorePort.enqueue
# covers: DurableFunctionStepPort.run
# covers: DurableRunAdminPort.request_cancel
# covers: DurableRunStorePort.mark_cancelled
# covers: DurableRunStorePort.refuse_cancel
# covers: DurableRunStorePort.mark_timed_out
"""

from __future__ import annotations

from datetime import timedelta
from typing import cast
from uuid import uuid4

import pytest
from psycopg import sql

from forze.application.contracts.durable.function import (
    DurableRunStorePort,
)
from forze.base.primitives import utcnow
from forze_mock import (
    MockDurableFunctionStepAdapter,
    MockDurableRunStore,
    MockDurableScheduleStore,
    MockState,
)
from forze_postgres.adapters.durable import (
    PostgresDurableFunctionStepAdapter,
    PostgresDurableRunStore,
    PostgresDurableScheduleStore,
)
from forze_postgres.execution.deps.configs import (
    PostgresDurableRunConfig,
    PostgresDurableScheduleConfig,
    PostgresDurableStepConfig,
)
from forze_postgres.kernel.client import PostgresClient
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
    run_tenancy_scenario,
    tenant_provider_for,
)

# ----------------------- #


@pytest.fixture
async def run_table(pg_client: PostgresClient) -> str:
    table = f"durable_run_{uuid4().hex[:8]}"
    await pg_client.execute(
        sql.SQL(
            """
            CREATE TABLE {table} (
                run_id text NOT NULL, name text NOT NULL, status text NOT NULL,
                idempotency_key text, input jsonb, output jsonb, error text,
                tenant_id uuid, attempts integer NOT NULL DEFAULT 0,
                leased_until timestamptz, available_at timestamptz,
                created_at timestamptz NOT NULL, updated_at timestamptz NOT NULL,
                cancel_requested_at timestamptz, cancel_refused_at timestamptz,
                PRIMARY KEY (run_id), UNIQUE (idempotency_key)
            )
            """
        ).format(table=sql.Identifier("public", table))
    )
    return table


@pytest.fixture
async def schedule_table(pg_client: PostgresClient) -> str:
    table = f"durable_schedule_{uuid4().hex[:8]}"
    await pg_client.execute(
        sql.SQL(
            """
            CREATE TABLE {table} (
                schedule_id text NOT NULL, name text NOT NULL, cron text NOT NULL,
                tz text, input jsonb, next_fire_at timestamptz NOT NULL,
                enabled boolean NOT NULL DEFAULT true, tenant_id uuid,
                created_at timestamptz NOT NULL, updated_at timestamptz NOT NULL,
                PRIMARY KEY (schedule_id)
            )
            """
        ).format(table=sql.Identifier("public", table))
    )
    return table


@pytest.fixture
async def step_table(pg_client: PostgresClient) -> str:
    table = f"durable_step_{uuid4().hex[:8]}"
    await pg_client.execute(
        sql.SQL(
            """
            CREATE TABLE {table} (
                run_id text NOT NULL, step_id text NOT NULL, result jsonb NOT NULL,
                tenant_id uuid, created_at timestamptz NOT NULL,
                PRIMARY KEY (run_id, step_id)
            )
            """
        ).format(table=sql.Identifier("public", table))
    )
    return table


class TestDurableMockVsPostgres:
    async def test_mock_matches_postgres_for_the_durable_lifecycle(
        self, pg_client: PostgresClient, run_table: str, step_table: str
    ) -> None:
        mock_state = MockState()
        mock_out = await run_lifecycle_scenario(
            MockDurableRunStore(state=mock_state),
            lambda: MockDurableFunctionStepAdapter(state=mock_state),
        )

        pg_out = await run_lifecycle_scenario(
            PostgresDurableRunStore(
                client=pg_client,
                config=PostgresDurableRunConfig(relation=("public", run_table)),
            ),
            lambda: PostgresDurableFunctionStepAdapter(
                client=pg_client,
                config=PostgresDurableStepConfig(relation=("public", step_table)),
            ),
        )

        # Same observable behavior on both engines — the mock is a faithful stand-in.
        assert mock_out == pg_out
        assert mock_out["step_ran_once"] == 1
        assert mock_out["final_status"] == "completed"

    async def test_mock_matches_postgres_for_list_runs(
        self, pg_client: PostgresClient, run_table: str
    ) -> None:
        mock_out = await run_list_scenario(MockDurableRunStore(state=MockState()))

        pg_out = await run_list_scenario(
            PostgresDurableRunStore(
                client=pg_client,
                config=PostgresDurableRunConfig(relation=("public", run_table)),
            )
        )

        assert mock_out == pg_out
        # Anchor the shared expectation (newest-first, keyset paging, filters).
        assert mock_out["all_names_newest_first"] == ["fn4", "fn3", "fn2", "fn1", "fn0"]
        assert mock_out["page1_names"] == ["fn4", "fn3"]
        assert mock_out["page2_names"] == ["fn2", "fn1"]
        assert mock_out["completed_names"] == ["fn2"]
        assert mock_out["by_name_count"] == 1
        assert mock_out["zero_limit_list"] == "validation"

    async def test_mock_matches_postgres_under_every_forced_race_schedule(
        self, pg_client: PostgresClient, run_table: str
    ) -> None:
        # The races replayed ordering-by-ordering on both engines. Comparing the *winner per
        # ordering* — not just "each side picked something legal" — is what catches a store
        # that resolves a tie the other way: both engines would pass a per-ordering legality
        # check while disagreeing about who won, and DST's mock oracle would then be proving
        # a rule Postgres does not follow.
        def pg_store() -> PostgresDurableRunStore:
            return PostgresDurableRunStore(
                client=pg_client,
                config=PostgresDurableRunConfig(relation=("public", run_table)),
            )

        async def pg_expire(store: DurableRunStorePort, run_id: str) -> None:
            await pg_client.execute(
                sql.SQL(
                    "UPDATE {table} SET leased_until = now() - interval '1 hour' "
                    "WHERE run_id = {run_id}"
                ).format(
                    table=sql.Identifier("public", run_table),
                    run_id=sql.Placeholder("run_id"),
                ),
                {"run_id": run_id},
            )

        async def mock_expire(store: DurableRunStorePort, run_id: str) -> None:
            state = cast(MockDurableRunStore, store).state
            state.durable_runs[run_id]["leased_until"] = utcnow() - timedelta(hours=1)

        mock_state = MockState()

        assert await run_cancel_vs_complete_race(
            lambda: MockDurableRunStore(state=mock_state)
        ) == await run_cancel_vs_complete_race(pg_store)

        assert await run_stale_holder_race(
            lambda: MockDurableRunStore(state=mock_state), mock_expire
        ) == await run_stale_holder_race(pg_store, pg_expire)

    async def test_mock_matches_postgres_for_run_control(
        self, pg_client: PostgresClient, run_table: str
    ) -> None:
        mock_out = await run_control_scenario(MockDurableRunStore(state=MockState()))

        pg_out = await run_control_scenario(
            PostgresDurableRunStore(
                client=pg_client,
                config=PostgresDurableRunConfig(relation=("public", run_table)),
            )
        )

        assert mock_out == pg_out

        # Anchor the shared expectation, so a matching pair of *wrong* answers still fails.
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
        assert mock_out["unfenced_refusal_recorded"] is True

        # The refusal survives being written against an already-terminal run: unlike every
        # other write on this port it is not guarded on RUNNING, and that is exactly the
        # ordering the runner produces (the stamp goes down in a ``finally``).
        assert mock_out["late_refusal_status"] == "completed"
        assert mock_out["late_refusal_stamped"] is True

        assert mock_out["failed_status"] == "failed"
        assert mock_out["failed_error"] == "boom"
        assert mock_out["failed_error_after_stale_write"] == "boom"
        assert mock_out["forward_incomplete_status"] == "forward_incomplete"

        assert mock_out["timed_out_status"] == "timed_out"
        assert mock_out["timed_out_error"] == "cap exceeded"

        assert mock_out["cancelled_names"] == ["cancel-pending", "cancel-running"]
        assert mock_out["timed_out_names"] == ["timed-out"]
        assert mock_out["supports_cancel"] is True

    async def test_mock_matches_postgres_for_the_recovery_scan(
        self, pg_client: PostgresClient, run_table: str
    ) -> None:
        mock_out = await run_claim_scenario(MockDurableRunStore(state=MockState()))

        pg_out = await run_claim_scenario(
            PostgresDurableRunStore(
                client=pg_client,
                config=PostgresDurableRunConfig(relation=("public", run_table)),
            )
        )

        assert mock_out == pg_out

        assert mock_out["first_scan_names"] == ["claimable"]
        assert mock_out["delayed_not_claimed"] is True
        assert mock_out["second_scan_names"] == []
        assert mock_out["holder_still_holds"] is True
        assert mock_out["begin_while_running"] is True
        assert mock_out["zero_limit_scan"] == []

    async def test_mock_matches_postgres_for_the_tenant_boundary(
        self, pg_client: PostgresClient, run_table: str, schedule_table: str
    ) -> None:
        mock_state = MockState()
        mock_out = await run_tenancy_scenario(
            lambda tenant: MockDurableRunStore(
                state=mock_state,
                tenant_provider=tenant_provider_for(tenant),
            ),
            lambda tenant: MockDurableScheduleStore(
                state=mock_state,
                tenant_provider=tenant_provider_for(tenant),
            ),
        )

        pg_out = await run_tenancy_scenario(
            lambda tenant: PostgresDurableRunStore(
                client=pg_client,
                config=PostgresDurableRunConfig(relation=("public", run_table)),
                tenant_provider=tenant_provider_for(tenant),
            ),
            lambda tenant: PostgresDurableScheduleStore(
                client=pg_client,
                config=PostgresDurableScheduleConfig(relation=("public", schedule_table)),
                tenant_provider=tenant_provider_for(tenant),
            ),
        )

        assert mock_out == pg_out

        # A bound tenant reaches none of another's run, on any verb that takes a run id.
        assert (mock_out["cross_load"], mock_out["cross_begin"]) == (True, True)
        assert mock_out["cross_renew"] is False
        assert mock_out["own_begin"] == "running"
        assert mock_out["cross_complete_status"] == "running"
        assert mock_out["cross_complete_output"] is None
        assert mock_out["own_complete_status"] == "completed"
        assert mock_out["own_complete_output"] == {"ok": True}
        assert mock_out["cross_fail_status"] == "running"
        assert mock_out["cross_fail_error"] is None

        # An untagged run is completable from anywhere and readable afterwards — the arm
        # that keeps it out of a reclaim loop, and the seal that makes the arm safe.
        assert mock_out["orphan_begin"] is True
        assert mock_out["orphan_renew"] is True
        assert mock_out["orphan_status"] == "completed"
        assert mock_out["orphan_output"] == {"ok": True}

        # …and invisible to a bound listing. Enumeration matches the tenant exactly.
        assert mock_out["orphan_listed_by_b"] == []

        # One tenant per operation: contradicting the binding refuses, naming a tenant with
        # nothing bound resolves everything — relation included — under it.
        assert mock_out["enqueue_mismatch"] == "authentication"
        assert mock_out["explicit_tenant_reaches_owner"] == "for-b"
        assert mock_out["explicit_tenant_hidden_from_other"] is True
        assert mock_out["schedule_mismatch"] == "authentication"
        assert mock_out["schedule_reaches_owner"] == "fn"
        assert mock_out["schedule_hidden_from_other"] is True

    async def test_mock_matches_postgres_for_schedules(
        self, pg_client: PostgresClient, schedule_table: str
    ) -> None:
        mock_out = await run_schedule_scenario(MockDurableScheduleStore(state=MockState()))

        pg_out = await run_schedule_scenario(
            PostgresDurableScheduleStore(
                client=pg_client,
                config=PostgresDurableScheduleConfig(relation=("public", schedule_table)),
            )
        )

        assert mock_out == pg_out

        assert mock_out["due_ids"] == ["nightly"]
        # One due instant, one advance: the loser is told it advanced nothing.
        assert (mock_out["advanced"], mock_out["advanced_again"]) == (True, False)
        assert mock_out["not_due_after_advance"] == []
        assert mock_out["reput_cron"] == "*/5 * * * *"
        assert mock_out["paused_not_due"] == []
        assert mock_out["zero_limit_due"] == []
        assert (mock_out["deleted"], mock_out["delete_again"]) == (True, False)
        assert mock_out["load_after_delete"] is None
