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

from collections.abc import Callable
from datetime import timedelta
from typing import Any, cast
from uuid import uuid4

import pytest
from psycopg import sql

from forze.application.contracts.durable.function import (
    DurableFunctionStepPort,
    DurableRunAdminPort,
    DurableRunContext,
    DurableRunStatus,
    DurableRunStorePort,
    bind_durable_run,
    durable_run_control_capabilities,
    reset_durable_run,
)
from forze.base.primitives import utcnow
from forze_mock import MockDurableFunctionStepAdapter, MockDurableRunStore, MockState
from forze_postgres.adapters.durable import (
    PostgresDurableFunctionStepAdapter,
    PostgresDurableRunStore,
)
from forze_postgres.execution.deps.configs import (
    PostgresDurableRunConfig,
    PostgresDurableStepConfig,
)
from forze_postgres.kernel.client import PostgresClient
from tests.support.durable_cancel_races import (
    run_cancel_vs_complete_race,
    run_stale_holder_race,
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


async def _scenario(
    store: DurableRunStorePort,
    step_of: Callable[[], DurableFunctionStepPort],
) -> dict[str, Any]:
    """Drive one durable lifecycle and collect the observable outcomes."""

    out: dict[str, Any] = {}

    first = await store.enqueue("fn", input_json={"n": 1}, idempotency_key="k")
    out["enqueue_status"] = first.status.value

    resubmit = await store.enqueue("fn", input_json={"n": 2}, idempotency_key="k")
    out["idempotent_same_run"] = first.run_id == resubmit.run_id
    out["idempotent_keeps_original_input"] = resubmit.input_json == {"n": 1}

    claimed = await store.begin(first.run_id, lease_for=timedelta(minutes=5))
    out["claimed_status"] = None if claimed is None else claimed.status.value
    out["claimed_attempts"] = None if claimed is None else claimed.attempts
    out["reclaim_while_running"] = (
        await store.begin(first.run_id, lease_for=timedelta(minutes=5)) is None
    )

    calls: list[int] = []
    token = bind_durable_run(DurableRunContext(run_id=first.run_id, name="fn"))
    try:
        step = step_of()

        async def work() -> dict:
            calls.append(1)
            return {"v": 42}

        out["step_result"] = await step.run("s1", work)
        out["step_replay"] = await step.run("s1", work)
    finally:
        reset_durable_run(token)

    out["step_ran_once"] = len(calls)

    await store.complete(first.run_id, output_json={"done": True})
    loaded = await store.load(first.run_id)
    out["final_status"] = None if loaded is None else loaded.status.value
    out["final_output"] = None if loaded is None else loaded.output_json

    abandoned = await store.claim_abandoned(limit=10, lease_for=timedelta(minutes=5))
    out["completed_not_reclaimed"] = first.run_id not in {a.run_id for a in abandoned}

    return out


# ....................... #


async def _list_scenario(store: DurableRunStorePort) -> dict[str, Any]:
    """Drive `list_runs` and collect plane-independent observables (names / counts).

    Run ids and timestamps differ between the two engines (each generates its own), so the
    outcomes compared are the *names* in newest-first order, page shapes, and filter counts —
    all identical across a faithful stand-in.
    """

    admin = cast(DurableRunAdminPort, store)

    records = [await store.enqueue(f"fn{i}", input_json={"i": i}) for i in range(5)]

    # Complete the middle run so the status filter has something to select.
    mid = records[2]
    await store.begin(mid.run_id, lease_for=timedelta(minutes=5))
    await store.complete(mid.run_id, output_json={"ok": True})

    out: dict[str, Any] = {}

    full = await admin.list_runs(limit=10)
    out["all_names_newest_first"] = [r.name for r in full.records]
    out["all_next_cursor_is_none"] = full.next_cursor is None
    out["created_at_populated"] = all(r.created_at is not None for r in full.records)

    page1 = await admin.list_runs(limit=2)
    out["page1_names"] = [r.name for r in page1.records]
    out["page1_has_cursor"] = page1.next_cursor is not None

    page2 = await admin.list_runs(limit=2, cursor=page1.next_cursor)
    out["page2_names"] = [r.name for r in page2.records]

    completed = await admin.list_runs(status=DurableRunStatus.COMPLETED, limit=10)
    out["completed_names"] = [r.name for r in completed.records]

    by_name = await admin.list_runs(name="fn0", limit=10)
    out["by_name_count"] = len(by_name.records)

    return out


# ....................... #


async def _cancel_scenario(store: DurableRunStorePort) -> dict[str, Any]:
    """Drive the full run-control battery and collect plane-independent observables.

    Timestamps and run ids differ per engine, so what is compared is the *shape* of every
    decision: which asks were accepted, which landings took effect, which were refused by
    the fence, and which stamps ended up set.
    """

    admin = cast(DurableRunAdminPort, store)
    out: dict[str, Any] = {}

    # 1. A PENDING run stops at once, and the recovery scan never sees it again.
    pending = await store.enqueue("cancel-pending", input_json=None)
    out["pending_ask"] = await admin.request_cancel(pending.run_id)

    landed = await store.load(pending.run_id)
    out["pending_status"] = None if landed is None else landed.status.value
    out["pending_stamped"] = landed is not None and landed.cancel_requested_at is not None

    claimed_ids = {
        r.run_id for r in await store.claim_abandoned(limit=50, lease_for=timedelta(minutes=5))
    }
    out["pending_not_reclaimed"] = pending.run_id not in claimed_ids

    # 6. Terminal now: the ask reports that nothing happened.
    out["terminal_ask"] = await admin.request_cancel(pending.run_id)

    # 2 + 6. A RUNNING run is only stamped; asking twice is idempotent down to the instant.
    running = await store.enqueue("cancel-running", input_json=None)
    holder = await store.begin(running.run_id, lease_for=timedelta(minutes=5))
    assert holder is not None

    out["running_ask"] = await admin.request_cancel(running.run_id)
    out["running_ask_again"] = await admin.request_cancel(running.run_id)

    stamped = await store.load(running.run_id)
    out["running_still_running"] = stamped is not None and stamped.status.value == "running"
    first_ask = None if stamped is None else stamped.cancel_requested_at

    reasked = await store.load(running.run_id)
    out["ask_instant_is_stable"] = (
        reasked is not None and reasked.cancel_requested_at == first_ask
    )

    # The holder learns about it on the heartbeat it was making anyway.
    renewal = await store.renew(
        running.run_id, lease_for=timedelta(minutes=5), fence=holder.attempts
    )
    out["renewal_held"] = renewal.held
    out["renewal_carries_cancel"] = renewal.cancel_requested

    # 4. A stale fence can neither renew nor land the cancel.
    stale = await store.renew(
        running.run_id, lease_for=timedelta(minutes=5), fence=holder.attempts + 99
    )
    out["stale_renewal_held"] = stale.held
    out["stale_renewal_cancel"] = stale.cancel_requested

    await store.mark_cancelled(running.run_id, fence=holder.attempts + 99)
    unmoved = await store.load(running.run_id)
    out["stale_landing_ignored"] = unmoved is not None and unmoved.status.value == "running"

    # The current holder lands it.
    await store.mark_cancelled(running.run_id, fence=holder.attempts)
    cancelled = await store.load(running.run_id)
    out["running_final_status"] = None if cancelled is None else cancelled.status.value

    # 3. A late completion against a landed cancel is a no-op, not an overwrite.
    await store.complete(running.run_id, output_json={"late": True}, fence=holder.attempts)
    after_race = await store.load(running.run_id)
    out["late_complete_status"] = None if after_race is None else after_race.status.value
    out["late_complete_output"] = None if after_race is None else after_race.output_json

    # 7. A refusal is recorded whatever the run's state, but only under a matching fence.
    refused = await store.enqueue("cancel-refused", input_json=None)
    refused_holder = await store.begin(refused.run_id, lease_for=timedelta(minutes=5))
    assert refused_holder is not None

    await admin.request_cancel(refused.run_id)
    await store.refuse_cancel(refused.run_id, fence=refused_holder.attempts + 99)
    not_refused = await store.load(refused.run_id)
    out["stale_refusal_ignored"] = (
        not_refused is not None and not_refused.cancel_refused_at is None
    )

    await store.refuse_cancel(refused.run_id, fence=refused_holder.attempts)
    mid_flight = await store.load(refused.run_id)
    out["refused_while_running"] = (
        mid_flight is not None and mid_flight.cancel_refused_at is not None
    )

    await store.complete(refused.run_id, output_json={"forward": True}, fence=refused_holder.attempts)
    completed = await store.load(refused.run_id)
    out["refused_status"] = None if completed is None else completed.status.value
    out["refused_asked"] = completed is not None and completed.cancel_requested_at is not None
    out["refused_stamped"] = completed is not None and completed.cancel_refused_at is not None

    # 7b. Refusing a run that has ALREADY landed — the ordering production actually takes,
    # and the one the "not guarded on RUNNING" promise exists for. The runner stamps the
    # refusal from its ``finally``, which runs after the terminal write, so a store that
    # quietly guarded this write like every other terminal write would drop every real
    # refusal while passing the mid-flight case above.
    late = await store.enqueue("cancel-refused-late", input_json=None)
    late_holder = await store.begin(late.run_id, lease_for=timedelta(minutes=5))
    assert late_holder is not None

    await admin.request_cancel(late.run_id)
    await store.complete(late.run_id, output_json={"forward": True}, fence=late_holder.attempts)
    await store.refuse_cancel(late.run_id, fence=late_holder.attempts)

    landed_late = await store.load(late.run_id)
    out["late_refusal_status"] = None if landed_late is None else landed_late.status.value
    out["late_refusal_stamped"] = (
        landed_late is not None and landed_late.cancel_refused_at is not None
    )

    # 9. The deadline terminal is its own state on both engines.
    expired = await store.enqueue("timed-out", input_json=None)
    expired_holder = await store.begin(expired.run_id, lease_for=timedelta(minutes=5))
    assert expired_holder is not None

    await store.mark_timed_out(expired.run_id, error="cap exceeded", fence=expired_holder.attempts)
    timed_out = await store.load(expired.run_id)
    out["timed_out_status"] = None if timed_out is None else timed_out.status.value
    out["timed_out_error"] = None if timed_out is None else timed_out.error

    # Both terminals are selectable through the ops listing, not just readable per-run.
    by_cancelled = await admin.list_runs(status=DurableRunStatus.CANCELLED, limit=10)
    by_timed_out = await admin.list_runs(status=DurableRunStatus.TIMED_OUT, limit=10)
    out["cancelled_names"] = sorted(r.name for r in by_cancelled.records)
    out["timed_out_names"] = sorted(r.name for r in by_timed_out.records)

    out["supports_cancel"] = durable_run_control_capabilities(store).supports_cancel

    return out


# ....................... #


class TestDurableMockVsPostgres:
    async def test_mock_matches_postgres_for_the_durable_lifecycle(
        self, pg_client: PostgresClient, run_table: str, step_table: str
    ) -> None:
        mock_state = MockState()
        mock_out = await _scenario(
            MockDurableRunStore(state=mock_state),
            lambda: MockDurableFunctionStepAdapter(state=mock_state),
        )

        pg_out = await _scenario(
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
        mock_out = await _list_scenario(MockDurableRunStore(state=MockState()))

        pg_out = await _list_scenario(
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
        mock_out = await _cancel_scenario(MockDurableRunStore(state=MockState()))

        pg_out = await _cancel_scenario(
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

        # The refusal survives being written against an already-terminal run: unlike every
        # other write on this port it is not guarded on RUNNING, and that is exactly the
        # ordering the runner produces (the stamp goes down in a ``finally``).
        assert mock_out["late_refusal_status"] == "completed"
        assert mock_out["late_refusal_stamped"] is True

        assert mock_out["timed_out_status"] == "timed_out"
        assert mock_out["timed_out_error"] == "cap exceeded"

        assert mock_out["cancelled_names"] == ["cancel-pending", "cancel-running"]
        assert mock_out["timed_out_names"] == ["timed-out"]
        assert mock_out["supports_cancel"] is True
