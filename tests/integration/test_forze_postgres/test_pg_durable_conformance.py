"""Differential conformance: the mock durable journal behaves like the Postgres one.

Runs one identical durable scenario (enqueue → idempotent re-submit → claim → journal a
step + replay → complete → refuse-reclaim) against the in-memory mock and against real
Postgres, and asserts the observable outcomes are identical — so "passed on the mock" means
"matches the self-hosted engine". JSON-native step results are used so the round-trip is
byte-identical on both (the JSON-projection divergence is documented, not exercised here).

# covers: DurableRunStorePort.enqueue
# covers: DurableFunctionStepPort.run
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any, Callable
from uuid import uuid4

import pytest
from psycopg import sql

from forze.application.contracts.durable.function import (
    DurableFunctionStepPort,
    DurableRunContext,
    DurableRunStorePort,
    bind_durable_run,
    reset_durable_run,
)
from forze_postgres.adapters.durable import (
    PostgresDurableFunctionStepAdapter,
    PostgresDurableRunStore,
)
from forze_postgres.execution.deps.configs import (
    PostgresDurableRunConfig,
    PostgresDurableStepConfig,
)
from forze_postgres.kernel.client import PostgresClient

from forze_mock import MockDurableFunctionStepAdapter, MockDurableRunStore, MockState

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
