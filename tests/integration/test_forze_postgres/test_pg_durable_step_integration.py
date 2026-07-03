"""Integration tests for the Postgres durable-function step-memo journal.

# covers: DurableFunctionStepPort.run

The headline is the exactly-once step effect: the first execution of ``(run_id, step_id)``
runs the body and journals its result; a replay returns the journaled result without
re-running the body — the primitive crash recovery memoizes over.
"""

from __future__ import annotations

import asyncio
from uuid import uuid4

import pytest
from psycopg import sql

from forze.application.contracts.durable.function import (
    DurableRunContext,
    bind_durable_run,
    reset_durable_run,
)
from forze.base.exceptions import CoreException
from forze_postgres.adapters.durable import PostgresDurableFunctionStepAdapter
from forze_postgres.execution.deps.configs import PostgresDurableStepConfig
from forze_postgres.kernel.client import PostgresClient

# ----------------------- #


@pytest.fixture
async def durable_step_table(pg_client: PostgresClient) -> str:
    """Create a dedicated ``durable_step`` table and return its name."""

    table = f"durable_step_{uuid4().hex[:8]}"
    await pg_client.execute(
        sql.SQL(
            """
            CREATE TABLE {table} (
                run_id     TEXT        NOT NULL,
                step_id    TEXT        NOT NULL,
                result     JSONB       NOT NULL,
                tenant_id  UUID,
                created_at TIMESTAMPTZ NOT NULL,
                PRIMARY KEY (run_id, step_id)
            )
            """
        ).format(table=sql.Identifier("public", table))
    )
    return table


def _adapter(
    pg_client: PostgresClient,
    table: str,
) -> PostgresDurableFunctionStepAdapter:
    return PostgresDurableFunctionStepAdapter(
        client=pg_client,
        config=PostgresDurableStepConfig(relation=("public", table)),
    )


async def _count_rows(pg_client: PostgresClient, table: str) -> int:
    return await pg_client.fetch_value(
        sql.SQL("SELECT count(*) FROM {table}").format(
            table=sql.Identifier("public", table)
        )
    )


# ....................... #


class TestPostgresDurableStep:
    async def test_step_runs_once_and_replays_from_journal(
        self, pg_client: PostgresClient, durable_step_table: str
    ) -> None:
        adapter = _adapter(pg_client, durable_step_table)
        calls: list[int] = []

        async def fn() -> dict[str, int]:
            calls.append(1)
            return {"n": 42}

        token = bind_durable_run(DurableRunContext(run_id="run-1", name="fn"))
        try:
            first = await adapter.run("s1", fn)
            second = await adapter.run("s1", fn)
        finally:
            reset_durable_run(token)

        assert first == {"n": 42}
        assert second == {"n": 42}  # replayed, body not re-run
        assert len(calls) == 1
        assert await _count_rows(pg_client, durable_step_table) == 1

    async def test_falsy_result_is_memoized_not_reexecuted(
        self, pg_client: PostgresClient, durable_step_table: str
    ) -> None:
        adapter = _adapter(pg_client, durable_step_table)
        calls: list[int] = []

        async def fn() -> None:
            calls.append(1)
            return None

        token = bind_durable_run(DurableRunContext(run_id="run-1", name="fn"))
        try:
            first = await adapter.run("s1", fn)
            second = await adapter.run("s1", fn)
        finally:
            reset_durable_run(token)

        # A journaled ``None`` replays (a present row), never re-runs — the row's existence,
        # not the result's truthiness, decides replay.
        assert first is None
        assert second is None
        assert len(calls) == 1

    async def test_distinct_steps_and_runs_are_isolated(
        self, pg_client: PostgresClient, durable_step_table: str
    ) -> None:
        adapter = _adapter(pg_client, durable_step_table)

        async def make(value: str):
            async def fn() -> str:
                return value

            return fn

        token = bind_durable_run(DurableRunContext(run_id="run-A", name="fn"))
        try:
            assert await adapter.run("s1", await make("a1")) == "a1"
            assert await adapter.run("s2", await make("a2")) == "a2"
        finally:
            reset_durable_run(token)

        token = bind_durable_run(DurableRunContext(run_id="run-B", name="fn"))
        try:
            # Same step id under a different run journals independently.
            assert await adapter.run("s1", await make("b1")) == "b1"
        finally:
            reset_durable_run(token)

        assert await _count_rows(pg_client, durable_step_table) == 3

    async def test_step_outside_a_run_is_rejected(
        self, pg_client: PostgresClient, durable_step_table: str
    ) -> None:
        adapter = _adapter(pg_client, durable_step_table)

        async def fn() -> int:
            return 1

        with pytest.raises(CoreException, match="durable run"):
            await adapter.run("s1", fn)

    async def test_non_serializable_result_is_rejected(
        self, pg_client: PostgresClient, durable_step_table: str
    ) -> None:
        adapter = _adapter(pg_client, durable_step_table)

        async def fn() -> object:
            return object()  # not JSON-serializable

        token = bind_durable_run(DurableRunContext(run_id="run-1", name="fn"))
        try:
            with pytest.raises(CoreException, match="JSON-serializable"):
                await adapter.run("s1", fn)
        finally:
            reset_durable_run(token)

        assert await _count_rows(pg_client, durable_step_table) == 0

    async def test_concurrent_first_execution_converges_on_one_journal_row(
        self, pg_client: PostgresClient, durable_step_table: str
    ) -> None:
        adapter = _adapter(pg_client, durable_step_table)
        order: list[int] = []

        async def fn() -> dict[str, str]:
            order.append(1)
            await asyncio.sleep(0.05)  # widen the race between the two SELECT/INSERTs
            return {"winner": "converged"}

        async def call() -> dict[str, str]:
            token = bind_durable_run(DurableRunContext(run_id="run-1", name="fn"))
            try:
                return await adapter.run("s1", fn)
            finally:
                reset_durable_run(token)

        first, second = await asyncio.gather(call(), call())

        # Both callers converge on the single journaled result (ON CONFLICT DO NOTHING keeps
        # exactly one), regardless of which INSERT won.
        assert first == {"winner": "converged"}
        assert second == {"winner": "converged"}
        assert await _count_rows(pg_client, durable_step_table) == 1
