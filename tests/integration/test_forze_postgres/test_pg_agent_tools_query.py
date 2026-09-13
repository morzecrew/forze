"""The query scenario on real Postgres — the other half of the mock-equals-real pair.

The scenario is the one the mock leg drives, imported rather than restated. What runs here
is a compiled `WHERE`, `ORDER BY` and `GROUP BY` against a real table, which is the point:
an in-memory store filtering a list of dicts can agree with the DSL's intent and a SQL
compiler still get an operator or a grouping wrong. This module owns the provisioning — a
table matching the shared spec's read model — and nothing about the assertions.
"""

from __future__ import annotations

from uuid import uuid4

import pytest

from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
)
from forze.application.execution import Deps, ExecutionContext
from forze_postgres.execution.deps import ConfigurablePostgresDocument
from forze_postgres.execution.deps.configs import PostgresDocumentConfig
from forze_postgres.execution.deps.keys import (
    PostgresClientDepKey,
    PostgresIntrospectorDepKey,
)
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
from forze_postgres.kernel.client.client import PostgresClient
from tests.support.agent_tools_query import (
    AGENT_TOOLS_QUERY_BATTERY,
    SEED,
    AgentToolsQueryHarness,
    Check,
    battery_is_populated,
    query_registry,
)
from tests.support.execution_context import context_from_deps

pytestmark = pytest.mark.integration

# ----------------------- #


def _table_context(pg_client: PostgresClient, table: str) -> ExecutionContext:
    configurable = ConfigurablePostgresDocument(
        config=PostgresDocumentConfig(
            read=("public", table),
            write=("public", table),
            bookkeeping_strategy="application",
        )
    )

    return context_from_deps(
        Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
                DocumentQueryDepKey: configurable,
                DocumentCommandDepKey: configurable,
            }
        )
    )


async def _table(pg_client: PostgresClient) -> str:
    table = f"agent_tools_query_{uuid4().hex[:12]}"
    await pg_client.execute(
        f"""
        CREATE TABLE {table} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            title text NOT NULL,
            category text NOT NULL,
            body text NOT NULL
        );
        """
    )

    return table


def _harness(pg_client: PostgresClient, table: str) -> AgentToolsQueryHarness:
    return AgentToolsQueryHarness(
        ctx=_table_context(pg_client, table),
        registry=query_registry(),
        backend="postgres",
    )


# ....................... #


def test_the_battery_still_has_its_checks() -> None:
    battery_is_populated()


# ....................... #


@pytest.mark.asyncio
@pytest.mark.parametrize("check", AGENT_TOOLS_QUERY_BATTERY, ids=lambda c: c.__name__)
async def test_agent_tools_query_battery(check: Check, pg_client: PostgresClient) -> None:
    await check(_harness(pg_client, await _table(pg_client)))


# ....................... #


@pytest.mark.asyncio
async def test_the_seed_reaches_the_table(pg_client: PostgresClient) -> None:
    """The battery reads through the port; this reads the table underneath it.

    Every check above would also pass against an empty table if the filter, the sort and
    the grouping each returned nothing and the oracle happened to be empty too — it is
    not, so they would fail, but the failure would name the wrong thing. This pins the
    setup so a provisioning break reads as a provisioning break.
    """

    table = await _table(pg_client)
    harness = _harness(pg_client, table)

    await harness.seed()

    rows = await pg_client.fetch_all(
        f"SELECT title, category, body FROM {table} ORDER BY title", row_factory="dict"
    )

    assert [(row["title"], row["category"], row["body"]) for row in rows] == sorted(SEED)
