"""The tenancy scenario on real Postgres — the other half of the mock-equals-real pair.

The scenario is the one the mock leg drives, imported rather than restated, so the two
engines cannot quietly diverge. What this module owns is the provisioning: a table with a
real ``tenant_id`` column, and a document adapter wired ``tenant_aware`` over it. The
isolation being exercised here is a `WHERE` predicate the database applies, not a
partitioned dictionary — which is what makes running the same body on both worth doing.
"""

from __future__ import annotations

from uuid import UUID, uuid4

import pytest
from pydantic import Field

from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
    DocumentSpec,
)
from forze.application.execution import Deps, ExecutionContext
from forze.base.primitives import StrKeyNamespace
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_kits.aggregates.document import build_document_registry
from forze_postgres.execution.deps import ConfigurablePostgresDocument
from forze_postgres.execution.deps.configs import PostgresDocumentConfig
from forze_postgres.execution.deps.keys import (
    PostgresClientDepKey,
    PostgresIntrospectorDepKey,
)
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
from forze_postgres.kernel.client.client import PostgresClient
from tests.support.agent_tools_tenancy import (
    AGENT_TOOLS_TENANCY_BATTERY,
    AgentToolsTenancyHarness,
    Check,
)
from tests.support.execution_context import context_from_deps

# ----------------------- #

_NS = StrKeyNamespace(prefix="notes")


class _Note(Document):
    """``tenant_id`` is populated by the gateway on insert."""

    tenant_id: UUID | None = Field(default=None)
    title: str


class _NoteRead(ReadDocument):
    tenant_id: UUID
    title: str


class _CreateNote(CreateDocumentCmd):
    title: str


class _UpdateNote(BaseDTO):
    title: str | None = None


def _spec() -> DocumentSpec:
    return DocumentSpec(
        name="notes",
        read=_NoteRead,
        write={
            "domain": _Note,
            "create_cmd": _CreateNote,
            "update_cmd": _UpdateNote,
        },
    )


def _tenant_table_context(pg_client: PostgresClient, table: str) -> ExecutionContext:
    configurable = ConfigurablePostgresDocument(
        config=PostgresDocumentConfig(
            read=("public", table),
            write=("public", table),
            bookkeeping_strategy="application",
            tenant_aware=True,
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
    table = f"agent_tools_tenancy_{uuid4().hex[:12]}"
    await pg_client.execute(
        f"""
        CREATE TABLE {table} (
            id uuid PRIMARY KEY,
            tenant_id uuid NOT NULL,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            title text NOT NULL
        );
        """
    )

    return table


# ....................... #


@pytest.mark.asyncio
@pytest.mark.parametrize("check", AGENT_TOOLS_TENANCY_BATTERY, ids=lambda c: c.__name__)
async def test_agent_tools_tenancy_battery(check: Check, pg_client: PostgresClient) -> None:
    table = await _table(pg_client)

    await check(
        AgentToolsTenancyHarness(
            ctx=_tenant_table_context(pg_client, table),
            registry=build_document_registry(_spec(), ns=_NS).freeze(),
            ns=_NS,
            backend="postgres",
        )
    )


# ....................... #


@pytest.mark.asyncio
async def test_the_tenant_column_is_what_isolates(pg_client: PostgresClient) -> None:
    """The scenario reads through the port; this reads the table underneath it.

    Asserting only through the port would leave one reading open: that the row landed
    tenant-less and the query filtered on something else. Here the column is inspected
    directly, so "isolated by tenant" names the mechanism rather than the outcome.
    """

    table = await _table(pg_client)
    harness = AgentToolsTenancyHarness(
        ctx=_tenant_table_context(pg_client, table),
        registry=build_document_registry(_spec(), ns=_NS).freeze(),
        ns=_NS,
        backend="postgres",
    )

    written = await harness.create(harness.tenant_a, "stamped")

    assert written.is_error is False, written.content

    rows = await pg_client.fetch_all(f"SELECT tenant_id, title FROM {table}", row_factory="dict")

    assert [(row["tenant_id"], row["title"]) for row in rows] == [
        (harness.tenant_a.tenant_id, "stamped")
    ]
