"""A tenant-aware federated search keeps a leg that is deliberately not tenant-aware.

Such a leg reads a corpus every tenant shares, often a table with no ``tenant_id`` column.
Scoping it by the federation's flag would fail the whole search on that column; the
isolation floor, not the factory, is what refuses the wiring where it matters.

# covers: ConfigurablePostgresFederatedSearch.__call__
"""

from __future__ import annotations

from uuid import UUID, uuid4

import pytest
from pydantic import BaseModel

from forze.application.contracts.search import (
    FederatedSearchQueryDepKey,
    FederatedSearchSpec,
    SearchSpec,
)
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import Deps
from forze_postgres.execution.deps import ConfigurablePostgresFederatedSearch
from forze_postgres.execution.deps.configs import (
    PostgresFederatedSearchConfig,
    PostgresFederatedSearchLegSearch,
    PostgresSearchConfig,
)
from forze_postgres.execution.deps.keys import PostgresClientDepKey, PostgresIntrospectorDepKey
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
from forze_postgres.kernel.client.client import PostgresClient
from tests.support.execution_context import context_from_deps


class _Hit(BaseModel):
    id: UUID
    label: str


def _leg(table: str, *, tenant_aware: bool) -> PostgresFederatedSearchLegSearch:
    return PostgresFederatedSearchLegSearch(
        search=PostgresSearchConfig(
            index=("public", f"i_{table}"),
            read=("public", table),
            engine="pgroonga",
            tenant_aware=tenant_aware,
        ),
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_a_leg_that_is_not_tenant_aware_reads_its_shared_corpus(
    pg_client: PostgresClient,
) -> None:
    await pg_client.execute("CREATE EXTENSION IF NOT EXISTS pgroonga;")
    suffix = uuid4().hex[:8]
    docs, help_ = f"docs_{suffix}", f"help_{suffix}"
    await pg_client.execute(
        f"""
        CREATE TABLE {docs} (id uuid PRIMARY KEY, tenant_id uuid NOT NULL, label text NOT NULL);
        CREATE INDEX i_{docs} ON {docs} USING pgroonga (label);
        CREATE TABLE {help_} (id uuid PRIMARY KEY, label text NOT NULL);
        CREATE INDEX i_{help_} ON {help_} USING pgroonga (label);
        """
    )
    tenant, other = uuid4(), uuid4()

    for owner, label in ((tenant, "token own doc"), (other, "token other doc")):
        await pg_client.execute(
            f"INSERT INTO {docs} VALUES (%(i)s, %(t)s, %(l)s)",
            {"i": uuid4(), "t": owner, "l": label},
        )

    await pg_client.execute(
        f"INSERT INTO {help_} VALUES (%(i)s, 'token help article')", {"i": uuid4()}
    )

    config = PostgresFederatedSearchConfig(
        tenant_aware=True,
        members={
            "docs": _leg(docs, tenant_aware=True),
            "help": _leg(help_, tenant_aware=False),
        },
    )
    ctx = context_from_deps(
        Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
                FederatedSearchQueryDepKey: ConfigurablePostgresFederatedSearch(config=config),
            }
        )
    )
    spec = FederatedSearchSpec(
        name="fed",
        members=(
            SearchSpec(name="docs", model_type=_Hit, fields=["label"]),
            SearchSpec(name="help", model_type=_Hit, fields=["label"]),
        ),
    )

    with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=tenant)):
        page = await ctx.search.federated(spec).search_page("token")

    assert sorted(hit.hit.label for hit in page.hits) == ["token help article", "token own doc"]
