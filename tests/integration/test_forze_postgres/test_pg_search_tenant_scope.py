"""A tenant-aware search reads only the current tenant's rows, with or without filters.

Each engine is set up the common way, the index on the read relation itself, and holds one
matching row per tenant. Every read path must return only the bound tenant's row: an offset
page, its count, and a cursor page, for a query and for a blank one.
"""

from __future__ import annotations

from uuid import UUID, uuid4

import pytest
from pydantic import BaseModel

from forze.application.contracts.embeddings import EmbeddingsProviderDepKey, EmbeddingsSpec
from forze.application.contracts.querying import QueryFilterExpression
from forze.application.contracts.search import (
    HubSearchQueryDepKey,
    HubSearchSpec,
    SearchQueryDepKey,
    SearchSpec,
)
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import Deps, ExecutionContext
from forze_mock import MockHashEmbeddingsProvider
from forze_postgres.adapters.search._vector_sql import vector_param_literal
from forze_postgres.execution.deps import ConfigurablePostgresHubSearch, ConfigurablePostgresSearch
from forze_postgres.execution.deps.configs import (
    FtsEngine,
    PgroongaEngine,
    PostgresHubSearchConfig,
    PostgresHubSearchMemberConfig,
    PostgresSearchConfig,
    VectorEngine,
)
from forze_postgres.execution.deps.keys import PostgresClientDepKey, PostgresIntrospectorDepKey
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
from forze_postgres.kernel.client.client import PostgresClient
from tests.support.execution_context import context_from_deps

# ----------------------- #


class _Doc(BaseModel):
    id: UUID
    title: str
    content: str


def _embeddings(_ctx: ExecutionContext, spec: EmbeddingsSpec) -> MockHashEmbeddingsProvider:
    return MockHashEmbeddingsProvider(dimensions=spec.dimensions)


_FILTERS: list[QueryFilterExpression | None] = [None, {"$values": {"content": "x"}}]


async def _assert_scoped(
    ctx: ExecutionContext,
    spec: SearchSpec[_Doc],
    *,
    mine: UUID,
    mine_id: UUID,
    query: str,
) -> None:
    for filters in _FILTERS:
        with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=mine)):
            port = ctx.search.query(spec)

            page = await port.search_page(
                query, filters=filters, options={"search_count": "exact"}
            )
            assert [h.id for h in page.hits] == [mine_id], (query, filters)
            assert page.count == 1, (query, filters)

            cursor = await port.search_cursor(query, filters=filters)
            assert [h.id for h in cursor.hits] == [mine_id], (query, filters)


async def _two_tenants(pg_client: PostgresClient, table: str, extra_cols: str = "") -> tuple[UUID, UUID]:
    await pg_client.execute(
        f"""
        CREATE TABLE {table} (
            id uuid PRIMARY KEY,
            tenant_id uuid NOT NULL,
            title text NOT NULL,
            content text NOT NULL{extra_cols}
        )
        """
    )
    return uuid4(), uuid4()


# ....................... #


@pytest.mark.asyncio
@pytest.mark.parametrize("query", ["ledger", ""])
async def test_fts_search_reads_only_the_tenants_rows(
    pg_client: PostgresClient, query: str
) -> None:
    suffix = uuid4().hex[:12]
    table = f"ts_fts_{suffix}"
    index = f"idx_ts_fts_{suffix}"
    mine, theirs = await _two_tenants(pg_client, table)
    await pg_client.execute(
        f"""
        CREATE INDEX {index} ON {table}
        USING gin (to_tsvector('english', coalesce(title, '') || ' ' || coalesce(content, '')))
        """
    )
    ids = {mine: uuid4(), theirs: uuid4()}

    for tenant, row_id in ids.items():
        await pg_client.execute(
            f"INSERT INTO {table} (id, tenant_id, title, content) "
            "VALUES (%(id)s, %(t)s, 'ledger', 'x')",
            {"id": row_id, "t": tenant},
        )

    ctx = context_from_deps(
        Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
                SearchQueryDepKey: ConfigurablePostgresSearch(
                    config=PostgresSearchConfig(
                        index=("public", index),
                        read=("public", table),
                        engine=FtsEngine(groups={"A": ("title",), "B": ("content",)}),
                        tenant_aware=True,
                    )
                ),
            }
        )
    )
    spec = SearchSpec(name=f"ts_fts_{suffix}", model_type=_Doc, fields=["title", "content"])

    await _assert_scoped(ctx, spec, mine=mine, mine_id=ids[mine], query=query)


@pytest.mark.asyncio
@pytest.mark.parametrize("query", ["ledger", ""])
async def test_pgroonga_search_reads_only_the_tenants_rows(
    pg_client: PostgresClient, query: str
) -> None:
    await pg_client.execute("CREATE EXTENSION IF NOT EXISTS pgroonga;")

    suffix = uuid4().hex[:12]
    table = f"ts_pgr_{suffix}"
    index = f"idx_ts_pgr_{suffix}"
    mine, theirs = await _two_tenants(pg_client, table)
    await pg_client.execute(
        f"CREATE INDEX {index} ON {table} USING pgroonga ((ARRAY[title, content]))"
    )
    ids = {mine: uuid4(), theirs: uuid4()}

    for tenant, row_id in ids.items():
        await pg_client.execute(
            f"INSERT INTO {table} (id, tenant_id, title, content) "
            "VALUES (%(id)s, %(t)s, 'ledger', 'x')",
            {"id": row_id, "t": tenant},
        )

    ctx = context_from_deps(
        Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
                SearchQueryDepKey: ConfigurablePostgresSearch(
                    config=PostgresSearchConfig(
                        index=("public", index),
                        read=("public", table),
                        engine=PgroongaEngine(),
                        tenant_aware=True,
                    )
                ),
            }
        )
    )
    spec = SearchSpec(name=f"ts_pgr_{suffix}", model_type=_Doc, fields=["title", "content"])

    await _assert_scoped(ctx, spec, mine=mine, mine_id=ids[mine], query=query)


@pytest.mark.asyncio
@pytest.mark.parametrize("query", ["ledger", ""])
async def test_vector_search_reads_only_the_tenants_rows(
    pgvector_client: PostgresClient, query: str
) -> None:
    await pgvector_client.execute("CREATE EXTENSION IF NOT EXISTS vector")

    suffix = uuid4().hex[:12]
    table = f"ts_vec_{suffix}"
    index = f"idx_ts_vec_{suffix}"
    mine, theirs = await _two_tenants(pgvector_client, table, ",\n            emb vector(3) NOT NULL")
    literal = vector_param_literal(await MockHashEmbeddingsProvider(dimensions=3).embed_one("ledger"))
    ids = {mine: uuid4(), theirs: uuid4()}

    for tenant, row_id in ids.items():
        await pgvector_client.execute(
            f"INSERT INTO {table} (id, tenant_id, title, content, emb) "
            f"VALUES (%(id)s, %(t)s, 'ledger', 'x', '{literal}'::vector)",
            {"id": row_id, "t": tenant},
        )

    ctx = context_from_deps(
        Deps.plain(
            {
                PostgresClientDepKey: pgvector_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pgvector_client),
                SearchQueryDepKey: ConfigurablePostgresSearch(
                    config=PostgresSearchConfig(
                        index=("public", index),
                        read=("public", table),
                        heap=("public", table),
                        engine=VectorEngine(
                            column="emb", dimensions=3, embeddings_name="ts_vec"
                        ),
                        tenant_aware=True,
                    )
                ),
                EmbeddingsProviderDepKey: _embeddings,
            }
        )
    )
    spec = SearchSpec(name=f"ts_vec_{suffix}", model_type=_Doc, fields=["title", "content"])

    await _assert_scoped(ctx, spec, mine=mine, mine_id=ids[mine], query=query)


class _HubRow(BaseModel):
    id: UUID
    title: str
    content: str


class _HubLeg(BaseModel):
    title: str = ""
    content: str = ""


@pytest.mark.asyncio
@pytest.mark.parametrize("execution", ["sql", "parallel"])
@pytest.mark.parametrize("query", ["ledger", ""])
async def test_hub_search_reads_only_the_tenants_rows(
    pg_client: PostgresClient, query: str, execution: str
) -> None:
    await pg_client.execute("CREATE EXTENSION IF NOT EXISTS pgroonga;")

    suffix = uuid4().hex[:12]
    table = f"ts_hub_{suffix}"
    index = f"idx_ts_hub_{suffix}"
    mine, theirs = await _two_tenants(pg_client, table)
    await pg_client.execute(
        f"CREATE INDEX {index} ON {table} USING pgroonga ((ARRAY[title, content]))"
    )
    ids = {mine: uuid4(), theirs: uuid4()}

    for tenant, row_id in ids.items():
        await pg_client.execute(
            f"INSERT INTO {table} (id, tenant_id, title, content) "
            "VALUES (%(id)s, %(t)s, 'ledger', 'x')",
            {"id": row_id, "t": tenant},
        )

    leg = f"ts_leg_{suffix}"
    ctx = context_from_deps(
        Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
                HubSearchQueryDepKey: ConfigurablePostgresHubSearch(
                    config=PostgresHubSearchConfig(
                        hub=("public", table),
                        members={
                            leg: PostgresHubSearchMemberConfig(
                                index=("public", index),
                                read=("public", table),
                                hub_fk="id",
                                same_heap_as_hub=True,
                                engine="pgroonga",
                            )
                        },
                        execution=execution,  # type: ignore[arg-type]
                        tenant_aware=True,
                    )
                ),
            }
        )
    )
    spec = HubSearchSpec(
        name=f"ts_hub_{suffix}",
        model_type=_HubRow,
        members=(SearchSpec(name=leg, model_type=_HubLeg, fields=["title", "content"]),),
    )

    for filters in _FILTERS:
        with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=mine)):
            port = ctx.search.hub(spec)
            page = await port.search_page(
                query, filters=filters, options={"search_count": "exact"}
            )
            assert [h.id for h in page.hits] == [ids[mine]], (query, filters)
            assert page.count == 1, (query, filters)
