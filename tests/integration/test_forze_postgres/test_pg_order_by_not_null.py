"""An order on a ``NOT NULL`` column is one a plain btree index serves.

Every key carries the canonical null placement (``NULLS FIRST`` ascending, ``NULLS LAST``
descending) so Postgres agrees with Mongo and the in-memory oracle. A plain btree stores
``ASC NULLS LAST`` and reads backwards as ``DESC NULLS FIRST``: it serves neither, so every
sorted page sorted the whole filtered set first. A column that cannot hold a null orders the
same with or without the clause, so it is left out there and kept everywhere else.
"""

from __future__ import annotations

from typing import Any
from uuid import UUID, uuid4

import pytest
from psycopg import sql
from pydantic import BaseModel

from forze.application.contracts.search import SearchQueryDepKey, SearchSpec
from forze.application.execution import Deps
from forze_postgres.execution.deps import ConfigurablePostgresSearch
from forze_postgres.execution.deps.configs import FtsEngine, PostgresSearchConfig
from forze_postgres.execution.deps.keys import PostgresClientDepKey, PostgresIntrospectorDepKey
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
from forze_postgres.kernel.client.client import PostgresClient
from forze_postgres.kernel.gateways import PostgresReadGateway
from tests.support.execution_context import context_from_deps
from tests.unit._gateway_codec_helpers import codec_for

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


class _Row(BaseModel):
    n: int
    m: int | None = None


class _Recording:
    """The client, recording the last statement a read sent through it."""

    def __init__(self, client: PostgresClient) -> None:
        self.client = client
        self.last: tuple[Any, Any] | None = None

    def __getattr__(self, name: str) -> Any:
        return getattr(self.client, name)

    async def fetch_all(self, stmt: Any, params: Any = None, **kwargs: Any) -> Any:
        self.last = (stmt, params)

        return await self.client.fetch_all(stmt, params, **kwargs)


async def _gateway(client: PostgresClient) -> tuple[PostgresReadGateway[_Row], _Recording, str]:
    table = f"order_nn_{uuid4().hex[:10]}"

    await client.execute(
        f"CREATE TABLE {table} (n int NOT NULL, m int); "
        f"CREATE INDEX ON {table} (n); CREATE INDEX ON {table} (m); "
        f"INSERT INTO {table} SELECT g, g FROM generate_series(1, 20000) g; "
        f"ANALYZE {table};"
    )
    recording = _Recording(client)
    gateway = PostgresReadGateway(
        relation=("public", table),
        client=recording,  # type: ignore[arg-type]
        model_type=_Row,
        codec=codec_for(_Row),
        introspector=PostgresIntrospector(client=client),
        tenant_aware=False,
    )

    return gateway, recording, table


async def _explain(client: PostgresClient, stmt: Any, params: Any = None) -> str:
    rows = await client.fetch_all(sql.SQL("EXPLAIN {}").format(stmt), params)

    return "\n".join(str(row["QUERY PLAN"]) for row in rows)


async def _plan(client: PostgresClient, sorts: dict[str, str]) -> str:
    gateway, _, table = await _gateway(client)
    order = await gateway.order_by_clause(sorts)

    return await _explain(
        client,
        sql.SQL("SELECT * FROM {} ORDER BY {} LIMIT 20").format(sql.Identifier(table), order),
    )


@pytest.mark.parametrize("direction", ["asc", "desc"])
async def test_a_not_null_column_is_read_from_its_index(
    pg_client: PostgresClient, direction: str
) -> None:
    plan = await _plan(pg_client, {"n": direction})

    assert "Index Scan" in plan and "Sort" not in plan, plan


async def test_a_nullable_column_keeps_its_null_placement(pg_client: PostgresClient) -> None:
    # Descending with nulls last is not what the plain index holds, so the rows are sorted.
    plan = await _plan(pg_client, {"m": "desc"})

    assert "Sort" in plan, plan


@pytest.mark.parametrize("direction", ["asc", "desc"])
async def test_a_cursor_page_is_read_from_the_index_too(
    pg_client: PostgresClient, direction: str
) -> None:
    gateway, recording, _ = await _gateway(pg_client)

    await gateway.find_many_with_cursor(None, {"limit": 20}, {"n": direction})

    assert recording.last is not None
    plan = await _explain(pg_client, *recording.last)

    assert "Index Scan" in plan and "Sort" not in plan, plan


async def test_a_blank_search_cursor_page_is_read_from_the_index_too(
    pg_client: PostgresClient,
) -> None:
    _, recording, table = await _gateway(pg_client)
    ctx = context_from_deps(
        Deps.plain(
            {
                PostgresClientDepKey: recording,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
                SearchQueryDepKey: ConfigurablePostgresSearch(
                    config=PostgresSearchConfig(
                        index=("public", table),
                        read=("public", table),
                        engine=FtsEngine(groups={"A": ("n",)}),
                    )
                ),
            }
        )
    )
    port = ctx.search.query(SearchSpec(name="rows", model_type=_Row, fields=["n"]))

    await port.search_cursor("", None, {"limit": 20}, {"n": "desc"})

    assert recording.last is not None
    plan = await _explain(pg_client, *recording.last)

    assert "Index Scan" in plan and "Sort" not in plan, plan


class _Labelled(BaseModel):
    id: UUID
    label: str
    m: int | None = None


async def test_a_ranked_cursor_walk_meets_every_row_of_a_nullable_sort(
    pg_client: PostgresClient,
) -> None:
    # The seek reads a null as the smallest value; an order that put nulls last on an
    # ascending key would walk past them.
    table = f"ranked_nulls_{uuid4().hex[:10]}"
    await pg_client.execute(
        f"CREATE TABLE {table} (id uuid PRIMARY KEY, label text NOT NULL, m int); "
        f"CREATE INDEX {table}_fts ON {table} USING gin (to_tsvector('english', label)); "
        f"INSERT INTO {table} SELECT gen_random_uuid(), 'alpha', "
        "CASE WHEN g % 2 = 0 THEN NULL ELSE g END FROM generate_series(1, 6) g;"
    )
    ctx = context_from_deps(
        Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
                SearchQueryDepKey: ConfigurablePostgresSearch(
                    config=PostgresSearchConfig(
                        index=("public", f"{table}_fts"),
                        read=("public", table),
                        engine=FtsEngine(groups={"A": ("label",)}),
                    )
                ),
            }
        )
    )
    port = ctx.search.query(SearchSpec(name="rows", model_type=_Labelled, fields=["label"]))

    walked: list[UUID] = []
    cursor: dict[str, Any] = {"limit": 1}

    for _ in range(10):
        page = await port.search_cursor("alpha", None, cursor, {"m": "asc"})
        walked += [hit.id for hit in page.hits]

        if not page.has_more:
            break

        cursor = {"limit": 1, "after": page.next_cursor}

    offset = await port.search("alpha", None, {"limit": 10}, {"m": "asc"})

    assert walked == [hit.id for hit in offset.hits]
    assert [hit.m for hit in offset.hits][:3] == [None, None, None]
