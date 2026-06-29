"""Integration tests: search filters/sorts by a materialized computed field.

A ``SearchSpec.materialized`` computed field is a real column on the search relation,
so search results can be filtered and sorted by the derived value at the database. The
value still recomputes from the row on decode (a computed field is read-only).
"""

from uuid import UUID, uuid4

import pytest
from pydantic import BaseModel, computed_field

from forze.application.contracts.querying import QueryFilterExpression
from forze.application.contracts.search import SearchQueryDepKey, SearchSpec
from forze.application.execution import Deps, ExecutionContext
from forze_postgres.execution.deps import ConfigurablePostgresSearch
from forze_postgres.execution.deps.configs import FtsEngine, PostgresSearchConfig
from forze_postgres.execution.deps.keys import (
    PostgresClientDepKey,
    PostgresIntrospectorDepKey,
)
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
from forze_postgres.kernel.client.client import PostgresClient
from tests.support.execution_context import context_from_deps


class MatArticle(BaseModel):
    id: UUID
    title: str
    qty: int
    unit_price: float

    @computed_field  # type: ignore[prop-decorator]
    @property
    def total(self) -> float:
        return self.qty * self.unit_price


def _ctx(pg_client: PostgresClient, *, table: str, index_name: str) -> ExecutionContext:
    return context_from_deps(
        Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
                SearchQueryDepKey: ConfigurablePostgresSearch(
                    config=PostgresSearchConfig(
                        index=("public", index_name),
                        read=("public", table),
                        engine=FtsEngine(groups={"A": ("title",)}),
                    )
                ),
            }
        )
    )


async def _seed(pg_client: PostgresClient) -> tuple[str, str]:
    suffix = uuid4().hex[:12]
    table = f"mat_search_{suffix}"
    index_name = f"idx_mat_{suffix}"

    await pg_client.execute(
        f"""
        CREATE TABLE {table} (
            id uuid PRIMARY KEY,
            title text NOT NULL,
            qty integer NOT NULL,
            unit_price double precision NOT NULL,
            total double precision NOT NULL
        );
        """
    )
    await pg_client.execute(
        f"CREATE INDEX {index_name} ON {table} "
        "USING gin (to_tsvector('english', coalesce(title, '')));"
    )

    # total is kept consistent with qty * unit_price (as the document side would write it).
    rows = [("widget alpha", 2, 5.0), ("widget beta", 3, 10.0), ("widget gamma", 1, 4.0)]
    for title, qty, price in rows:
        await pg_client.execute(
            f"INSERT INTO {table} (id, title, qty, unit_price, total) "
            "VALUES (%(id)s, %(title)s, %(qty)s, %(price)s, %(total)s)",
            {
                "id": uuid4(),
                "title": title,
                "qty": qty,
                "price": price,
                "total": qty * price,
            },
        )

    return table, index_name


@pytest.mark.integration
@pytest.mark.asyncio
async def test_search_sorts_by_materialized_field(pg_client: PostgresClient) -> None:
    table, index_name = await _seed(pg_client)
    ctx = _ctx(pg_client, table=table, index_name=index_name)

    spec = SearchSpec(
        name="mat_search",
        model_type=MatArticle,
        fields=["title"],
        materialized={"total"},
    )
    adapter = ctx.search.query(spec)

    page = await adapter.search_page("widget", sorts={"total": "desc"})

    totals = [hit.total for hit in page.hits]
    assert totals == [30.0, 10.0, 4.0]  # ordered by the materialized column, desc


@pytest.mark.integration
@pytest.mark.asyncio
async def test_search_filters_by_materialized_field(pg_client: PostgresClient) -> None:
    table, index_name = await _seed(pg_client)
    ctx = _ctx(pg_client, table=table, index_name=index_name)

    spec = SearchSpec(
        name="mat_search",
        model_type=MatArticle,
        fields=["title"],
        materialized={"total"},
    )
    adapter = ctx.search.query(spec)

    flt: QueryFilterExpression = {"$values": {"total": {"$gte": 10.0}}}
    page = await adapter.search_page("widget", filters=flt)

    assert page.count == 2
    assert {hit.total for hit in page.hits} == {10.0, 30.0}
