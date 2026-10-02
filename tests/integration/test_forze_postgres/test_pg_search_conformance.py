"""Postgres FTS and PGroonga against the shared search battery.

Postgres searches the system of record, so the corpus is inserted as rows and the adapter
reads the same table an application would already own. The two text engines share the
offset pipeline but not its blank-query browse, so both run the battery.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any
from uuid import UUID, uuid4

import pytest
import pytest_asyncio
from pydantic import BaseModel

from forze.application.contracts.search import HubSearchSpec, SearchQueryDepKey, SearchSpec
from forze.application.execution import Deps
from forze_postgres.execution.deps import (
    ConfigurablePostgresHubSearch,
    ConfigurablePostgresSearch,
)
from forze_postgres.execution.deps.configs import (
    FtsEngine,
    PostgresHubSearchConfig,
    PostgresHubSearchMemberConfig,
    PostgresSearchConfig,
)
from forze_postgres.execution.deps.keys import (
    PostgresClientDepKey,
    PostgresIntrospectorDepKey,
)
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
from forze_postgres.kernel.client.client import PostgresClient
from tests.support.execution_context import context_from_deps
from tests.support.search_conformance import (
    DEFAULT_SORT,
    SEARCH_BATTERY,
    Check,
    SearchHarness,
    capped_rows,
    corpus_rows,
    searchable_fields,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


class _Row(BaseModel):
    id: UUID
    title: str
    content: str
    category: str = ""
    price: Decimal = Decimal(0)
    rank: int | None = None


class _Leg(BaseModel):
    title: str
    content: str


_FTS = "USING gin (to_tsvector('english', coalesce(title,'') || ' ' || coalesce(content,'')))"
_INDEXES = {"fts": _FTS, "pgroonga": "USING pgroonga ((ARRAY[title, content]))", "hub": _FTS}
_FTS_GROUPS = FtsEngine(groups={"A": ("title",), "B": ("content",)})


async def _port(
    pg_client: PostgresClient, engine: str, rows: list[dict[str, Any]], *, capped: bool
) -> Any:
    """A port over *rows* in a table of their own; *capped* takes the smallest candidate cap."""

    table = f"search_conf_{uuid4().hex[:10]}"
    index = f"idx_{table}"

    await pg_client.execute("CREATE EXTENSION IF NOT EXISTS pgroonga;")
    await pg_client.execute(
        f"""
        CREATE TABLE {table} (
            id uuid PRIMARY KEY,
            title text NOT NULL,
            content text NOT NULL,
            category text NOT NULL,
            price numeric NOT NULL,
            rank int
        );
        CREATE INDEX {index} ON {table} {_INDEXES[engine]};
        """
    )

    for row in rows:
        await pg_client.execute(
            f"INSERT INTO {table} (id, title, content, category, price, rank) "
            "VALUES (%(id)s, %(title)s, %(content)s, %(category)s, %(price)s, %(rank)s)",
            row,
        )

    cap: dict[str, Any] = {"candidate_limit": 1} if capped else {}
    ctx = context_from_deps(
        Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
                SearchQueryDepKey: ConfigurablePostgresSearch(
                    config=PostgresSearchConfig(
                        index=("public", index),
                        read=("public", table),
                        engine=_FTS_GROUPS if engine == "fts" else "pgroonga",
                        **cap,
                    )
                ),
            }
        )
    )

    if engine != "hub":
        return ctx.search.query(
            SearchSpec(
                name="rows", model_type=_Row, fields=searchable_fields(), default_sort=DEFAULT_SORT
            )
        )

    # One leg over the hub table itself: each row is its own leg match.
    leg = SearchSpec(name="leg", model_type=_Leg, fields=searchable_fields())
    hub = ConfigurablePostgresHubSearch(
        config=PostgresHubSearchConfig(
            hub=("public", table),
            members={
                "leg": PostgresHubSearchMemberConfig(
                    index=("public", index),
                    read=("public", table),
                    hub_fk="id",
                    engine=_FTS_GROUPS,
                )
            },
            combo_limit=1 if capped else None,
        )
    )

    return hub(
        ctx,
        HubSearchSpec(name="rows", model_type=_Row, members=(leg,), default_sort=DEFAULT_SORT),
    )


@pytest_asyncio.fixture(params=["fts", "pgroonga", "hub"])
async def harness(request: pytest.FixtureRequest, pg_client: PostgresClient) -> SearchHarness:
    engine: str = request.param

    return SearchHarness(
        query=await _port(pg_client, engine, corpus_rows(lambda: str(uuid4())), capped=False),
        backend=f"pg_{engine}",
        blank_query_matches_all=True,
        capped=await _port(pg_client, engine, capped_rows(lambda: str(uuid4())), capped=True),
    )


@pytest.mark.conformance(plane="search", engine="postgres")
@pytest.mark.parametrize("check", SEARCH_BATTERY, ids=lambda check: check.__name__)
async def test_search_battery(check: Check, harness: SearchHarness) -> None:
    await check(harness)
