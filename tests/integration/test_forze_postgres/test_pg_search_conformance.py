"""Postgres FTS against the shared search battery.

Postgres searches the system of record, so the corpus is inserted as rows and the adapter
reads the same table an application would already own.
"""

from __future__ import annotations

from uuid import UUID, uuid4

import pytest
import pytest_asyncio
from pydantic import BaseModel

from forze.application.contracts.search import SearchQueryDepKey, SearchSpec
from forze.application.execution import Deps
from forze_postgres.execution.deps import ConfigurablePostgresSearch
from forze_postgres.execution.deps.configs import FtsEngine, PostgresSearchConfig
from forze_postgres.execution.deps.keys import (
    PostgresClientDepKey,
    PostgresIntrospectorDepKey,
)
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
from forze_postgres.kernel.client.client import PostgresClient
from tests.support.execution_context import context_from_deps
from tests.support.search_conformance import (
    SEARCH_BATTERY,
    Check,
    SearchHarness,
    corpus_rows,
    searchable_fields,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


class _Row(BaseModel):
    id: UUID
    title: str
    content: str
    category: str = ""


@pytest_asyncio.fixture
async def harness(pg_client: PostgresClient) -> SearchHarness:
    table = f"search_conf_{uuid4().hex[:10]}"
    index = f"idx_{table}"

    await pg_client.execute(
        f"""
        CREATE TABLE {table} (
            id uuid PRIMARY KEY,
            title text NOT NULL,
            content text NOT NULL,
            category text NOT NULL
        );
        CREATE INDEX {index} ON {table}
        USING gin (to_tsvector('english', coalesce(title,'') || ' ' || coalesce(content,'')));
        """
    )

    for row in corpus_rows(lambda: str(uuid4())):
        await pg_client.execute(
            f"INSERT INTO {table} (id, title, content, category) "
            "VALUES (%(id)s, %(title)s, %(content)s, %(category)s)",
            row,
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
                    )
                ),
            }
        )
    )

    spec = SearchSpec(name="rows", model_type=_Row, fields=searchable_fields())

    return SearchHarness(
        query=ctx.search.query(spec),
        backend="pg_fts",
        blank_query_matches_all=True,
    )


@pytest.mark.parametrize("check", SEARCH_BATTERY, ids=lambda check: check.__name__)
async def test_search_battery(check: Check, harness: SearchHarness) -> None:
    await check(harness)
