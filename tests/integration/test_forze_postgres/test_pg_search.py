from uuid import UUID, uuid4

import pytest
from pydantic import BaseModel

from forze.application.contracts.search.specs import SearchSpec
from forze.application.execution import Deps, ExecutionContext
from forze_postgres.execution.deps.deps import postgres_search
from forze_postgres.execution.deps.keys import (
    PostgresClientDepKey,
    PostgresIntrospectorDepKey,
)
from forze_postgres.kernel.introspect import PostgresIntrospector
from forze_postgres.kernel.platform.client import PostgresClient


class SearchableModel(BaseModel):
    id: UUID
    title: str
    content: str


@pytest.fixture
def execution_context(pg_client: PostgresClient):
    deps = Deps(
        {
            PostgresClientDepKey: pg_client,
            PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
        }
    )
    return ExecutionContext(deps=deps)


@pytest.mark.asyncio
async def test_postgres_search_adapter(
    pg_client: PostgresClient, execution_context: ExecutionContext
):
    # pgroonga is available in the image
    await pg_client.execute("CREATE EXTENSION IF NOT EXISTS pgroonga;")

    await pg_client.execute(
        """
        CREATE TABLE search_items (
            id uuid PRIMARY KEY,
            title text NOT NULL,
            content text NOT NULL
        );
        """
    )

    # Create PGroonga index.
    # Must use ARRAY for multi-column search so it's matched as array by introspector
    await pg_client.execute(
        """
        CREATE INDEX idx_search_items_pgroonga
        ON search_items USING pgroonga ((ARRAY[title, content]));
        """
    )

    docs = [
        {
            "id": uuid4(),
            "title": "Forze Framework",
            "content": "Hexagonal architecture framework in python",
        },
        {
            "id": uuid4(),
            "title": "Postgres Guide",
            "content": "How to use postgres with python",
        },
        {
            "id": uuid4(),
            "title": "Python Tips",
            "content": "Advanced python development",
        },
    ]

    for doc in docs:
        await pg_client.execute(
            "INSERT INTO search_items (id, title, content) VALUES (%(id)s, %(title)s, %(content)s)",
            doc,
        )

    spec = SearchSpec(
        namespace="search_ns",
        model=SearchableModel,
        indexes={
            "idx_search_items_pgroonga": {
                "source": "search_items",
                "mode": "pgroonga",
                "fields": [{"path": "title"}, {"path": "content"}],
            }
        },
        default_index="idx_search_items_pgroonga",
    )

    adapter = postgres_search(execution_context, spec)

    res, cnt = await adapter.search("python")
    assert cnt == 3
    assert len(res) == 3

    res2, cnt2 = await adapter.search("hexagonal")
    assert cnt2 == 1
    assert len(res2) == 1
    assert res2[0].title == "Forze Framework"
