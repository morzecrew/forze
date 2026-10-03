"""A PGroonga search uses its index whatever expression the index was declared on.

Postgres serves an expression from an index only when the query names the very expression the
index holds. The match used to rewrap every column as ``coalesce(col::text, '')``, so an index
declared the natural way — ``ARRAY[title, content]`` or a plain ``(title)`` — was never used,
and every search scanned the whole table.
"""

from __future__ import annotations

from typing import Any
from uuid import UUID, uuid4

import pytest
from psycopg import sql
from pydantic import BaseModel

from forze.application.contracts.search import (
    HubSearchSpec,
    SearchQueryDepKey,
    SearchSpec,
)
from forze.application.execution import Deps
from forze_postgres.execution.deps import (
    ConfigurablePostgresHubSearch,
    ConfigurablePostgresSearch,
)
from forze_postgres.execution.deps.configs import (
    PgroongaEngine,
    PostgresHubSearchConfig,
    PostgresHubSearchMemberConfig,
    PostgresSearchConfig,
)
from forze_postgres.execution.deps.keys import PostgresClientDepKey, PostgresIntrospectorDepKey
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
from forze_postgres.kernel.client.client import PostgresClient
from tests.support.execution_context import context_from_deps

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


class _Row(BaseModel):
    id: UUID
    title: str | None = None
    content: str | None = None


_EXPRESSIONS = {
    "bare array": "(ARRAY[title, content])",
    "coalesced array": "(ARRAY[coalesce(title, ''), coalesce(content, '')])",
    "mixed array": "(ARRAY[title::text, coalesce(content, '')])",
    "plain column": "(title)",
}


async def _heap(client: PostgresClient, expression: str) -> tuple[str, str]:
    heap = f"pgx_{uuid4().hex[:8]}"
    await client.execute("CREATE EXTENSION IF NOT EXISTS pgroonga")
    await client.execute(f"CREATE TABLE {heap} (id uuid PRIMARY KEY, title text, content text)")
    # Nullable columns, NULL elements included: a NULL never matches, and never hides the
    # other element of the same row.
    await client.execute(
        f"INSERT INTO {heap} SELECT gen_random_uuid(), "
        "CASE WHEN g % 5 = 0 THEN NULL ELSE 'python ' || g END, "
        "CASE WHEN g % 3 = 0 THEN NULL WHEN g % 7 = 0 THEN 'python body ' || g ELSE 'body' END "
        "FROM generate_series(1, 200) g"
    )
    index = f"{heap}_pgr"
    await client.execute(f"CREATE INDEX {index} ON {heap} USING pgroonga ({expression})")
    await client.execute(f"ANALYZE {heap}")

    return heap, index


def _search_port(client: PostgresClient, heap: str, index: str, plan: str) -> Any:
    ctx = context_from_deps(
        Deps.plain(
            {
                PostgresClientDepKey: client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=client),
                SearchQueryDepKey: ConfigurablePostgresSearch(
                    config=PostgresSearchConfig(
                        index=("public", index),
                        read=("public", heap),
                        engine=PgroongaEngine(plan=plan),  # type: ignore[arg-type]
                    )
                ),
            }
        )
    )

    return ctx.search.query(SearchSpec(name="rows", model_type=_Row, fields=["title", "content"]))


def _hub_port(client: PostgresClient, heap: str, index: str) -> Any:
    member = f"leg_{heap}"
    spec = HubSearchSpec(
        name=f"hub_{heap}",
        model_type=_Row,
        members=(SearchSpec(name=member, model_type=_Row, fields=["title", "content"]),),
    )
    config = PostgresHubSearchConfig(
        hub=("public", heap),
        members={
            member: PostgresHubSearchMemberConfig(
                engine="pgroonga",
                index=("public", index),
                read=("public", heap),
                hub_fk="id",
                same_heap_as_hub=True,
            )
        },
    )
    ctx = context_from_deps(
        Deps.plain(
            {
                PostgresClientDepKey: client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=client),
            }
        )
    )

    return ConfigurablePostgresHubSearch(config=config)(ctx, spec)


async def _matching_plan(
    client: PostgresClient, monkeypatch: pytest.MonkeyPatch, port: Any
) -> tuple[list[UUID], str]:
    """Run one search and return its ids and the plan of the statement that matched."""

    statements: list[tuple[Any, Any]] = []
    fetch_all = PostgresClient.fetch_all

    async def spy(self: PostgresClient, stmt: Any, params: Any = None, *a: Any, **k: Any) -> Any:
        statements.append((stmt, params))
        return await fetch_all(self, stmt, params, *a, **k)

    monkeypatch.setattr(PostgresClient, "fetch_all", spy)
    page = await port.search("python", None, {"limit": 50})
    monkeypatch.undo()

    matching = [
        (stmt, params)
        for stmt, params in statements
        if "&@~" in stmt.as_string(None)  # pyright: ignore[reportUnknownMemberType]
    ]
    stmt, params = matching[-1]

    # With sequential scans priced out, a seq scan in the plan means the index could not serve
    # the match expression at all.
    async with client.transaction():
        await client.execute("SET LOCAL enable_seqscan = off")
        rows = await client.fetch_all(
            sql.SQL("EXPLAIN (COSTS OFF) {}").format(stmt), params, row_factory="tuple"
        )

    return [hit.id for hit in page.hits], "\n".join(str(row[0]) for row in rows)


def _uses_index(plan: str, index: str) -> bool:
    return f"Index Scan using {index}" in plan or f"Bitmap Index Scan on {index}" in plan


@pytest.mark.parametrize("plan", ["filter_first", "index_first"])
@pytest.mark.parametrize("expression", list(_EXPRESSIONS.values()), ids=list(_EXPRESSIONS))
async def test_a_search_uses_the_index_it_names(
    pg_client: PostgresClient, monkeypatch: pytest.MonkeyPatch, expression: str, plan: str
) -> None:
    heap, index = await _heap(pg_client, expression)

    ids, explained = await _matching_plan(
        pg_client, monkeypatch, _search_port(pg_client, heap, index, plan)
    )

    assert ids
    assert _uses_index(explained, index), explained


@pytest.mark.parametrize("expression", list(_EXPRESSIONS.values()), ids=list(_EXPRESSIONS))
async def test_a_hub_leg_uses_the_index_it_names(
    pg_client: PostgresClient, monkeypatch: pytest.MonkeyPatch, expression: str
) -> None:
    heap, index = await _heap(pg_client, expression)

    ids, explained = await _matching_plan(pg_client, monkeypatch, _hub_port(pg_client, heap, index))

    assert ids
    assert _uses_index(explained, index), explained


@pytest.mark.parametrize("plan", ["filter_first", "index_first"])
async def test_a_bare_and_a_coalesced_index_find_the_same_rows(
    pg_client: PostgresClient, monkeypatch: pytest.MonkeyPatch, plan: str
) -> None:
    found: dict[str, set[tuple[str | None, str | None]]] = {}

    for name in ("bare array", "coalesced array"):
        heap, index = await _heap(pg_client, _EXPRESSIONS[name])
        page = await _search_port(pg_client, heap, index, plan).search(
            "python", None, {"limit": 200}
        )
        found[name] = {(hit.title, hit.content) for hit in page.hits}

    # Every row holding the term in either column, NULL elements included, and no other.
    expected = {
        (
            None if g % 5 == 0 else f"python {g}",
            None if g % 3 == 0 else f"python body {g}" if g % 7 == 0 else "body",
        )
        for g in range(1, 201)
        if g % 5 != 0 or (g % 3 != 0 and g % 7 == 0)
    }

    assert found["bare array"] == found["coalesced array"] == expected


@pytest.mark.parametrize(
    "expression",
    [_EXPRESSIONS["bare array"], _EXPRESSIONS["coalesced array"], _EXPRESSIONS["plain column"]],
    ids=["bare array", "coalesced array", "plain column"],
)
async def test_a_match_follows_the_index_tokenizer(
    pg_client: PostgresClient, expression: str
) -> None:
    # Served from the index, "python" matches the word, not a longer word that starts with
    # it: what a coalesce-declared index always returned. A scan of a bare index's columns
    # used to match "pythonic" too.
    heap = f"pgt_{uuid4().hex[:8]}"
    await pg_client.execute("CREATE EXTENSION IF NOT EXISTS pgroonga")
    await pg_client.execute(f"CREATE TABLE {heap} (id uuid PRIMARY KEY, title text, content text)")
    await pg_client.execute(
        f"INSERT INTO {heap} VALUES (gen_random_uuid(), 'python guide', 'body'), "
        "(gen_random_uuid(), 'pythonic', 'x'), (gen_random_uuid(), 'the python', 'body')"
    )
    index = f"{heap}_pgr"
    await pg_client.execute(f"CREATE INDEX {index} ON {heap} USING pgroonga ({expression})")

    page = await _search_port(pg_client, heap, index, "filter_first").search(
        "python", None, {"limit": 10}
    )

    assert {hit.title for hit in page.hits} == {"python guide", "the python"}
