"""Shared bootstrap helpers for Postgres search integration tests."""

from __future__ import annotations

from uuid import UUID, uuid4

from pydantic import BaseModel

from forze.application.contracts.search import SearchQueryDepKey, SearchSpec
from forze.application.execution import Deps, ExecutionContext
from forze_postgres.execution.deps.deps import ConfigurablePostgresSearch
from forze_postgres.execution.deps.keys import (
    PostgresClientDepKey,
    PostgresIntrospectorDepKey,
)
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
from forze_postgres.kernel.client.client import PostgresClient
from tests.support import IntegrationSearchHitFactory


class PgSearchRow(BaseModel):
    id: UUID
    title: str
    content: str


async def bootstrap_pgroonga_search_table(
    pg_client: PostgresClient,
    *,
    suffix: str | None = None,
) -> tuple[str, str, list[dict[str, object]]]:
    """Create PGroonga table + index; return ``(table, index_name, inserted_rows)``."""

    await pg_client.execute("CREATE EXTENSION IF NOT EXISTS pgroonga;")
    tag = suffix or uuid4().hex[:12]
    table = f"pgr_path_{tag}"
    index_name = f"idx_pgr_path_{tag}"

    await pg_client.execute(
        f"""
        CREATE TABLE {table} (
            id uuid PRIMARY KEY,
            title text NOT NULL,
            content text NOT NULL
        );
        CREATE INDEX {index_name}
        ON {table} USING pgroonga ((ARRAY[title, content]));
        """,
    )

    rows: list[dict[str, object]] = []
    for title, content in (
        ("Forze Framework", "hexagonal python framework"),
        ("Postgres Guide", "database with python"),
        ("Cooking Tips", "recipes without databases"),
    ):
        hit = IntegrationSearchHitFactory.build(title=title, content=content)
        rows.append({"id": hit.id, "title": hit.title, "content": hit.content})
        await pg_client.execute(
            f"""
            INSERT INTO {table} (id, title, content)
            VALUES (%(id)s, %(title)s, %(content)s)
            """,
            rows[-1],
        )

    return table, index_name, rows


async def bootstrap_fts_search_table(
    pg_client: PostgresClient,
    *,
    suffix: str | None = None,
) -> tuple[str, str, list[dict[str, object]]]:
    """Create GIN tsvector table + index; return ``(table, index_name, inserted_rows)``."""

    tag = suffix or uuid4().hex[:12]
    table = f"fts_path_{tag}"
    index_name = f"idx_fts_path_{tag}"

    await pg_client.execute(
        f"""
        CREATE TABLE {table} (
            id uuid PRIMARY KEY,
            title text NOT NULL,
            content text NOT NULL
        );
        CREATE INDEX {index_name}
        ON {table}
        USING gin (to_tsvector('english', coalesce(title, '') || ' ' || coalesce(content, '')));
        """,
    )

    rows: list[dict[str, object]] = []
    for title, content in (
        ("PostgreSQL FTS", "full text search with tsvector"),
        ("Cooking", "recipes without database jargon"),
    ):
        hit = IntegrationSearchHitFactory.build(title=title, content=content)
        rows.append({"id": hit.id, "title": hit.title, "content": hit.content})
        await pg_client.execute(
            f"""
            INSERT INTO {table} (id, title, content)
            VALUES (%(id)s, %(title)s, %(content)s)
            """,
            rows[-1],
        )

    return table, index_name, rows


def pgroonga_search_context(
    pg_client: PostgresClient,
    *,
    table: str,
    index_name: str,
) -> ExecutionContext:
    return ExecutionContext(
        deps=Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
                SearchQueryDepKey: ConfigurablePostgresSearch(
                    config={
                        "index": ("public", index_name),
                        "read": ("public", table),
                        "engine": "pgroonga",
                    },
                ),
            },
        ),
    )


def fts_search_context(
    pg_client: PostgresClient,
    *,
    table: str,
    index_name: str,
) -> ExecutionContext:
    return ExecutionContext(
        deps=Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
                SearchQueryDepKey: ConfigurablePostgresSearch(
                    config={
                        "index": ("public", index_name),
                        "read": ("public", table),
                        "engine": "fts",
                        "fts_groups": {
                            "A": ("title",),
                            "B": ("content",),
                        },
                    },
                ),
            },
        ),
    )


def search_row_spec(*, name: str) -> SearchSpec[PgSearchRow]:
    return SearchSpec(
        name=name,
        model_type=PgSearchRow,
        fields=["title", "content"],
    )


pgroonga_spec = search_row_spec
fts_spec = search_row_spec
