"""Integration tests for ``$empty`` on JSON/JSONB columns (JSON array payload)."""

from __future__ import annotations

from uuid import uuid4

import pytest

from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
    DocumentSpec,
)
from forze.application.execution import Deps, ExecutionContext
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_postgres.execution.deps.deps import ConfigurablePostgresDocument
from forze_postgres.execution.deps.keys import (
    PostgresClientDepKey,
    PostgresIntrospectorDepKey,
)
from forze_postgres.kernel.introspect import PostgresIntrospector
from forze_postgres.kernel.platform.client import PostgresClient


class _ListDoc(Document):
    title: str
    characteristics: list[str]


class _ListCreate(CreateDocumentCmd):
    title: str
    characteristics: list[str]


class _ListUpdate(BaseDTO):
    title: str | None = None
    characteristics: list[str] | None = None


class _ListRead(ReadDocument):
    title: str
    characteristics: list[str]


def _ctx(pg_client: PostgresClient, table: str) -> ExecutionContext:
    doc = ConfigurablePostgresDocument(
        config={
            "read": ("public", table),
            "write": ("public", table),
            "bookkeeping_strategy": "application",
        }
    )
    return ExecutionContext(
        deps=Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
                DocumentQueryDepKey: doc,
                DocumentCommandDepKey: doc,
            }
        )
    )


def _spec() -> DocumentSpec:
    return DocumentSpec(
        name="jsonb_empty_ns",
        read=_ListRead,
        write={
            "domain": _ListDoc,
            "create_cmd": _ListCreate,
            "update_cmd": _ListUpdate,
        },
    )


@pytest.mark.asyncio
async def test_empty_filter_jsonb_array_column(pg_client: PostgresClient) -> None:
    """``$empty`` matches empty vs non-empty JSON arrays in a jsonb column."""
    t = f"jb_empty_{uuid4().hex[:12]}"
    await pg_client.execute(
        f"""
        CREATE TABLE {t} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            title text NOT NULL,
            characteristics jsonb NOT NULL
        );
        """
    )
    ctx = _ctx(pg_client, t)
    spec = _spec()
    cmd = ctx.doc_command(spec)
    query = ctx.doc_query(spec)

    await cmd.create(_ListCreate(title="none", characteristics=[]))
    await cmd.create(_ListCreate(title="one", characteristics=["a"]))
    await cmd.create(_ListCreate(title="two", characteristics=["a", "b"]))

    empty_f = {"$fields": {"characteristics": {"$empty": True}}}
    nonempty_f = {"$fields": {"characteristics": {"$empty": False}}}

    assert await query.count(empty_f) == 1
    assert await query.count(nonempty_f) == 2

    e = await query.find(empty_f)
    assert e is not None and e.title == "none" and e.characteristics == []

    __p = await query.find_many(
        nonempty_f,
        pagination={"limit": 10, "offset": 0},
        sorts={"title": "asc"},
        return_count=True,
    )
    assert __p.count == 2
    assert {r.title for r in __p.hits} == {"one", "two"}


@pytest.mark.asyncio
async def test_empty_filter_json_column(pg_client: PostgresClient) -> None:
    """``$empty`` works when the column type is ``json`` (cast to jsonb in SQL)."""
    t = f"j_empty_{uuid4().hex[:12]}"
    await pg_client.execute(
        f"""
        CREATE TABLE {t} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            title text NOT NULL,
            characteristics json NOT NULL
        );
        """
    )
    ctx = _ctx(pg_client, t)
    spec = _spec()
    cmd = ctx.doc_command(spec)
    query = ctx.doc_query(spec)

    await cmd.create(_ListCreate(title="empty", characteristics=[]))
    await cmd.create(_ListCreate(title="full", characteristics=["x"]))

    assert await query.count({"$fields": {"characteristics": {"$empty": True}}}) == 1
    assert await query.count({"$fields": {"characteristics": {"$empty": False}}}) == 1


@pytest.mark.asyncio
async def test_empty_filter_native_pg_array_unaffected(pg_client: PostgresClient) -> None:
    """Native ``text[]`` still uses ``cardinality`` via the same ``$empty`` operator."""
    t = f"arr_empty_{uuid4().hex[:12]}"

    class _ArrDoc(Document):
        title: str
        tags: list[str]

    class _ArrCreate(CreateDocumentCmd):
        title: str
        tags: list[str]

    class _ArrUpdate(BaseDTO):
        title: str | None = None
        tags: list[str] | None = None

    class _ArrRead(ReadDocument):
        title: str
        tags: list[str]

    await pg_client.execute(
        f"""
        CREATE TABLE {t} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            title text NOT NULL,
            tags text[] NOT NULL
        );
        """
    )
    arr_spec = DocumentSpec(
        name="arr_empty_ns",
        read=_ArrRead,
        write={
            "domain": _ArrDoc,
            "create_cmd": _ArrCreate,
            "update_cmd": _ArrUpdate,
        },
    )
    ctx = _ctx(pg_client, t)
    cmd = ctx.doc_command(arr_spec)
    query = ctx.doc_query(arr_spec)

    await cmd.create(_ArrCreate(title="a", tags=[]))
    await cmd.create(_ArrCreate(title="b", tags=["z"]))

    assert await query.count({"$fields": {"tags": {"$empty": True}}}) == 1
    assert await query.count({"$fields": {"tags": {"$empty": False}}}) == 1
