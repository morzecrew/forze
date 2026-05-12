"""Integration tests for :class:`ConfigurablePostgresReadOnlyDocument`."""

from uuid import uuid4

import pytest

from forze.application.contracts.document import DocumentQueryDepKey, DocumentSpec
from forze.application.execution import Deps, ExecutionContext
from forze.base.errors import NotFoundError
from forze.domain.models import ReadDocument
from forze_postgres.execution.deps.deps import ConfigurablePostgresReadOnlyDocument
from forze_postgres.execution.deps.keys import (
    PostgresClientDepKey,
    PostgresIntrospectorDepKey,
)
from forze_postgres.kernel.introspect import PostgresIntrospector
from forze_postgres.kernel.platform.client import PostgresClient


class _ReadOnlyRow(ReadDocument):
    title: str


@pytest.mark.asyncio
async def test_readonly_get_after_sql_insert(pg_client: PostgresClient) -> None:
    """Read-only adapter loads rows inserted outside the document command API."""
    t = f"ro_doc_{uuid4().hex[:12]}"
    await pg_client.execute(
        f"""
        CREATE TABLE {t} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            title text NOT NULL
        );
        """
    )

    doc_id = uuid4()
    await pg_client.execute(
        f"""
        INSERT INTO {t} (id, rev, created_at, last_update_at, title)
        VALUES (%(id)s, 1, NOW(), NOW(), %(title)s)
        """,
        {"id": doc_id, "title": "from sql"},
    )

    ro = ConfigurablePostgresReadOnlyDocument(
        config={"read": ("public", t)},
    )
    ctx = ExecutionContext(
        deps=Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
                DocumentQueryDepKey: ro,
            }
        )
    )

    spec = DocumentSpec(name="ro_ns", read=_ReadOnlyRow, write=None)
    q = ctx.doc_query(spec)

    row = await q.get(doc_id)
    assert row.title == "from sql"
    assert row.rev == 1


@pytest.mark.asyncio
async def test_readonly_find_many_sorts_and_count(pg_client: PostgresClient) -> None:
    """Read-only query port supports find_many with sorts and filtered count."""
    t = f"ro_many_{uuid4().hex[:12]}"
    await pg_client.execute(
        f"""
        CREATE TABLE {t} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            title text NOT NULL
        );
        """
    )

    titles = ["gamma", "alpha", "beta"]
    for title in titles:
        await pg_client.execute(
            f"""
            INSERT INTO {t} (id, rev, created_at, last_update_at, title)
            VALUES (%(id)s, 1, NOW(), NOW(), %(title)s)
            """,
            {"id": uuid4(), "title": title},
        )

    ro = ConfigurablePostgresReadOnlyDocument(config={"read": ("public", t)})
    ctx = ExecutionContext(
        deps=Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
                DocumentQueryDepKey: ro,
            }
        )
    )

    spec = DocumentSpec(name="ro_many_ns", read=_ReadOnlyRow, write=None)
    q = ctx.doc_query(spec)

    __p = await q.find_page(None,
        pagination={"limit": 10, "offset": 0},
        sorts={"title": "asc"},
    )
    rows = __p.hits
    total = __p.count
    assert total == 3
    assert [r.title for r in rows] == ["alpha", "beta", "gamma"]

    __p = await q.find_page(None,
        pagination={"limit": 1, "offset": 1},
        sorts={"title": "asc"},
    )
    page = __p.hits
    total_p = __p.count
    assert total_p == 3
    assert len(page) == 1
    assert page[0].title == "beta"

    assert await q.count({"$fields": {"title": "gamma"}}) == 1


@pytest.mark.asyncio
async def test_readonly_get_missing_raises(pg_client: PostgresClient) -> None:
    t = f"ro_miss_{uuid4().hex[:12]}"
    await pg_client.execute(
        f"""
        CREATE TABLE {t} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            title text NOT NULL
        );
        """
    )
    ro = ConfigurablePostgresReadOnlyDocument(config={"read": ("public", t)})
    ctx = ExecutionContext(
        deps=Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
                DocumentQueryDepKey: ro,
            }
        )
    )
    q = ctx.doc_query(DocumentSpec(name="ro_miss_ns", read=_ReadOnlyRow, write=None))
    with pytest.raises(NotFoundError, match="Record not found"):
        await q.get(uuid4())


@pytest.mark.asyncio
async def test_readonly_get_many_partial_missing_raises(pg_client: PostgresClient) -> None:
    t = f"ro_gm_{uuid4().hex[:12]}"
    await pg_client.execute(
        f"""
        CREATE TABLE {t} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            title text NOT NULL
        );
        """
    )
    doc_id = uuid4()
    await pg_client.execute(
        f"""
        INSERT INTO {t} (id, rev, created_at, last_update_at, title)
        VALUES (%(id)s, 1, NOW(), NOW(), %(title)s)
        """,
        {"id": doc_id, "title": "solo"},
    )
    ro = ConfigurablePostgresReadOnlyDocument(config={"read": ("public", t)})
    ctx = ExecutionContext(
        deps=Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
                DocumentQueryDepKey: ro,
            }
        )
    )
    q = ctx.doc_query(DocumentSpec(name="ro_gm_ns", read=_ReadOnlyRow, write=None))
    with pytest.raises(NotFoundError, match="Some records not found"):
        await q.get_many([doc_id, uuid4()])
