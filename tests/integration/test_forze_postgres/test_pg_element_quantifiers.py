"""Integration tests for ``$not`` and element quantifiers (``$any``, ``$all``, ``$none``)."""

from __future__ import annotations

from uuid import uuid4

import pytest
from pydantic import BaseModel

from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
    DocumentSpec,
)
from forze.application.execution import Deps, ExecutionContext
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_postgres.execution.deps import ConfigurablePostgresDocument
from forze_postgres.execution.deps.keys import (
    PostgresClientDepKey,
    PostgresIntrospectorDepKey,
)
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
from forze_postgres.kernel.client.client import PostgresClient
from forze_postgres.execution.deps.configs import PostgresDocumentConfig


def _ctx(pg_client: PostgresClient, table: str) -> ExecutionContext:
    doc = ConfigurablePostgresDocument(
        config=PostgresDocumentConfig(
            read=("public", table),
            write=("public", table),
            bookkeeping_strategy="application",
        )
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


@pytest.mark.asyncio
async def test_element_any_on_native_text_array(pg_client: PostgresClient) -> None:
    t = f"elem_any_{uuid4().hex[:12]}"

    class _Doc(Document):
        title: str
        tags: list[str]

    class _Create(CreateDocumentCmd):
        title: str
        tags: list[str]

    class _Update(BaseDTO):
        title: str | None = None
        tags: list[str] | None = None

    class _Read(ReadDocument):
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
    spec = DocumentSpec(
        name="elem_any_ns",
        read=_Read,
        write={"domain": _Doc, "create_cmd": _Create, "update_cmd": _Update},
    )
    ctx = _ctx(pg_client, t)
    cmd = ctx.document.command(spec)
    query = ctx.document.query(spec)

    await cmd.create(_Create(title="a", tags=["urgent", "ops"]))
    await cmd.create(_Create(title="b", tags=["ops"]))
    await cmd.create(_Create(title="c", tags=[]))

    urgent = {"$values": {"tags": {"$any": "urgent"}}}
    assert await query.count(urgent) == 1
    row = await query.find(urgent)
    assert row is not None and row.title == "a"

    all_ops = {"$values": {"tags": {"$all": {"$eq": "ops"}}}}
    # ``b`` has only ``ops``; empty ``c`` satisfies ``$all`` vacuously.
    assert await query.count(all_ops) == 2

    none_urgent = {"$values": {"tags": {"$none": "urgent"}}}
    assert await query.count(none_urgent) == 2


@pytest.mark.asyncio
async def test_not_combinator(pg_client: PostgresClient) -> None:
    t = f"elem_not_{uuid4().hex[:12]}"

    class _Doc(Document):
        title: str

    class _Create(CreateDocumentCmd):
        title: str

    class _Update(BaseDTO):
        title: str | None = None

    class _Read(ReadDocument):
        title: str

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
    spec = DocumentSpec(
        name="elem_not_ns",
        read=_Read,
        write={"domain": _Doc, "create_cmd": _Create, "update_cmd": _Update},
    )
    ctx = _ctx(pg_client, t)
    cmd = ctx.document.command(spec)
    query = ctx.document.query(spec)

    await cmd.create(_Create(title="keep"))
    await cmd.create(_Create(title="drop"))

    filt = {"$not": {"$values": {"title": "drop"}}}
    assert await query.count(filt) == 1
    row = await query.find(filt)
    assert row is not None and row.title == "keep"


@pytest.mark.asyncio
async def test_element_any_on_jsonb_object_array(pg_client: PostgresClient) -> None:
    t = f"elem_obj_{uuid4().hex[:12]}"

    class _Item(BaseModel):
        status: str
        qty: int

    class _Doc(Document):
        title: str
        items: list[_Item]

    class _Create(CreateDocumentCmd):
        title: str
        items: list[_Item]

    class _Update(BaseDTO):
        title: str | None = None
        items: list[_Item] | None = None

    class _Read(ReadDocument):
        title: str
        items: list[_Item]

    await pg_client.execute(
        f"""
        CREATE TABLE {t} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            title text NOT NULL,
            items jsonb NOT NULL
        );
        """
    )
    spec = DocumentSpec(
        name="elem_obj_ns",
        read=_Read,
        write={"domain": _Doc, "create_cmd": _Create, "update_cmd": _Update},
    )
    ctx = _ctx(pg_client, t)
    cmd = ctx.document.command(spec)
    query = ctx.document.query(spec)

    await cmd.create(
        _Create(
            title="match",
            items=[_Item(status="open", qty=2), _Item(status="closed", qty=1)],
        ),
    )
    await cmd.create(
        _Create(title="miss", items=[_Item(status="closed", qty=5)]),
    )

    filt = {
        "$values": {
            "items": {
                "$any": {
                    "$values": {
                        "status": "open",
                        "qty": {"$gte": 2},
                    },
                },
            },
        },
    }
    assert await query.count(filt) == 1
    row = await query.find(filt)
    assert row is not None and row.title == "match"

    none_open = {
        "$values": {
            "items": {"$none": {"$values": {"status": "open"}}},
        },
    }
    assert await query.count(none_open) == 1
    assert (await query.find(none_open)).title == "miss"


@pytest.mark.asyncio
async def test_element_any_scalar_ordering(pg_client: PostgresClient) -> None:
    t = f"elem_ord_{uuid4().hex[:12]}"

    class _Doc(Document):
        title: str
        scores: list[int]

    class _Create(CreateDocumentCmd):
        title: str
        scores: list[int]

    class _Update(BaseDTO):
        title: str | None = None
        scores: list[int] | None = None

    class _Read(ReadDocument):
        title: str
        scores: list[int]

    await pg_client.execute(
        f"""
        CREATE TABLE {t} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            title text NOT NULL,
            scores int[] NOT NULL
        );
        """
    )
    spec = DocumentSpec(
        name="elem_ord_ns",
        read=_Read,
        write={"domain": _Doc, "create_cmd": _Create, "update_cmd": _Update},
    )
    ctx = _ctx(pg_client, t)
    cmd = ctx.document.command(spec)
    query = ctx.document.query(spec)

    await cmd.create(_Create(title="high", scores=[5, 15]))
    await cmd.create(_Create(title="low", scores=[1, 2]))

    filt = {"$values": {"scores": {"$any": {"$gte": 10}}}}
    assert await query.count(filt) == 1
    row = await query.find(filt)
    assert row is not None and row.title == "high"


@pytest.mark.asyncio
async def test_not_with_nested_or(pg_client: PostgresClient) -> None:
    t = f"elem_not_or_{uuid4().hex[:12]}"

    class _Doc(Document):
        status: str

    class _Create(CreateDocumentCmd):
        status: str

    class _Update(BaseDTO):
        status: str | None = None

    class _Read(ReadDocument):
        status: str

    await pg_client.execute(
        f"""
        CREATE TABLE {t} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            status text NOT NULL
        );
        """
    )
    spec = DocumentSpec(
        name="elem_not_or_ns",
        read=_Read,
        write={"domain": _Doc, "create_cmd": _Create, "update_cmd": _Update},
    )
    ctx = _ctx(pg_client, t)
    cmd = ctx.document.command(spec)
    query = ctx.document.query(spec)

    await cmd.create(_Create(status="active"))
    await cmd.create(_Create(status="archived"))
    await cmd.create(_Create(status="pending"))

    filt = {
        "$not": {
            "$or": [
                {"$values": {"status": "archived"}},
                {"$values": {"status": "pending"}},
            ],
        },
    }
    assert await query.count(filt) == 1
    row = await query.find(filt)
    assert row is not None and row.status == "active"


@pytest.mark.asyncio
async def test_element_quantifiers_combined_with_and(pg_client: PostgresClient) -> None:
    t = f"elem_and_{uuid4().hex[:12]}"

    class _Doc(Document):
        title: str
        tags: list[str]

    class _Create(CreateDocumentCmd):
        title: str
        tags: list[str]

    class _Update(BaseDTO):
        title: str | None = None
        tags: list[str] | None = None

    class _Read(ReadDocument):
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
    spec = DocumentSpec(
        name="elem_and_ns",
        read=_Read,
        write={"domain": _Doc, "create_cmd": _Create, "update_cmd": _Update},
    )
    ctx = _ctx(pg_client, t)
    cmd = ctx.document.command(spec)
    query = ctx.document.query(spec)

    await cmd.create(_Create(title="yes", tags=["api", "urgent"]))
    await cmd.create(_Create(title="no_tag", tags=["api"]))
    await cmd.create(_Create(title="no_title", tags=["urgent"]))

    filt = {
        "$and": [
            {"$values": {"title": "yes"}},
            {"$values": {"tags": {"$any": "urgent"}}},
        ],
    }
    assert await query.count(filt) == 1
    assert (await query.find(filt)).title == "yes"


@pytest.mark.asyncio
async def test_element_none_on_scalar_array(pg_client: PostgresClient) -> None:
    t = f"elem_none_{uuid4().hex[:12]}"

    class _Doc(Document):
        title: str
        tags: list[str]

    class _Create(CreateDocumentCmd):
        title: str
        tags: list[str]

    class _Update(BaseDTO):
        title: str | None = None
        tags: list[str] | None = None

    class _Read(ReadDocument):
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
    spec = DocumentSpec(
        name="elem_none_ns",
        read=_Read,
        write={"domain": _Doc, "create_cmd": _Create, "update_cmd": _Update},
    )
    ctx = _ctx(pg_client, t)
    cmd = ctx.document.command(spec)
    query = ctx.document.query(spec)

    await cmd.create(_Create(title="clean", tags=["api"]))
    await cmd.create(_Create(title="dirty", tags=["urgent", "api"]))

    filt = {"$values": {"tags": {"$none": "urgent"}}}
    assert await query.count(filt) == 1
    assert (await query.find(filt)).title == "clean"
