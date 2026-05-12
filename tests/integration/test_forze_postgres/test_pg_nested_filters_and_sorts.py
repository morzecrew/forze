"""Integration tests for dot-path filters and sorts on JSONB (nested Pydantic fields)."""

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
from forze_postgres.execution.deps.deps import ConfigurablePostgresDocument
from forze_postgres.execution.deps.keys import (
    PostgresClientDepKey,
    PostgresIntrospectorDepKey,
)
from forze_postgres.kernel.introspect import PostgresIntrospector
from forze_postgres.kernel.platform.client import PostgresClient


class Meta(BaseModel):
    """Nested payload stored in ``meta`` jsonb."""

    score: int
    tag: str = ""


class RowDoc(Document):
    title: str
    meta: Meta


class RowCreate(CreateDocumentCmd):
    title: str
    meta: Meta


class RowUpdate(BaseDTO):
    title: str | None = None
    meta: Meta | None = None


class RowRead(ReadDocument):
    title: str
    meta: Meta


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
        name="nested_pg_ns",
        read=RowRead,
        write={"domain": RowDoc, "create_cmd": RowCreate, "update_cmd": RowUpdate},
    )


@pytest.mark.asyncio
async def test_sort_by_nested_jsonb_field(pg_client: PostgresClient) -> None:
    t = f"nest_sort_{uuid4().hex[:12]}"
    await pg_client.execute(
        f"""
        CREATE TABLE {t} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            title text NOT NULL,
            meta jsonb NOT NULL
        );
        """
    )
    ctx = _ctx(pg_client, t)
    spec = _spec()
    cmd = ctx.doc_command(spec)
    query = ctx.doc_query(spec)

    await cmd.create(
        RowCreate(title="c", meta=Meta(score=30, tag="low")),
    )
    await cmd.create(
        RowCreate(title="a", meta=Meta(score=10, tag="mid")),
    )
    await cmd.create(
        RowCreate(title="b", meta=Meta(score=20, tag="high")),
    )

    __p = await query.find_page(None,
        pagination={"limit": 10, "offset": 0},
        sorts={"meta.score": "asc"},
    )
    rows = __p.hits
    total = __p.count
    assert total == 3
    assert [r.meta.score for r in rows] == [10, 20, 30]


@pytest.mark.asyncio
async def test_filter_on_nested_jsonb_scalar(pg_client: PostgresClient) -> None:
    t = f"nest_filt_{uuid4().hex[:12]}"
    await pg_client.execute(
        f"""
        CREATE TABLE {t} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            title text NOT NULL,
            meta jsonb NOT NULL
        );
        """
    )
    ctx = _ctx(pg_client, t)
    spec = _spec()
    cmd = ctx.doc_command(spec)
    query = ctx.doc_query(spec)

    await cmd.create(RowCreate(title="keep", meta=Meta(score=5, tag="x")))
    await cmd.create(RowCreate(title="drop", meta=Meta(score=50, tag="y")))

    __p = await query.find_page({"$fields": {"meta.score": {"$lte": 10}}},
        pagination={"limit": 10, "offset": 0},
    )
    rows = __p.hits
    total = __p.count
    assert total == 1
    assert rows[0].title == "keep"
    assert rows[0].meta.score == 5


@pytest.mark.asyncio
async def test_logical_and_across_top_level_and_nested_paths(
    pg_client: PostgresClient,
) -> None:
    t = f"nest_and_{uuid4().hex[:12]}"
    await pg_client.execute(
        f"""
        CREATE TABLE {t} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            title text NOT NULL,
            meta jsonb NOT NULL
        );
        """
    )
    ctx = _ctx(pg_client, t)
    spec = _spec()
    cmd = ctx.doc_command(spec)
    query = ctx.doc_query(spec)

    await cmd.create(RowCreate(title="match", meta=Meta(score=7, tag="a")))
    await cmd.create(RowCreate(title="other", meta=Meta(score=7, tag="b")))
    await cmd.create(RowCreate(title="match", meta=Meta(score=99, tag="c")))

    filt = {
        "$and": [
            {"$fields": {"title": "match"}},
            {"$fields": {"meta.score": {"$eq": 7}}},
        ]
    }
    assert await query.count(filt) == 1
    row = await query.find(filt)
    assert row is not None
    assert row.meta.tag == "a"


@pytest.mark.asyncio
async def test_logical_or_nested_and_top_level(pg_client: PostgresClient) -> None:
    t = f"nest_or_{uuid4().hex[:12]}"
    await pg_client.execute(
        f"""
        CREATE TABLE {t} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            title text NOT NULL,
            meta jsonb NOT NULL
        );
        """
    )
    ctx = _ctx(pg_client, t)
    spec = _spec()
    cmd = ctx.doc_command(spec)
    query = ctx.doc_query(spec)

    await cmd.create(RowCreate(title="low", meta=Meta(score=1, tag="p")))
    await cmd.create(RowCreate(title="high", meta=Meta(score=100, tag="q")))
    await cmd.create(RowCreate(title="mid", meta=Meta(score=50, tag="r")))

    filt = {
        "$or": [
            {"$fields": {"meta.score": {"$gte": 100}}},
            {"$fields": {"title": {"$eq": "low"}}},
        ]
    }
    __p = await query.find_page(filt,
        pagination={"limit": 10, "offset": 0},
        sorts={"meta.score": "asc"},
    )
    rows = __p.hits
    total = __p.count
    assert total == 2
    assert {r.title for r in rows} == {"low", "high"}


@pytest.mark.asyncio
async def test_filter_on_nested_string_leaf(pg_client: PostgresClient) -> None:
    t = f"nest_str_{uuid4().hex[:12]}"
    await pg_client.execute(
        f"""
        CREATE TABLE {t} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            title text NOT NULL,
            meta jsonb NOT NULL
        );
        """
    )
    ctx = _ctx(pg_client, t)
    spec = _spec()
    cmd = ctx.doc_command(spec)
    query = ctx.doc_query(spec)

    await cmd.create(RowCreate(title="x", meta=Meta(score=1, tag="gold")))
    await cmd.create(RowCreate(title="y", meta=Meta(score=2, tag="silver")))

    __p = await query.find_page({"$fields": {"meta.tag": "gold"}},
        pagination={"limit": 10, "offset": 0},
    )
    rows = __p.hits
    total = __p.count
    assert total == 1 and rows[0].title == "x"


@pytest.mark.asyncio
async def test_multi_field_sort_nested_then_scalar(pg_client: PostgresClient) -> None:
    t = f"nest_msort_{uuid4().hex[:12]}"
    await pg_client.execute(
        f"""
        CREATE TABLE {t} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            title text NOT NULL,
            meta jsonb NOT NULL
        );
        """
    )
    ctx = _ctx(pg_client, t)
    spec = _spec()
    cmd = ctx.doc_command(spec)
    query = ctx.doc_query(spec)

    await cmd.create(RowCreate(title="b", meta=Meta(score=10, tag="p")))
    await cmd.create(RowCreate(title="a", meta=Meta(score=10, tag="q")))
    await cmd.create(RowCreate(title="z", meta=Meta(score=5, tag="r")))

    __p = await query.find_page(None,
        pagination={"limit": 10, "offset": 0},
        sorts={"meta.score": "desc", "title": "asc"},
    )
    rows = __p.hits
    total = __p.count
    assert total == 3
    assert [r.title for r in rows] == ["a", "b", "z"]
