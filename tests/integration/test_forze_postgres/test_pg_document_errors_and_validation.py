"""Integration tests for Postgres document error paths and domain validation."""

from __future__ import annotations

from uuid import uuid4

import pytest

from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
    DocumentSpec,
)
from forze.application.execution import Deps, ExecutionContext
from forze.base.errors import ConflictError, NotFoundError, ValidationError
from forze.domain.mixins import SoftDeletionMixin
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_postgres.execution.deps.deps import ConfigurablePostgresDocument
from forze_postgres.execution.deps.keys import (
    PostgresClientDepKey,
    PostgresIntrospectorDepKey,
)
from forze_postgres.kernel.introspect import PostgresIntrospector
from forze_postgres.kernel.platform.client import PostgresClient


class _Doc(Document):
    title: str


class _Create(CreateDocumentCmd):
    title: str


class _Update(BaseDTO):
    title: str | None = None


class _Read(ReadDocument):
    title: str


class _SoftDoc(Document, SoftDeletionMixin):
    title: str


class _SoftRead(ReadDocument):
    title: str
    is_deleted: bool = False


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
        name="err_ns",
        read=_Read,
        write={"domain": _Doc, "create_cmd": _Create, "update_cmd": _Update},
    )


@pytest.mark.asyncio
async def test_get_missing_raises_not_found(pg_client: PostgresClient) -> None:
    t = f"e_get_{uuid4().hex[:12]}"
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
    ctx = _ctx(pg_client, t)
    query = ctx.doc_query(_spec())
    with pytest.raises(NotFoundError, match="Record not found"):
        await query.get(uuid4())


@pytest.mark.asyncio
async def test_find_missing_returns_none(pg_client: PostgresClient) -> None:
    t = f"e_find_{uuid4().hex[:12]}"
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
    ctx = _ctx(pg_client, t)
    query = ctx.doc_query(_spec())
    assert (
        await query.find({"$fields": {"title": "does-not-exist"}}) is None
    )


@pytest.mark.asyncio
async def test_update_with_stale_rev_raises_conflict(pg_client: PostgresClient) -> None:
    t = f"e_rev_{uuid4().hex[:12]}"
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
    ctx = _ctx(pg_client, t)
    cmd = ctx.doc_command(_spec())
    doc = await cmd.create(_Create(title="v1"))
    await cmd.update(doc.id, doc.rev, _Update(title="v2"))
    with pytest.raises(ConflictError, match="Revision mismatch"):
        await cmd.update(doc.id, 1, _Update(title="stale"))


@pytest.mark.asyncio
async def test_touch_missing_raises_not_found(pg_client: PostgresClient) -> None:
    t = f"e_touch_{uuid4().hex[:12]}"
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
    ctx = _ctx(pg_client, t)
    cmd = ctx.doc_command(_spec())
    with pytest.raises(NotFoundError):
        await cmd.touch(uuid4())


@pytest.mark.asyncio
async def test_cannot_update_non_deleted_fields_when_soft_deleted(
    pg_client: PostgresClient,
) -> None:
    t = f"e_soft_{uuid4().hex[:12]}"
    await pg_client.execute(
        f"""
        CREATE TABLE {t} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            title text NOT NULL,
            is_deleted boolean NOT NULL DEFAULT false
        );
        """
    )
    spec = DocumentSpec(
        name="soft_err_ns",
        read=_SoftRead,
        write={"domain": _SoftDoc, "create_cmd": _Create, "update_cmd": _Update},
    )
    ctx = _ctx(pg_client, t)
    cmd = ctx.doc_command(spec)
    doc = await cmd.create(_Create(title="live"))
    deleted = await cmd.delete(doc.id, rev=doc.rev)
    with pytest.raises(ValidationError, match="soft-deleted"):
        await cmd.update(deleted.id, deleted.rev, _Update(title="nope"))


@pytest.mark.asyncio
async def test_count_and_find_many_on_empty_table(pg_client: PostgresClient) -> None:
    t = f"e_empty_{uuid4().hex[:12]}"
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
    ctx = _ctx(pg_client, t)
    spec = _spec()
    query = ctx.doc_query(spec)
    assert await query.count() == 0
    assert await query.count(None) == 0
    __p = await query.find_many(
        None, pagination={"limit": 10, "offset": 0}, return_count=True
    )
    rows = __p.hits
    total = __p.count
    assert rows == [] and total == 0


@pytest.mark.asyncio
async def test_count_with_field_filter(pg_client: PostgresClient) -> None:
    t = f"e_cnt_{uuid4().hex[:12]}"
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
    ctx = _ctx(pg_client, t)
    cmd = ctx.doc_command(_spec())
    await cmd.create(_Create(title="red"))
    await cmd.create(_Create(title="blue"))
    await cmd.create(_Create(title="red"))
    query = ctx.doc_query(_spec())
    assert await query.count({"$fields": {"title": "red"}}) == 2
    assert await query.count({"$fields": {"title": "green"}}) == 0
