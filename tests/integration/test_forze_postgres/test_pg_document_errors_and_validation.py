"""Integration tests for Postgres document error paths and domain validation."""

from __future__ import annotations

from forze.base.exceptions import CoreException
from uuid import uuid4

import pytest

from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
    DocumentSpec,
)
from forze.application.execution import Deps, ExecutionContext
from forze_patterns.soft_deletion.models import DocWithSoftDeletion, UpdateCmdWithSoftDeletion
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_postgres.execution.deps import ConfigurablePostgresDocument
from forze_postgres.execution.deps.keys import (
    PostgresClientDepKey,
    PostgresIntrospectorDepKey,
)
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
from forze_postgres.kernel.client.client import PostgresClient
from forze_postgres.execution.deps.configs import PostgresDocumentConfig
from tests.support.execution_context import context_from_deps

class _Doc(Document):
    title: str

class _Create(CreateDocumentCmd):
    title: str

class _Update(BaseDTO):
    title: str | None = None

class _Read(ReadDocument):
    title: str

class _SoftDoc(DocWithSoftDeletion):
    title: str

class _SoftUpdate(UpdateCmdWithSoftDeletion):
    title: str | None = None

class _SoftRead(ReadDocument):
    title: str
    is_deleted: bool = False

def _ctx(pg_client: PostgresClient, table: str) -> ExecutionContext:
    doc = ConfigurablePostgresDocument(
        config=PostgresDocumentConfig(
            read=("public", table),
            write=("public", table),
            bookkeeping_strategy="application",
        )
    )
    return context_from_deps(Deps.plain(
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
    query = ctx.document.query(_spec())
    with pytest.raises(CoreException, match="Record not found"):
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
    query = ctx.document.query(_spec())
    assert (
        await query.find({"$values": {"title": "does-not-exist"}}) is None
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
    cmd = ctx.document.command(_spec())
    doc = await cmd.create(_Create(title="v1"))
    await cmd.update(doc.id, doc.rev, _Update(title="v2"))
    with pytest.raises(CoreException, match="Revision mismatch"):
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
    cmd = ctx.document.command(_spec())
    with pytest.raises(CoreException):
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
        write={"domain": _SoftDoc, "create_cmd": _Create, "update_cmd": _SoftUpdate},
    )
    ctx = _ctx(pg_client, t)
    cmd = ctx.document.command(spec)
    doc = await cmd.create(_Create(title="live"))
    deleted = await cmd.update(doc.id, doc.rev, _SoftUpdate(is_deleted=True))
    with pytest.raises(CoreException, match="soft-deleted"):
        await cmd.update(deleted.id, deleted.rev, _SoftUpdate(title="nope"))

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
    query = ctx.document.query(spec)
    assert await query.count() == 0
    assert await query.count(None) == 0
    __p = await query.find_page(
        None,
        pagination={"limit": 10, "offset": 0},
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
    cmd = ctx.document.command(_spec())
    await cmd.create(_Create(title="red"))
    await cmd.create(_Create(title="blue"))
    await cmd.create(_Create(title="red"))
    query = ctx.document.query(_spec())
    assert await query.count({"$values": {"title": "red"}}) == 2
    assert await query.count({"$values": {"title": "green"}}) == 0
