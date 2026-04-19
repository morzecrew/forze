"""Integration tests for soft delete / restore on Postgres document adapters."""

from __future__ import annotations

from uuid import uuid4

import pytest

from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
    DocumentSpec,
)
from forze.application.execution import Deps, ExecutionContext
from forze.domain.mixins import SoftDeletionMixin
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_postgres.execution.deps.deps import ConfigurablePostgresDocument
from forze_postgres.execution.deps.keys import (
    PostgresClientDepKey,
    PostgresIntrospectorDepKey,
)
from forze_postgres.kernel.introspect import PostgresIntrospector
from forze_postgres.kernel.platform.client import PostgresClient


class SoftDoc(Document, SoftDeletionMixin):
    name: str


class SoftCreate(CreateDocumentCmd):
    name: str


class SoftUpdate(BaseDTO):
    name: str | None = None


class SoftRead(ReadDocument):
    name: str
    is_deleted: bool = False


def _ctx(pg_client: PostgresClient, table: str) -> ExecutionContext:
    configurable = ConfigurablePostgresDocument(
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
                DocumentQueryDepKey: configurable,
                DocumentCommandDepKey: configurable,
            }
        )
    )


@pytest.mark.asyncio
async def test_soft_delete_restore_and_hard_kill(pg_client: PostgresClient) -> None:
    t = f"soft_doc_{uuid4().hex[:12]}"
    await pg_client.execute(
        f"""
        CREATE TABLE {t} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            name text NOT NULL,
            is_deleted boolean NOT NULL DEFAULT false
        );
        """
    )

    ctx = _ctx(pg_client, t)
    spec = DocumentSpec(
        name="soft_ns",
        read=SoftRead,
        write={
            "domain": SoftDoc,
            "create_cmd": SoftCreate,
            "update_cmd": SoftUpdate,
        },
    )
    cmd = ctx.doc_command(spec)

    doc = await cmd.create(SoftCreate(name="live"))
    assert doc.is_deleted is False

    soft = await cmd.delete(doc.id, rev=doc.rev)
    assert soft.is_deleted is True
    assert soft.rev == doc.rev + 1

    loaded = await cmd.get(doc.id)
    assert loaded.is_deleted is True

    restored = await cmd.restore(doc.id, rev=soft.rev)
    assert restored.is_deleted is False

    await cmd.kill(doc.id)
    rows = await pg_client.fetch_all(f"SELECT * FROM {t}")
    assert len(rows) == 0


@pytest.mark.asyncio
async def test_soft_delete_many_and_restore_many(pg_client: PostgresClient) -> None:
    t = f"soft_many_{uuid4().hex[:12]}"
    await pg_client.execute(
        f"""
        CREATE TABLE {t} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            name text NOT NULL,
            is_deleted boolean NOT NULL DEFAULT false
        );
        """
    )

    ctx = _ctx(pg_client, t)
    spec = DocumentSpec(
        name="soft_many_ns",
        read=SoftRead,
        write={
            "domain": SoftDoc,
            "create_cmd": SoftCreate,
            "update_cmd": SoftUpdate,
        },
    )
    cmd = ctx.doc_command(spec)

    a = await cmd.create(SoftCreate(name="a"))
    b = await cmd.create(SoftCreate(name="b"))

    deleted = await cmd.delete_many([(a.id, a.rev), (b.id, b.rev)])
    assert len(deleted) == 2
    assert all(d.is_deleted for d in deleted)

    restored = await cmd.restore_many([(d.id, d.rev) for d in deleted])
    assert len(restored) == 2
    assert not any(r.is_deleted for r in restored)
