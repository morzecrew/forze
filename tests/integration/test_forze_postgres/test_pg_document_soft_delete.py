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
from forze.domain.models import CreateDocumentCmd, ReadDocument
from forze_kits.domain.soft_deletion.models import DocWithSoftDeletion, UpdateCmdWithSoftDeletion
from forze_postgres.execution.deps import ConfigurablePostgresDocument
from forze_postgres.execution.deps.configs import PostgresDocumentConfig
from forze_postgres.execution.deps.keys import (
    PostgresClientDepKey,
    PostgresIntrospectorDepKey,
)
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
from forze_postgres.kernel.client.client import PostgresClient
from tests.support.execution_context import context_from_deps

# ----------------------- #


class SoftDoc(DocWithSoftDeletion):
    name: str


class SoftCreate(CreateDocumentCmd):
    name: str


class SoftUpdate(UpdateCmdWithSoftDeletion):
    name: str | None = None


class SoftRead(ReadDocument):
    name: str
    is_deleted: bool = False


def _ctx(pg_client: PostgresClient, table: str) -> ExecutionContext:
    configurable = ConfigurablePostgresDocument(
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
    cmd = ctx.document.command(spec)

    doc = await cmd.create(SoftCreate(name="live"))
    assert doc.is_deleted is False

    soft = await cmd.update(doc.id, doc.rev, SoftUpdate(is_deleted=True))
    assert soft.is_deleted is True
    assert soft.rev == doc.rev + 1

    loaded = await cmd.get(doc.id)
    assert loaded.is_deleted is True

    restored = await cmd.update(doc.id, soft.rev, SoftUpdate(is_deleted=False))
    assert restored.is_deleted is False

    await cmd.kill(doc.id)
    rows = await pg_client.fetch_all(f"SELECT * FROM {t}")
    assert len(rows) == 0


@pytest.mark.asyncio
async def test_soft_delete_many_via_sequential_updates(pg_client: PostgresClient) -> None:
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
    cmd = ctx.document.command(spec)

    a = await cmd.create(SoftCreate(name="a"))
    b = await cmd.create(SoftCreate(name="b"))

    da = await cmd.update(a.id, a.rev, SoftUpdate(is_deleted=True))
    db = await cmd.update(b.id, b.rev, SoftUpdate(is_deleted=True))
    assert da.is_deleted and db.is_deleted

    ra = await cmd.update(a.id, da.rev, SoftUpdate(is_deleted=False))
    rb = await cmd.update(b.id, db.rev, SoftUpdate(is_deleted=False))
    assert not ra.is_deleted and not rb.is_deleted
