"""Integration tests for Postgres document reads with row locking (``FOR UPDATE``)."""

from __future__ import annotations

from uuid import UUID, uuid4

import pytest

from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
    DocumentSpec,
)
from forze.application.contracts.transaction.deps import TransactionManagerDepKey
from forze.application.execution import Deps, ExecutionContext
from forze.base.exceptions import CoreException
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_postgres.execution.deps import (
    ConfigurablePostgresDocument,
    postgres_txmanager,
)
from forze_postgres.execution.deps.configs import PostgresDocumentConfig
from forze_postgres.execution.deps.keys import (
    PostgresClientDepKey,
    PostgresIntrospectorDepKey,
)
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
from forze_postgres.kernel.client.client import PostgresClient
from tests.support.execution_context import (
    context_from_deps,
)


class _Doc(Document):
    title: str

class _Create(CreateDocumentCmd):
    title: str

class _Update(BaseDTO):
    title: str | None = None

class _Read(ReadDocument):
    title: str

def _execution_context(pg_client: PostgresClient, table: str) -> ExecutionContext:
    doc = ConfigurablePostgresDocument(
        config=PostgresDocumentConfig(
            read=("public", table),
            write=("public", table),
            bookkeeping_strategy="application",
        )
    )
    plain = Deps.plain(
        {
            PostgresClientDepKey: pg_client,
            PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
            DocumentQueryDepKey: doc,
            DocumentCommandDepKey: doc,
        }
    )
    routed = Deps.routed({TransactionManagerDepKey: {"main": postgres_txmanager}})
    return context_from_deps(plain.merge(routed))

@pytest.mark.asyncio
async def test_get_for_update_requires_active_transaction(
    pg_client: PostgresClient,
) -> None:
    t = f"lock_doc_{uuid4().hex[:12]}"
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

    ctx = _execution_context(pg_client, t)
    spec = DocumentSpec(
        name="lock_ns",
        read=_Read,
        write={"domain": _Doc, "create_cmd": _Create, "update_cmd": _Update},
    )
    cmd = ctx.document.command(spec)
    doc = await cmd.create(_Create(title="row"))

    query = ctx.document.query(spec)
    with pytest.raises(CoreException, match="Transactional context is required"):
        await query.get(doc.id, for_update=True)

@pytest.mark.asyncio
async def test_get_for_update_succeeds_inside_transaction(
    pg_client: PostgresClient,
) -> None:
    t = f"lock_ok_{uuid4().hex[:12]}"
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

    ctx = _execution_context(pg_client, t)
    spec = DocumentSpec(
        name="lock_ok_ns",
        read=_Read,
        write={"domain": _Doc, "create_cmd": _Create, "update_cmd": _Update},
    )
    created = await ctx.document.command(spec).create(_Create(title="locked"))

    async with ctx.tx_ctx.scope("main"):
        query = ctx.document.query(spec)
        row = await query.get(created.id, for_update=True)
        assert row.id == created.id
        assert row.title == "locked"

@pytest.mark.asyncio
async def test_find_for_update_with_projection_inside_transaction(
    pg_client: PostgresClient,
) -> None:
    t = f"lock_find_{uuid4().hex[:12]}"
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

    ctx = _execution_context(pg_client, t)
    spec = DocumentSpec(
        name="lock_find_ns",
        read=_Read,
        write={"domain": _Doc, "create_cmd": _Create, "update_cmd": _Update},
    )
    await ctx.document.command(spec).create(_Create(title="unique-find-title"))

    async with ctx.tx_ctx.scope("main"):
        query = ctx.document.query(spec)
        proj = await query.project(
            {"$values": {"title": "unique-find-title"}},
            ("id", "rev", "title"),
            for_update=True,
        )
        assert proj is not None
        assert proj["title"] == "unique-find-title"
        assert isinstance(proj["id"], UUID)

@pytest.mark.asyncio
async def test_get_many_raises_when_any_pk_missing(
    pg_client: PostgresClient,
) -> None:
    t = f"gm_miss_{uuid4().hex[:12]}"
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

    ctx = _execution_context(pg_client, t)
    spec = DocumentSpec(
        name="gm_ns",
        read=_Read,
        write={"domain": _Doc, "create_cmd": _Create, "update_cmd": _Update},
    )
    existing = await ctx.document.command(spec).create(_Create(title="only"))
    missing = uuid4()

    query = ctx.document.query(spec)
    with pytest.raises(CoreException, match="Some records not found"):
        await query.get_many([existing.id, missing])
