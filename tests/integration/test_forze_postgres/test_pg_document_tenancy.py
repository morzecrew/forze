"""Integration tests for row-level multi-tenancy on Postgres document adapters."""

from __future__ import annotations

import pytest
from uuid import UUID, uuid4

from pydantic import Field

from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
    DocumentSpec,
)
from forze.application.contracts.authn import AuthnIdentity
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import (
    CallContext,
    Deps,
    ExecutionContext,
)
from forze.base.errors import CoreError, NotFoundError
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_postgres.execution.deps.deps import ConfigurablePostgresDocument
from forze_postgres.execution.deps.keys import (
    PostgresClientDepKey,
    PostgresIntrospectorDepKey,
)
from forze_postgres.kernel.introspect import PostgresIntrospector
from forze_postgres.kernel.platform.client import PostgresClient


class TenantDoc(Document):
    """Domain model: ``tenant_id`` is populated by the gateway on insert."""

    tenant_id: UUID | None = Field(default=None)
    name: str


class TenantCreateDoc(CreateDocumentCmd):
    name: str


class TenantUpdateDoc(BaseDTO):
    name: str | None = None


class TenantReadDoc(ReadDocument):
    tenant_id: UUID
    name: str


def _tenant_table_context(
    pg_client: PostgresClient, table: str
) -> ExecutionContext:
    configurable = ConfigurablePostgresDocument(
        config={
            "read": ("public", table),
            "write": ("public", table),
            "bookkeeping_strategy": "application",
            "tenant_aware": True,
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


def _call() -> CallContext:
    return CallContext(execution_id=uuid4(), correlation_id=uuid4())


def _spec() -> DocumentSpec:
    return DocumentSpec(
        name="tenant_docs_ns",
        read=TenantReadDoc,
        write={
            "domain": TenantDoc,
            "create_cmd": TenantCreateDoc,
            "update_cmd": TenantUpdateDoc,
        },
    )


@pytest.mark.asyncio
async def test_tenant_aware_requires_tenant_in_identity(
    pg_client: PostgresClient,
) -> None:
    table = f"tenant_docs_req_{uuid4().hex[:12]}"
    await pg_client.execute(
        f"""
        CREATE TABLE {table} (
            id uuid PRIMARY KEY,
            tenant_id uuid NOT NULL,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            name text NOT NULL
        );
        """
    )

    execution_context = _tenant_table_context(pg_client, table)
    spec = _spec()
    adapter = execution_context.doc_command(spec)

    with execution_context.bind_call(
        call=_call(),
        identity=AuthnIdentity(principal_id=uuid4()),
    ):
        with pytest.raises(CoreError, match="Tenant ID is required"):
            await adapter.create(TenantCreateDoc(name="orphan"))


@pytest.mark.asyncio
async def test_rows_are_isolated_by_tenant(pg_client: PostgresClient) -> None:
    table = f"tenant_docs_iso_{uuid4().hex[:12]}"
    await pg_client.execute(
        f"""
        CREATE TABLE {table} (
            id uuid PRIMARY KEY,
            tenant_id uuid NOT NULL,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            name text NOT NULL
        );
        """
    )

    execution_context = _tenant_table_context(pg_client, table)
    tenant_a = uuid4()
    tenant_b = uuid4()
    spec = _spec()

    with execution_context.bind_call(
        call=_call(),
        identity=AuthnIdentity(principal_id=uuid4()),
        tenancy=TenantIdentity(tenant_id=tenant_a),
    ):
        adapter = execution_context.doc_command(spec)
        doc_a = await adapter.create(TenantCreateDoc(name="alpha"))
        pk_a = doc_a.id

    row = await pg_client.fetch_one(
        f"SELECT tenant_id FROM {table} WHERE id = %s",
        [pk_a],
        row_factory="dict",
    )
    assert row is not None
    assert row["tenant_id"] == tenant_a

    with execution_context.bind_call(
        call=_call(),
        identity=AuthnIdentity(principal_id=uuid4()),
        tenancy=TenantIdentity(tenant_id=tenant_b),
    ):
        adapter = execution_context.doc_command(spec)
        doc_b = await adapter.create(TenantCreateDoc(name="beta"))

    with execution_context.bind_call(
        call=_call(),
        identity=AuthnIdentity(principal_id=uuid4()),
        tenancy=TenantIdentity(tenant_id=tenant_b),
    ):
        adapter = execution_context.doc_command(spec)
        with pytest.raises(NotFoundError):
            await adapter.get(pk_a)

        got_b = await adapter.get(doc_b.id)
        assert got_b.id == doc_b.id
        assert (await adapter.count({})) == 1

    with execution_context.bind_call(
        call=_call(),
        identity=AuthnIdentity(principal_id=uuid4()),
        tenancy=TenantIdentity(tenant_id=tenant_a),
    ):
        adapter = execution_context.doc_command(spec)
        loaded = await adapter.get(pk_a)
        assert loaded.name == "alpha"
        assert loaded.tenant_id == tenant_a
        assert (await adapter.count({})) == 1


@pytest.mark.asyncio
async def test_kill_respects_tenant_scope(pg_client: PostgresClient) -> None:
    table = f"tenant_docs_kill_{uuid4().hex[:12]}"
    await pg_client.execute(
        f"""
        CREATE TABLE {table} (
            id uuid PRIMARY KEY,
            tenant_id uuid NOT NULL,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            name text NOT NULL
        );
        """
    )

    execution_context = _tenant_table_context(pg_client, table)
    tenant_a = uuid4()
    tenant_b = uuid4()
    spec = _spec()

    with execution_context.bind_call(
        call=_call(),
        identity=AuthnIdentity(principal_id=uuid4()),
        tenancy=TenantIdentity(tenant_id=tenant_a),
    ):
        adapter = execution_context.doc_command(spec)
        doc_a = await adapter.create(TenantCreateDoc(name="kill-me"))
        pk_a = doc_a.id

    with execution_context.bind_call(
        call=_call(),
        identity=AuthnIdentity(principal_id=uuid4()),
        tenancy=TenantIdentity(tenant_id=tenant_b),
    ):
        adapter = execution_context.doc_command(spec)
        with pytest.raises(NotFoundError, match="Record not found"):
            await adapter.kill(pk_a)

    n_before = await pg_client.fetch_value(
        f"SELECT COUNT(*) FROM {table} WHERE id = %s",
        [pk_a],
        default=0,
    )
    assert int(n_before) == 1

    with execution_context.bind_call(
        call=_call(),
        identity=AuthnIdentity(principal_id=uuid4()),
        tenancy=TenantIdentity(tenant_id=tenant_a),
    ):
        adapter = execution_context.doc_command(spec)
        await adapter.kill(pk_a)

    n_after = await pg_client.fetch_value(
        f"SELECT COUNT(*) FROM {table}",
        [],
        default=0,
    )
    assert int(n_after) == 0


@pytest.mark.asyncio
async def test_update_cross_tenant_is_not_found(pg_client: PostgresClient) -> None:
    table = f"tenant_docs_upd_{uuid4().hex[:12]}"
    await pg_client.execute(
        f"""
        CREATE TABLE {table} (
            id uuid PRIMARY KEY,
            tenant_id uuid NOT NULL,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            name text NOT NULL
        );
        """
    )

    execution_context = _tenant_table_context(pg_client, table)
    tenant_a = uuid4()
    tenant_b = uuid4()
    spec = _spec()

    with execution_context.bind_call(
        call=_call(),
        identity=AuthnIdentity(principal_id=uuid4()),
        tenancy=TenantIdentity(tenant_id=tenant_a),
    ):
        adapter = execution_context.doc_command(spec)
        doc_a = await adapter.create(TenantCreateDoc(name="u"))

    with execution_context.bind_call(
        call=_call(),
        identity=AuthnIdentity(principal_id=uuid4()),
        tenancy=TenantIdentity(tenant_id=tenant_b),
    ):
        adapter = execution_context.doc_command(spec)
        with pytest.raises(NotFoundError):
            await adapter.update(doc_a.id, doc_a.rev, TenantUpdateDoc(name="hijack"))
