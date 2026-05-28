"""Integration tests for :class:`~forze_postgres.kernel.gateways.write.PostgresWriteGateway` with a real Postgres instance."""

from datetime import datetime
from uuid import UUID, uuid4

import pytest

from forze.application.contracts.document import DocumentWriteTypes
from forze.application.execution import Deps, ExecutionContext
from forze.base.exceptions import CoreException, ExceptionKind
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document
from forze_postgres.execution.deps.keys import (
    PostgresClientDepKey,
    PostgresIntrospectorDepKey,
)
from forze_postgres.execution.deps.utils import doc_write_gw
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
from forze_postgres.kernel.client.client import PostgresClient


class PgGwDoc(Document):
    name: str


class PgGwCreate(CreateDocumentCmd):
    name: str


class PgGwUpdate(BaseDTO):
    name: str | None = None


def _write_types() -> DocumentWriteTypes[PgGwDoc, PgGwCreate, PgGwUpdate]:
    return DocumentWriteTypes(
        domain=PgGwDoc,
        create_cmd=PgGwCreate,
        update_cmd=PgGwUpdate,
    )


class PgGwDocDeadline(Document):
    name: str
    deadline: datetime | None = None


class PgGwCreateDeadline(CreateDocumentCmd):
    name: str
    deadline: datetime | None = None


class PgGwUpdateDeadline(BaseDTO):
    deadline: datetime | None = None


def _write_types_deadline() -> (
    DocumentWriteTypes[PgGwDocDeadline, PgGwCreateDeadline, PgGwUpdateDeadline]
):
    return DocumentWriteTypes(
        domain=PgGwDocDeadline,
        create_cmd=PgGwCreateDeadline,
        update_cmd=PgGwUpdateDeadline,
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_postgres_write_gateway_roundtrip_and_projections(
    pg_client: PostgresClient,
) -> None:
    """Create/update via write gateway; read with projections and list bounds."""
    table = f"pg_gw_{uuid4().hex[:8]}"
    await pg_client.execute(
        f"""
        CREATE TABLE public.{table} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            name text NOT NULL
        );
        """
    )

    ctx = ExecutionContext(
        deps=Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
            }
        )
    )

    write = doc_write_gw(
        ctx,
        write_types=_write_types(),
        write_relation=("public", table),
        bookkeeping_strategy="application",
        tenant_aware=False,
    )
    read = write.read_gw

    created = await write.create(PgGwCreate(name="pg-gw"))
    assert created.name == "pg-gw"
    assert created.rev == 1

    by_id = await read.get(created.id)
    assert by_id.name == "pg-gw"

    await write.create(PgGwCreate(name="other-row"))

    rows = await read.find_many(limit=5)
    assert len(rows) >= 2

    n = await read.count(None)
    assert n >= 2

    updated, _ = await write.update(created.id, PgGwUpdate(name="renamed"))
    assert updated.name == "renamed"
    assert updated.rev == 2


@pytest.mark.integration
@pytest.mark.asyncio
async def test_postgres_write_gateway_upsert_insert_then_conflict_updates(
    pg_client: PostgresClient,
) -> None:
    """``upsert`` inserts on first call and applies ``update_dto`` when the PK already exists."""
    table = f"pg_gw_up_{uuid4().hex[:8]}"
    await pg_client.execute(
        f"""
        CREATE TABLE public.{table} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            name text NOT NULL
        );
        """
    )

    pk = UUID(int=0x12345678123456781234567812345678)
    ctx = ExecutionContext(
        deps=Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
            }
        )
    )
    write = doc_write_gw(
        ctx,
        write_types=_write_types(),
        write_relation=("public", table),
        bookkeeping_strategy="application",
        tenant_aware=False,
    )

    first = await write.upsert(
        PgGwCreate(id=pk, name="inserted"),
        PgGwUpdate(name="should-not-apply-on-insert"),
    )
    assert first.id == pk
    assert first.name == "inserted"
    assert first.rev == 1

    second = await write.upsert(
        PgGwCreate(id=pk, name="ignored-on-conflict"),
        PgGwUpdate(name="after-conflict"),
    )
    assert second.id == pk
    assert second.name == "after-conflict"
    assert second.rev == 2


@pytest.mark.integration
@pytest.mark.asyncio
async def test_postgres_write_gateway_upsert_many_mixed_batch(
    pg_client: PostgresClient,
) -> None:
    """``upsert_many`` inserts fresh rows and updates existing ones in the same batch."""
    table = f"pg_gw_um_{uuid4().hex[:8]}"
    await pg_client.execute(
        f"""
        CREATE TABLE public.{table} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            name text NOT NULL
        );
        """
    )

    ctx = ExecutionContext(
        deps=Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
            }
        )
    )
    write = doc_write_gw(
        ctx,
        write_types=_write_types(),
        write_relation=("public", table),
        bookkeeping_strategy="application",
        tenant_aware=False,
    )

    existing = await write.create(PgGwCreate(name="seed"))

    pairs = [
        (PgGwCreate(name="brand-new-one"), PgGwUpdate(name="n/a")),
        (
            PgGwCreate(id=existing.id, name="ignored"),
            PgGwUpdate(name="patched-existing"),
        ),
    ]
    out = await write.upsert_many(pairs, batch_size=10)
    assert len(out) == 2

    by_id = {d.id: d for d in out}
    assert by_id[existing.id].name == "patched-existing"
    assert by_id[existing.id].rev >= 2

    fresh_id = next(i for i in by_id if i != existing.id)
    assert by_id[fresh_id].name == "brand-new-one"
    assert by_id[fresh_id].rev == 1


@pytest.mark.integration
@pytest.mark.asyncio
async def test_postgres_write_gateway_ensure_returns_existing_row(
    pg_client: PostgresClient,
) -> None:
    """``ensure`` uses ``ON CONFLICT DO NOTHING`` and reads the row when the PK exists."""
    table = f"pg_gw_en_{uuid4().hex[:8]}"
    await pg_client.execute(
        f"""
        CREATE TABLE public.{table} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            name text NOT NULL
        );
        """
    )

    pk = UUID(int=0xABCDEF0123456789ABCDEF0123456789)
    ctx = ExecutionContext(
        deps=Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
            }
        )
    )
    write = doc_write_gw(
        ctx,
        write_types=_write_types(),
        write_relation=("public", table),
        bookkeeping_strategy="application",
        tenant_aware=False,
    )

    first = await write.create(PgGwCreate(id=pk, name="original"))
    second = await write.ensure(PgGwCreate(id=pk, name="ignored"))
    assert second.id == pk
    assert second.name == "original"
    assert second.rev == first.rev


@pytest.mark.integration
@pytest.mark.asyncio
async def test_postgres_write_gateway_ensure_many_inserts_and_reuses_existing(
    pg_client: PostgresClient,
) -> None:
    table = f"pg_gw_em_{uuid4().hex[:8]}"
    await pg_client.execute(
        f"""
        CREATE TABLE public.{table} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            name text NOT NULL
        );
        """
    )

    ctx = ExecutionContext(
        deps=Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
            }
        )
    )
    write = doc_write_gw(
        ctx,
        write_types=_write_types(),
        write_relation=("public", table),
        bookkeeping_strategy="application",
        tenant_aware=False,
    )

    seed = await write.create(PgGwCreate(name="seed"))
    new_id = uuid4()
    out = await write.ensure_many(
        [
            PgGwCreate(id=new_id, name="inserted"),
            PgGwCreate(id=seed.id, name="ignored"),
        ],
        batch_size=10,
    )
    assert len(out) == 2
    by_id = {d.id: d for d in out}
    assert by_id[new_id].name == "inserted"
    assert by_id[seed.id].id == seed.id
    assert by_id[seed.id].name == "seed"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_postgres_write_gateway_create_many_empty(
    pg_client: PostgresClient,
) -> None:
    table = f"pg_gw_ce_{uuid4().hex[:8]}"
    await pg_client.execute(
        f"""
        CREATE TABLE public.{table} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            name text NOT NULL
        );
        """
    )

    ctx = ExecutionContext(
        deps=Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
            }
        )
    )
    write = doc_write_gw(
        ctx,
        write_types=_write_types(),
        write_relation=("public", table),
        bookkeeping_strategy="application",
        tenant_aware=False,
    )

    assert await write.create_many([], batch_size=50) == []


@pytest.mark.integration
@pytest.mark.asyncio
async def test_postgres_write_gateway_update_many_empty_diff_skips_batch_update(
    pg_client: PostgresClient,
) -> None:
    """Updates whose DTOs produce an empty diff leave rows unchanged (no ``UPDATE`` batch)."""
    table = f"pg_gw_ud_{uuid4().hex[:8]}"
    await pg_client.execute(
        f"""
        CREATE TABLE public.{table} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            name text NOT NULL
        );
        """
    )

    ctx = ExecutionContext(
        deps=Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
            }
        )
    )
    write = doc_write_gw(
        ctx,
        write_types=_write_types(),
        write_relation=("public", table),
        bookkeeping_strategy="application",
        tenant_aware=False,
    )

    a = await write.create(PgGwCreate(name="a"))
    b = await write.create(PgGwCreate(name="b"))
    res, diffs = await write.update_many(
        [a.id, b.id],
        [PgGwUpdate(), PgGwUpdate()],
        revs=[a.rev, b.rev],
        batch_size=10,
    )
    assert [d.id for d in res] == [a.id, b.id]
    assert res[0].rev == a.rev and res[1].rev == b.rev
    assert diffs == [{}, {}]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_postgres_write_gateway_touch_many(
    pg_client: PostgresClient,
) -> None:
    table = f"pg_gw_tm_{uuid4().hex[:8]}"
    await pg_client.execute(
        f"""
        CREATE TABLE public.{table} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            name text NOT NULL
        );
        """
    )

    ctx = ExecutionContext(
        deps=Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
            }
        )
    )
    write = doc_write_gw(
        ctx,
        write_types=_write_types(),
        write_relation=("public", table),
        bookkeeping_strategy="application",
        tenant_aware=False,
    )

    a = await write.create(PgGwCreate(name="x"))
    b = await write.create(PgGwCreate(name="y"))
    touched = await write.touch_many([a.id, b.id], batch_size=10)
    assert len(touched) == 2
    assert all(t.rev == 2 for t in touched)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_postgres_write_gateway_kill_many_empty_is_noop(
    pg_client: PostgresClient,
) -> None:
    table = f"pg_gw_km_{uuid4().hex[:8]}"
    await pg_client.execute(
        f"""
        CREATE TABLE public.{table} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            name text NOT NULL
        );
        """
    )

    ctx = ExecutionContext(
        deps=Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
            }
        )
    )
    write = doc_write_gw(
        ctx,
        write_types=_write_types(),
        write_relation=("public", table),
        bookkeeping_strategy="application",
        tenant_aware=False,
    )

    await write.kill_many([])


@pytest.mark.integration
@pytest.mark.asyncio
async def test_postgres_write_gateway_create_many_empty_is_noop(
    pg_client: PostgresClient,
) -> None:
    table = f"pg_gw_cme_{uuid4().hex[:8]}"
    await pg_client.execute(
        f"""
        CREATE TABLE public.{table} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            name text NOT NULL
        );
        """
    )
    ctx = ExecutionContext(
        deps=Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
            }
        )
    )
    write = doc_write_gw(
        ctx,
        write_types=_write_types(),
        write_relation=("public", table),
        bookkeeping_strategy="application",
        tenant_aware=False,
    )
    assert await write.create_many(()) == []
    assert await write.create_many([]) == []


@pytest.mark.integration
@pytest.mark.asyncio
async def test_postgres_write_gateway_update_many_nullable_timestamptz_all_null(
    pg_client: PostgresClient,
) -> None:
    """Batched ``UPDATE … FROM (VALUES…)`` must type ``NULL`` cells (not infer ``text``).

    PostgreSQL assigns ``text`` to a ``VALUES`` column whose expressions are all
    untyped nulls; ``SET deadline = v.deadline`` then fails for ``timestamptz``.
    """

    table = f"pg_gw_ts_{uuid4().hex[:8]}"
    await pg_client.execute(
        f"""
        CREATE TABLE public.{table} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            name text NOT NULL,
            deadline timestamptz
        );
        """
    )

    ctx = ExecutionContext(
        deps=Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
            }
        )
    )
    write = doc_write_gw(
        ctx,
        write_types=_write_types_deadline(),
        write_relation=("public", table),
        bookkeeping_strategy="application",
        tenant_aware=False,
    )
    read = write.read_gw

    a = await write.create(PgGwCreateDeadline(name="a"))
    b = await write.create(PgGwCreateDeadline(name="b"))
    assert a.deadline is None and b.deadline is None

    res, _ = await write.update_many(
        [a.id, b.id],
        [PgGwUpdateDeadline(deadline=None), PgGwUpdateDeadline(deadline=None)],
        revs=[a.rev, b.rev],
        batch_size=10,
    )
    assert len(res) == 2
    assert all(r.deadline is None for r in res)

    ra = await read.get(a.id)
    rb = await read.get(b.id)
    assert ra.deadline is None and rb.deadline is None


class PgGwDocTenant(Document):
    tenant_id: UUID
    name: str
    email: str


class PgGwCreateTenant(CreateDocumentCmd):
    tenant_id: UUID
    name: str
    email: str


class PgGwUpdateTenant(BaseDTO):
    name: str | None = None


def _write_types_tenant() -> (
    DocumentWriteTypes[PgGwDocTenant, PgGwCreateTenant, PgGwUpdateTenant]
):
    return DocumentWriteTypes(
        domain=PgGwDocTenant,
        create_cmd=PgGwCreateTenant,
        update_cmd=PgGwUpdateTenant,
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_postgres_write_gateway_ensure_composite_pk_and_secondary_unique(
    pg_client: PostgresClient,
) -> None:
    """``ensure``/``upsert`` use inferred composite PK; secondary UNIQUE still enforces."""
    table = f"pg_gw_ct_{uuid4().hex[:8]}"
    tenant_id = uuid4()
    await pg_client.execute(
        f"""
        CREATE TABLE public.{table} (
            tenant_id uuid NOT NULL,
            id uuid NOT NULL,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            name text NOT NULL,
            email text NOT NULL,
            PRIMARY KEY (tenant_id, id),
            UNIQUE (email)
        );
        """
    )

    ctx = ExecutionContext(
        deps=Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
            }
        )
    )
    write = doc_write_gw(
        ctx,
        write_types=_write_types_tenant(),
        write_relation=("public", table),
        bookkeeping_strategy="application",
        tenant_aware=False,
    )

    doc_id = uuid4()
    seeded = await write.create(
        PgGwCreateTenant(
            id=doc_id,
            tenant_id=tenant_id,
            name="seeded",
            email="a@example.com",
        ),
    )

    second = await write.ensure(
        PgGwCreateTenant(
            id=doc_id,
            tenant_id=tenant_id,
            name="ignored",
            email="other@example.com",
        ),
    )
    assert second.id == doc_id
    assert second.name == "seeded"
    assert second.email == "a@example.com"
    assert second.rev == seeded.rev

    dup_email_id = uuid4()
    with pytest.raises(CoreException) as err:
        await write.ensure(
            PgGwCreateTenant(
                id=dup_email_id,
                tenant_id=tenant_id,
                name="dup",
                email="a@example.com",
            ),
        )
    assert err.value.kind == ExceptionKind.CONFLICT or (
        err.value.details.get("exc_type") == "UniqueViolation"
    )

    patched = await write.upsert(
        PgGwCreateTenant(
            id=doc_id,
            tenant_id=tenant_id,
            name="ignored",
            email="ignored",
        ),
        PgGwUpdateTenant(name="patched"),
    )
    assert patched.name == "patched"
    assert patched.email == "a@example.com"
