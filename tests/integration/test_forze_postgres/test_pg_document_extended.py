"""Integration tests for document query/batch paths on Postgres."""

from uuid import uuid4

import pytest

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


class _Doc(Document):
    name: str
    kind: str


class _Create(CreateDocumentCmd):
    name: str
    kind: str


class _Update(BaseDTO):
    name: str | None = None
    kind: str | None = None


class _Read(ReadDocument):
    name: str
    kind: str


def _ctx_for_table(pg_client: PostgresClient, table: str) -> ExecutionContext:
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
async def test_find_find_many_count_and_projections(
    pg_client: PostgresClient,
) -> None:
    """find, find_many, count, get with return_fields, and sorts."""
    t = f"ext_doc_{uuid4().hex[:12]}"
    await pg_client.execute(
        f"""
        CREATE TABLE {t} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            name text NOT NULL,
            kind text NOT NULL
        );
        """
    )

    ctx = _ctx_for_table(pg_client, t)
    spec = DocumentSpec(
        name="ext_ns",
        read=_Read,
        write={
            "domain": _Doc,
            "create_cmd": _Create,
            "update_cmd": _Update,
        },
    )
    cmd = ctx.doc_command(spec)

    a = await cmd.create(_Create(name="alpha", kind="a"))
    b = await cmd.create(_Create(name="beta", kind="b"))
    await cmd.create(_Create(name="gamma", kind="a"))

    found = await cmd.find({"$fields": {"name": "beta"}})
    assert found is not None
    assert found.name == "beta"

    rows, total = await cmd.find_many(
        {"$fields": {"kind": "a"}},
        limit=10,
        offset=0,
        sorts={"name": "asc"},
    )
    assert total == 2
    assert [r.name for r in rows] == ["alpha", "gamma"]

    assert await cmd.count({"$fields": {"kind": "b"}}) == 1

    row = await cmd.get(a.id, return_fields=("name", "kind"))
    assert row == {"name": "alpha", "kind": "a"}

    many = await cmd.get_many([a.id, b.id])
    assert len(many) == 2
    assert {x.id for x in many} == {a.id, b.id}

    assert await cmd.get_many([]) == []


@pytest.mark.asyncio
async def test_create_update_touch_kill_many(
    pg_client: PostgresClient,
) -> None:
    """create_many, update_many, touch_many, kill_many."""
    t = f"ext_batch_{uuid4().hex[:12]}"
    await pg_client.execute(
        f"""
        CREATE TABLE {t} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            name text NOT NULL,
            kind text NOT NULL
        );
        """
    )

    ctx = _ctx_for_table(pg_client, t)
    spec = DocumentSpec(
        name="batch_ns",
        read=_Read,
        write={
            "domain": _Doc,
            "create_cmd": _Create,
            "update_cmd": _Update,
        },
    )
    cmd = ctx.doc_command(spec)

    created = await cmd.create_many(
        [
            _Create(name="n1", kind="k"),
            _Create(name="n2", kind="k"),
        ]
    )
    assert len(created) == 2

    u1 = await cmd.update(created[0].id, created[0].rev, _Update(name="n1x"))
    u2 = await cmd.update(created[1].id, created[1].rev, _Update(name="n2x"))
    assert u1.name == "n1x" and u2.name == "n2x"

    t1 = await cmd.touch(created[0].id)
    t2 = await cmd.touch(created[1].id)
    assert t1.rev == 3 and t2.rev == 3

    await cmd.kill_many([created[0].id, created[1].id])
    assert await cmd.count() == 0


@pytest.mark.asyncio
async def test_empty_create_and_update_shortcuts(
    pg_client: PostgresClient,
) -> None:
    """Empty create_many / update_many / touch_many / kill_many are no-ops."""
    t = f"ext_empty_{uuid4().hex[:12]}"
    await pg_client.execute(
        f"""
        CREATE TABLE {t} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            name text NOT NULL,
            kind text NOT NULL
        );
        """
    )

    ctx = _ctx_for_table(pg_client, t)
    spec = DocumentSpec(
        name="empty_ns",
        read=_Read,
        write={
            "domain": _Doc,
            "create_cmd": _Create,
            "update_cmd": _Update,
        },
    )
    cmd = ctx.doc_command(spec)

    assert await cmd.create_many([]) == []
    assert await cmd.update_many([]) == []
    assert await cmd.touch_many([]) == []
    assert await cmd.kill_many([]) is None
