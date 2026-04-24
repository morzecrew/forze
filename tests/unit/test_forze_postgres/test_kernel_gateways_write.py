"""Unit tests for ``forze_postgres.kernel.gateways.write``."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import pytest

from forze.base.errors import ConcurrencyError, NotFoundError
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document
from forze_postgres.kernel.gateways import (
    PostgresQualifiedName,
    PostgresReadGateway,
    PostgresWriteGateway,
)
from forze_postgres.kernel.introspect import PostgresIntrospector
from forze_postgres.kernel.platform import PostgresClient


class MyDoc(Document):
    name: str


class MyCreateDoc(CreateDocumentCmd):
    name: str


class MyUpdateDoc(BaseDTO):
    name: str | None = None


def _build_gateway() -> (
    tuple[PostgresWriteGateway[MyDoc, MyCreateDoc, MyUpdateDoc], MagicMock]
):
    client = MagicMock(spec=PostgresClient)
    client.fetch_all = AsyncMock()

    introspector = MagicMock(spec=PostgresIntrospector)
    introspector.get_column_types = AsyncMock(return_value={})

    qname = PostgresQualifiedName(schema="public", name="docs")
    read = MagicMock(spec=PostgresReadGateway)
    read.source_qname = qname
    read.client = client
    read.tenant_aware = False

    gw = PostgresWriteGateway(
        source_qname=qname,
        client=client,
        model_type=MyDoc,
        introspector=introspector,
        read_gw=read,
        create_cmd_type=MyCreateDoc,
        update_cmd_type=MyUpdateDoc,
        strategy="application",
    )
    return gw, client


def _row(*, pk: UUID, name: str, ts: datetime) -> dict[str, object]:
    return {
        "id": pk,
        "rev": 1,
        "created_at": ts,
        "last_update_at": ts,
        "name": name,
    }


@pytest.mark.asyncio
async def test_create_many_batches_and_preserves_parameter_order() -> None:
    gw, client = _build_gateway()
    ts = datetime(2025, 1, 1, tzinfo=UTC)

    id1 = UUID("11111111-1111-1111-1111-111111111111")
    id2 = UUID("22222222-2222-2222-2222-222222222222")
    id3 = UUID("33333333-3333-3333-3333-333333333333")

    dtos = [
        MyCreateDoc(id=id1, created_at=ts, name="first"),
        MyCreateDoc(id=id2, created_at=ts, name="second"),
        MyCreateDoc(id=id3, created_at=ts, name="third"),
    ]

    client.fetch_all.side_effect = [
        [_row(pk=id1, name="first", ts=ts), _row(pk=id2, name="second", ts=ts)],
        [_row(pk=id3, name="third", ts=ts)],
    ]

    result = await gw.create_many(dtos, batch_size=2)

    assert client.fetch_all.await_count == 2
    calls = client.fetch_all.await_args_list
    params_by_uuids: list[list[UUID]] = []
    for call in calls:
        params = call.args[1]
        params_by_uuids.append([x for x in params if isinstance(x, UUID)])
        assert call.kwargs["row_factory"] == "dict"
        assert call.kwargs["commit"] is True

    assert sorted(params_by_uuids, key=lambda u: u[0] if u else id1) == [
        [id1, id2],
        [id3],
    ]
    name_chunks: list[list[str]] = []
    for call in calls:
        params = call.args[1]
        name_chunks.append([x for x in params if x in {"first", "second", "third"}])
    assert sorted(name_chunks, key=lambda ns: ns[0] if ns else "") == [
        ["first", "second"],
        ["third"],
    ]

    assert [x.id for x in result] == [id1, id2, id3]
    assert [x.name for x in result] == ["first", "second", "third"]


@pytest.mark.asyncio
async def test_create_many_raises_when_batch_returns_fewer_rows() -> None:
    gw, client = _build_gateway()
    ts = datetime(2025, 1, 1, tzinfo=UTC)

    id1 = UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
    id2 = UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
    dtos = [
        MyCreateDoc(id=id1, created_at=ts, name="first"),
        MyCreateDoc(id=id2, created_at=ts, name="second"),
    ]

    client.fetch_all.return_value = [_row(pk=id1, name="first", ts=ts)]

    with pytest.raises(ConcurrencyError, match="mismatch in number of rows"):
        await gw.create_many(dtos, batch_size=100)


@pytest.mark.asyncio
async def test_ensure_many_skips_conflicts_and_loads_existing() -> None:
    gw, client = _build_gateway()
    read = gw.read_gw
    read.get = AsyncMock()
    read.get_many = AsyncMock()
    ts = datetime(2025, 1, 1, tzinfo=UTC)
    id1 = UUID("11111111-1111-1111-1111-111111111111")
    id2 = UUID("22222222-2222-2222-2222-222222222222")
    conflict_row = _row(pk=id1, name="unchanged", ts=ts)
    insert_row = _row(pk=id2, name="inserted", ts=ts)
    dtos = [
        MyCreateDoc(id=id1, created_at=ts, name="try-overwrite"),
        MyCreateDoc(id=id2, created_at=ts, name="inserted"),
    ]
    client.fetch_all.return_value = [insert_row]
    read.get_many = AsyncMock(
        return_value=[
            MyDoc(
                id=id1,
                rev=1,
                created_at=ts,
                last_update_at=ts,
                name=conflict_row["name"],  # type: ignore[arg-type]
            )
        ],
    )
    out = await gw.ensure_many(dtos, batch_size=20)
    assert [d.id for d in out] == [id1, id2]
    assert out[0].name == "unchanged"
    assert out[1].name == "inserted"
    read.get_many.assert_awaited_once_with([id1])


@pytest.mark.asyncio
async def test_upsert_inserts_or_updates() -> None:
    gw, client = _build_gateway()
    read = gw.read_gw
    read.get = AsyncMock()
    read.get_many = AsyncMock()
    ts = datetime(2025, 1, 1, tzinfo=UTC)
    id1 = UUID("11111111-1111-1111-1111-111111111111")
    id2 = UUID("22222222-2222-2222-2222-222222222222")
    c1 = MyCreateDoc(id=id1, created_at=ts, name="one")
    c2 = MyCreateDoc(id=id2, created_at=ts, name="two")
    u2 = MyUpdateDoc(name="patched")
    new_row = _row(pk=id1, name="one", ts=ts)
    existing = MyDoc(
        id=id2,
        rev=1,
        created_at=ts,
        last_update_at=ts,
        name="old",
    )
    updated_row = {
        "id": id2,
        "rev": 2,
        "created_at": ts,
        "last_update_at": ts,
        "name": "patched",
    }
    read.get.return_value = existing
    client.fetch_one = AsyncMock(
        side_effect=[
            new_row,
            None,
            updated_row,
        ]
    )

    out1 = await gw.upsert(c1, MyUpdateDoc())
    assert out1.id == id1
    assert out1.name == "one"

    out2 = await gw.upsert(c2, u2)
    assert out2.id == id2
    assert out2.name == "patched"
    assert out2.rev == 2
    assert read.get.await_count >= 1
    assert any(call.args[0] == id2 for call in read.get.await_args_list)


def _build_tenant_aware_gateway() -> (
    tuple[PostgresWriteGateway[MyDoc, MyCreateDoc, MyUpdateDoc], MagicMock]
):
    tid = UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
    client = MagicMock(spec=PostgresClient)
    client.execute = AsyncMock(return_value=1)

    introspector = MagicMock(spec=PostgresIntrospector)
    introspector.get_column_types = AsyncMock(return_value={})

    qname = PostgresQualifiedName(schema="public", name="docs")
    read = MagicMock(spec=PostgresReadGateway)
    read.source_qname = qname
    read.client = client
    read.tenant_aware = True

    gw = PostgresWriteGateway(
        source_qname=qname,
        client=client,
        model_type=MyDoc,
        introspector=introspector,
        read_gw=read,
        create_cmd_type=MyCreateDoc,
        update_cmd_type=MyUpdateDoc,
        strategy="application",
        tenant_aware=True,
        tenant_provider=lambda: tid,
    )
    return gw, client


@pytest.mark.asyncio
async def test_kill_tenant_aware_uses_rowcount_and_raises_when_missing() -> None:
    gw, client = _build_tenant_aware_gateway()
    pk = UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")

    await gw.kill(pk)

    client.execute.assert_awaited_once()
    assert client.execute.await_args.kwargs.get("return_rowcount") is True

    client.execute = AsyncMock(return_value=0)
    with pytest.raises(NotFoundError, match="Record not found"):
        await gw.kill(pk)


@pytest.mark.asyncio
async def test_kill_many_tenant_aware_raises_when_rowcount_mismatch() -> None:
    gw, client = _build_tenant_aware_gateway()
    pks = [UUID("11111111-1111-1111-1111-111111111111")]

    await gw.kill_many(pks, batch_size=10)

    client.execute.assert_awaited_once()
    assert client.execute.await_args.kwargs.get("return_rowcount") is True

    client.execute = AsyncMock(return_value=0)
    with pytest.raises(NotFoundError, match="not accessible"):
        await gw.kill_many(pks)


@pytest.mark.asyncio
async def test_update_tenant_aware_includes_tenant_in_where_params() -> None:
    tid = UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
    pk = UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
    ts = datetime(2025, 1, 1, tzinfo=UTC)
    current = MyDoc(id=pk, rev=1, created_at=ts, last_update_at=ts, name="before")

    gw, client = _build_tenant_aware_gateway()
    read = gw.read_gw
    read.get = AsyncMock(return_value=current)

    client.fetch_one = AsyncMock(
        return_value={
            "id": pk,
            "rev": 2,
            "created_at": ts,
            "last_update_at": ts,
            "name": "after",
        }
    )

    await gw.update(pk, MyUpdateDoc(name="after"))

    params = client.fetch_one.await_args.args[1]
    assert tid in params
