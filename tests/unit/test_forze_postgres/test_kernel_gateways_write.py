"""Unit tests for ``forze_postgres.kernel.gateways.write``."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import pytest

from forze.base.errors import ConcurrencyError
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


def _build_gateway() -> tuple[
    PostgresWriteGateway[MyDoc, MyCreateDoc, MyUpdateDoc], MagicMock
]:
    client = MagicMock(spec=PostgresClient)
    client.fetch_all = AsyncMock()

    introspector = MagicMock(spec=PostgresIntrospector)
    introspector.get_column_types = AsyncMock(return_value={})

    qname = PostgresQualifiedName(schema="public", name="docs")
    read = MagicMock(spec=PostgresReadGateway)
    read.qname = qname
    read.client = client
    read.tenant_aware = False

    gw = PostgresWriteGateway(
        qname=qname,
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
    first_call = client.fetch_all.await_args_list[0]
    second_call = client.fetch_all.await_args_list[1]

    first_params = first_call.args[1]
    second_params = second_call.args[1]

    assert [x for x in first_params if isinstance(x, UUID)] == [id1, id2]
    assert [x for x in first_params if x in {"first", "second", "third"}] == [
        "first",
        "second",
    ]
    assert [x for x in second_params if isinstance(x, UUID)] == [id3]
    assert [x for x in second_params if x in {"first", "second", "third"}] == ["third"]

    assert first_call.kwargs["row_factory"] == "dict"
    assert first_call.kwargs["commit"] is True
    assert second_call.kwargs["row_factory"] == "dict"
    assert second_call.kwargs["commit"] is True

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
