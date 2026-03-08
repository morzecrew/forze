"""Unit tests for ``forze_mongo.adapters.document``."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import pytest

from forze.base.errors import CoreError
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_mongo.adapters.document import MongoDocumentAdapter
from forze_mongo.kernel.gateways import MongoReadGateway, MongoWriteGateway


class MyDoc(Document):
    name: str
    is_deleted: bool = False


class MyCreateDoc(CreateDocumentCmd):
    name: str


class MyUpdateDoc(BaseDTO):
    name: str | None = None


class MyReadDoc(ReadDocument):
    name: str
    is_deleted: bool = False


def _read_doc(pk: UUID, *, rev: int = 1, name: str = "item") -> MyReadDoc:
    now = datetime.now(tz=UTC)
    return MyReadDoc(id=pk, rev=rev, created_at=now, last_update_at=now, name=name)


def _domain_doc(pk: UUID, *, rev: int = 1, name: str = "item") -> MyDoc:
    now = datetime.now(tz=UTC)
    return MyDoc(id=pk, rev=rev, created_at=now, last_update_at=now, name=name)


def _build_read_gateway() -> MagicMock:
    gateway = MagicMock(spec=MongoReadGateway)
    gateway.model = MyReadDoc
    gateway.client = object()
    gateway.get = AsyncMock()
    gateway.get_many = AsyncMock()
    return gateway


def _build_write_gateway(client: object) -> MagicMock:
    gateway = MagicMock(spec=MongoWriteGateway)
    gateway.client = client
    gateway.update = AsyncMock()
    return gateway


class TestMongoDocumentAdapter:
    @pytest.mark.asyncio
    async def test_get_falls_back_to_read_gateway_when_cache_get_fails(self) -> None:
        pk = uuid4()
        expected = _read_doc(pk)
        read_gw = _build_read_gateway()
        read_gw.get.return_value = expected

        cache = MagicMock()
        cache.get = AsyncMock(side_effect=RuntimeError("cache unavailable"))
        cache.set_versioned = AsyncMock()

        adapter = MongoDocumentAdapter(read_gw=read_gw, cache=cache)
        result = await adapter.get(pk, for_update=True)

        assert result == expected
        read_gw.get.assert_awaited_once_with(pk, for_update=True, return_fields=None)
        cache.set_versioned.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_get_many_falls_back_to_read_gateway_when_cache_get_many_fails(self) -> None:
        pks = [uuid4(), uuid4()]
        expected = [_read_doc(pk, rev=i + 1) for i, pk in enumerate(pks)]
        read_gw = _build_read_gateway()
        read_gw.get_many.return_value = expected

        cache = MagicMock()
        cache.get_many = AsyncMock(side_effect=RuntimeError("cache unavailable"))
        cache.set_many_versioned = AsyncMock()

        adapter = MongoDocumentAdapter(read_gw=read_gw, cache=cache)
        result = await adapter.get_many(pks)

        assert result == expected
        read_gw.get_many.assert_awaited_once_with(pks, return_fields=None)
        cache.set_many_versioned.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_update_delegates_to_write_gateway_and_maps_domain_to_read(self) -> None:
        pk = uuid4()
        read_gw = _build_read_gateway()
        write_gw = _build_write_gateway(read_gw.client)
        write_gw.update.return_value = _domain_doc(pk, rev=2, name="after")

        adapter = MongoDocumentAdapter(read_gw=read_gw, write_gw=write_gw)
        updated = await adapter.update(pk, MyUpdateDoc(name="after"), rev=1)

        assert isinstance(updated, MyReadDoc)
        assert updated.id == pk
        assert updated.rev == 2
        assert updated.name == "after"
        write_gw.update.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_update_requires_write_gateway(self) -> None:
        adapter = MongoDocumentAdapter(read_gw=_build_read_gateway())

        with pytest.raises(CoreError, match="Write gateway is not configured"):
            await adapter.update(uuid4(), MyUpdateDoc(name="x"))
