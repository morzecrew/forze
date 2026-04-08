"""Unit tests for ``forze_mongo.adapters.document``."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import pytest

from forze.application.contracts.document import DocumentSpec
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
    gateway.tenant_aware = False
    gateway.get = AsyncMock()
    gateway.get_many = AsyncMock()
    return gateway


def _doc_spec() -> DocumentSpec[MyReadDoc, MyDoc, MyCreateDoc, MyUpdateDoc]:
    return DocumentSpec(
        name="mongo-adapter-test",
        read=MyReadDoc,
        write={
            "domain": MyDoc,
            "create_cmd": MyCreateDoc,
            "update_cmd": MyUpdateDoc,
        },
    )


def _build_write_gateway(client: object) -> MagicMock:
    gateway = MagicMock(spec=MongoWriteGateway)
    gateway.client = client
    gateway.tenant_aware = False
    gateway.update = AsyncMock(return_value=(None, {}))
    gateway.update_many = AsyncMock(return_value=([], []))
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

        adapter = MongoDocumentAdapter(spec=_doc_spec(), read_gw=read_gw, cache=cache)
        result = await adapter.get(pk, for_update=True)

        assert result == expected
        read_gw.get.assert_awaited_once_with(pk, for_update=True, return_fields=None)
        cache.set_versioned.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_get_many_falls_back_to_read_gateway_when_cache_get_many_fails(
        self,
    ) -> None:
        pks = [uuid4(), uuid4()]
        expected = [_read_doc(pk, rev=i + 1) for i, pk in enumerate(pks)]
        read_gw = _build_read_gateway()
        read_gw.get_many.return_value = expected

        cache = MagicMock()
        cache.get_many = AsyncMock(side_effect=RuntimeError("cache unavailable"))
        cache.set_many_versioned = AsyncMock()

        adapter = MongoDocumentAdapter(spec=_doc_spec(), read_gw=read_gw, cache=cache)
        result = await adapter.get_many(pks)

        assert result == expected
        read_gw.get_many.assert_awaited_once_with(pks, return_fields=None)
        cache.set_many_versioned.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_update_delegates_to_write_gateway_and_maps_domain_to_read(
        self,
    ) -> None:
        pk = uuid4()
        read_gw = _build_read_gateway()
        write_gw = _build_write_gateway(read_gw.client)
        expected_read = _read_doc(pk, rev=2, name="after")
        read_gw.get.return_value = expected_read

        adapter = MongoDocumentAdapter(spec=_doc_spec(), read_gw=read_gw, write_gw=write_gw)
        updated = await adapter.update(pk, 1, MyUpdateDoc(name="after"))

        assert isinstance(updated, MyReadDoc)
        assert updated.id == pk
        assert updated.rev == 2
        assert updated.name == "after"
        write_gw.update.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_update_requires_write_gateway(self) -> None:
        adapter = MongoDocumentAdapter(spec=_doc_spec(), read_gw=_build_read_gateway())

        with pytest.raises(CoreError, match="Write gateway is not configured"):
            await adapter.update(uuid4(), 1, MyUpdateDoc(name="x"))


class TestMongoDocumentAdapterReturnNew:
    """``return_new=False`` mutates via write gateway but skips read-back and cache sets."""

    @pytest.mark.asyncio
    async def test_create_skips_read_and_cache_when_return_new_false(self) -> None:
        read_gw = _build_read_gateway()
        write_gw = _build_write_gateway(read_gw.client)
        dom = _domain_doc(uuid4())
        write_gw.create = AsyncMock(return_value=dom)
        cache = MagicMock()
        cache.set_versioned = AsyncMock()

        adapter = MongoDocumentAdapter(
            spec=_doc_spec(),
            read_gw=read_gw,
            write_gw=write_gw,
            cache=cache,
        )

        assert await adapter.create(MyCreateDoc(name="n"), return_new=False) is None
        write_gw.create.assert_awaited_once()
        read_gw.get.assert_not_awaited()
        cache.set_versioned.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_create_many_skips_read_when_return_new_false(self) -> None:
        read_gw = _build_read_gateway()
        write_gw = _build_write_gateway(read_gw.client)
        d1, d2 = _domain_doc(uuid4()), _domain_doc(uuid4())
        write_gw.create_many = AsyncMock(return_value=[d1, d2])
        cache = MagicMock()
        cache.set_many_versioned = AsyncMock()

        adapter = MongoDocumentAdapter(
            spec=_doc_spec(),
            read_gw=read_gw,
            write_gw=write_gw,
            batch_size=50,
        )

        assert (
            await adapter.create_many(
                [MyCreateDoc(name="a"), MyCreateDoc(name="b")],
                return_new=False,
            )
            is None
        )
        write_gw.create_many.assert_awaited_once()
        read_gw.get_many.assert_not_awaited()
        cache.set_many_versioned.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_update_skips_read_when_return_new_false(self) -> None:
        pk = uuid4()
        read_gw = _build_read_gateway()
        write_gw = _build_write_gateway(read_gw.client)
        write_gw.update = AsyncMock(return_value=(None, {}))
        cache = MagicMock()
        cache.delete_many = AsyncMock()

        adapter = MongoDocumentAdapter(
            spec=_doc_spec(),
            read_gw=read_gw,
            write_gw=write_gw,
            cache=cache,
        )

        assert (
            await adapter.update(pk, 1, MyUpdateDoc(name="z"), return_new=False) is None
        )
        write_gw.update.assert_awaited_once()
        read_gw.get.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_touch_skips_read_when_return_new_false(self) -> None:
        pk = uuid4()
        read_gw = _build_read_gateway()
        write_gw = _build_write_gateway(read_gw.client)
        write_gw.touch = AsyncMock()
        cache = MagicMock()
        cache.delete_many = AsyncMock()

        adapter = MongoDocumentAdapter(
            spec=_doc_spec(),
            read_gw=read_gw,
            write_gw=write_gw,
            cache=cache,
        )

        assert await adapter.touch(pk, return_new=False) is None
        write_gw.touch.assert_awaited_once_with(pk)
        read_gw.get.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_touch_many_skips_read_when_return_new_false(self) -> None:
        pks = [uuid4(), uuid4()]
        read_gw = _build_read_gateway()
        write_gw = _build_write_gateway(read_gw.client)
        write_gw.touch_many = AsyncMock()
        cache = MagicMock()
        cache.delete_many = AsyncMock()

        adapter = MongoDocumentAdapter(
            spec=_doc_spec(),
            read_gw=read_gw,
            write_gw=write_gw,
            batch_size=40,
        )

        assert await adapter.touch_many(pks, return_new=False) is None
        write_gw.touch_many.assert_awaited_once_with(pks, batch_size=40)
        read_gw.get_many.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_delete_restore_skips_read_when_return_new_false(self) -> None:
        pk = uuid4()
        read_gw = _build_read_gateway()
        write_gw = _build_write_gateway(read_gw.client)
        write_gw.delete = AsyncMock()
        write_gw.restore = AsyncMock()
        cache = MagicMock()
        cache.delete_many = AsyncMock()

        adapter = MongoDocumentAdapter(
            spec=_doc_spec(),
            read_gw=read_gw,
            write_gw=write_gw,
            cache=cache,
        )

        assert await adapter.delete(pk, 1, return_new=False) is None
        write_gw.delete.assert_awaited_once_with(pk, rev=1)
        read_gw.get.assert_not_awaited()

        assert await adapter.restore(pk, 2, return_new=False) is None
        write_gw.restore.assert_awaited_once_with(pk, rev=2)
        read_gw.get.assert_not_awaited()
