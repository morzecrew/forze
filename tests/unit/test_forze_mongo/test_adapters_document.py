"""Unit tests for ``forze_mongo.adapters.document``."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import pytest

from forze.base.errors import CoreError
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_mongo.adapters.document import MongoDocumentAdapter
from forze_mongo.kernel.platform import MongoClient


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


class MyDocWithoutSoftDelete(Document):
    name: str


def _doc_row(pk: UUID, *, rev: int = 1, name: str = "item") -> dict[str, object]:
    now = datetime.now(tz=UTC)
    now_iso = now.isoformat()
    pk_s = str(pk)
    return {
        "_id": pk_s,
        "id": pk_s,
        "rev": rev,
        "created_at": now_iso,
        "last_update_at": now_iso,
        "name": name,
        "is_deleted": False,
    }


def _build_client() -> MagicMock:
    client = MagicMock(spec=MongoClient)
    collection = object()
    client.collection.return_value = collection
    client.find_one = AsyncMock()
    client.find_many = AsyncMock()
    client.insert_one = AsyncMock()
    client.insert_many = AsyncMock()
    client.update_one = AsyncMock()
    client.delete_one = AsyncMock()
    client.delete_many = AsyncMock()
    client.count = AsyncMock(return_value=0)
    return client


class TestMongoDocumentAdapter:
    @pytest.mark.asyncio
    async def test_get_falls_back_to_client_when_cache_get_fails(self) -> None:
        pk = uuid4()
        row = _doc_row(pk)
        client = _build_client()
        client.find_one.return_value = row

        cache = MagicMock()
        cache.get = AsyncMock(side_effect=RuntimeError("cache unavailable"))
        cache.set_versioned = AsyncMock()

        adapter = MongoDocumentAdapter(
            client=client,
            read_model=MyReadDoc,
            domain_model=MyDoc,
            create_dto=MyCreateDoc,
            update_dto=MyUpdateDoc,
            read_source="docs",
            cache=cache,
        )

        result = await adapter.get(pk)

        assert isinstance(result, MyReadDoc)
        assert result.id == pk
        cache.set_versioned.assert_not_awaited()
        client.find_one.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_get_many_falls_back_to_client_when_cache_get_many_fails(self) -> None:
        pks = [uuid4(), uuid4()]
        rows = [_doc_row(pk, rev=i + 1) for i, pk in enumerate(pks)]
        client = _build_client()
        client.find_many.return_value = rows

        cache = MagicMock()
        cache.get_many = AsyncMock(side_effect=RuntimeError("cache unavailable"))
        cache.set_many_versioned = AsyncMock()

        adapter = MongoDocumentAdapter(
            client=client,
            read_model=MyReadDoc,
            domain_model=MyDoc,
            create_dto=MyCreateDoc,
            update_dto=MyUpdateDoc,
            read_source="docs",
            cache=cache,
        )

        result = await adapter.get_many(pks)

        assert [x.id for x in result] == pks
        cache.set_many_versioned.assert_not_awaited()
        client.find_many.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_update_uses_revision_filter_and_bumps_revision(self) -> None:
        pk = uuid4()
        current = _doc_row(pk, rev=1, name="before")
        client = _build_client()
        client.find_one.return_value = current
        client.update_one.return_value = 1

        adapter = MongoDocumentAdapter(
            client=client,
            read_model=MyReadDoc,
            domain_model=MyDoc,
            create_dto=MyCreateDoc,
            update_dto=MyUpdateDoc,
            read_source="docs",
        )

        updated = await adapter.update(pk, MyUpdateDoc(name="after"))

        assert updated.rev == 2
        assert updated.name == "after"
        update_filter = client.update_one.await_args.args[1]
        update_payload = client.update_one.await_args.args[2]
        assert update_filter == {"_id": str(pk), "rev": 1}
        assert update_payload["$inc"] == {"rev": 1}

    @pytest.mark.asyncio
    async def test_delete_raises_when_model_has_no_soft_delete_field(self) -> None:
        client = _build_client()
        adapter = MongoDocumentAdapter(
            client=client,
            read_model=MyReadDoc,
            domain_model=MyDocWithoutSoftDelete,
            create_dto=MyCreateDoc,
            update_dto=MyUpdateDoc,
            read_source="docs",
        )

        with pytest.raises(CoreError, match="Soft deletion is not supported"):
            await adapter.delete(uuid4())
