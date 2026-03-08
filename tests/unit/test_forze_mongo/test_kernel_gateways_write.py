"""Unit tests for ``forze_mongo.kernel.gateways.write``."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import pytest

from forze.base.errors import CoreError
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document
from forze_mongo.kernel.gateways import MongoWriteGateway
from forze_mongo.kernel.platform import MongoClient


class MyDoc(Document):
    name: str
    is_deleted: bool = False


class MyCreateDoc(CreateDocumentCmd):
    name: str


class MyUpdateDoc(BaseDTO):
    name: str | None = None


def _domain_doc(pk: UUID, *, rev: int = 1, name: str = "item") -> MyDoc:
    now = datetime.now(tz=UTC)
    return MyDoc(id=pk, rev=rev, created_at=now, last_update_at=now, name=name)


def _build_client() -> MagicMock:
    client = MagicMock(spec=MongoClient)
    client.collection.return_value = object()
    client.update_one = AsyncMock()
    return client


def _build_read(client: MagicMock, source: str = "docs") -> MagicMock:
    read = MagicMock()
    read.client = client
    read.source = source
    read.db_name = None
    read.model = MyDoc
    read.get = AsyncMock()
    return read


class TestMongoWriteGateway:
    @pytest.mark.asyncio
    async def test_update_bumps_revision_in_application_strategy(self) -> None:
        pk = uuid4()
        current = _domain_doc(pk, rev=1, name="before")
        client = _build_client()
        client.update_one.return_value = 1
        read = _build_read(client)
        read.get.return_value = current

        gw = MongoWriteGateway(
            source="docs",
            client=client,
            model=MyDoc,
            read=read,
            create_dto=MyCreateDoc,
            update_dto=MyUpdateDoc,
            rev_bump_strategy="application",
        )
        updated = await gw.update(pk, MyUpdateDoc(name="after"))

        assert updated.rev == 2
        assert updated.name == "after"
        update_filter = client.update_one.await_args.args[1]
        update_payload = client.update_one.await_args.args[2]
        assert update_filter == {"_id": str(pk), "rev": 1}
        assert update_payload["$set"]["rev"] == 2

    def test_init_rejects_non_application_rev_strategy(self) -> None:
        client = _build_client()
        read = _build_read(client)

        with pytest.raises(CoreError, match="Invalid revision bump strategy"):
            MongoWriteGateway(
                source="docs",
                client=client,
                model=MyDoc,
                read=read,
                create_dto=MyCreateDoc,
                update_dto=MyUpdateDoc,
                rev_bump_strategy="database",  # pyright: ignore[reportArgumentType]
            )
