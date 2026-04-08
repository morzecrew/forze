"""Unit tests for ``forze_mongo.kernel.gateways.write``."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import pytest

from forze.base.errors import ConcurrencyError
from forze.domain.constants import TENANT_ID_FIELD
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document
from forze_mongo.kernel.gateways import MongoReadGateway, MongoWriteGateway
from forze_mongo.kernel.gateways.write import optimistic_retry
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


def _build_read(client: MagicMock, *, collection: str = "docs") -> MagicMock:
    read = MagicMock(spec=MongoReadGateway)
    read.client = client
    read.collection = collection
    read.database = None
    read.tenant_aware = False
    read.model_type = MyDoc
    read.get = AsyncMock()
    return read


class TestMongoWriteGateway:
    @pytest.mark.asyncio
    async def test_update_bumps_revision_in_application_strategy(self) -> None:
        pk = uuid4()
        current = _domain_doc(pk, rev=1, name="before")
        after_write = _domain_doc(pk, rev=2, name="after")
        client = _build_client()
        client.update_one.return_value = 1
        read = _build_read(client)
        read.get.side_effect = [
            current,
            after_write,
        ]  # read-before-write, then read-after-write

        gw = MongoWriteGateway(
            model_type=MyDoc,
            collection="docs",
            database=None,
            client=client,
            read_gw=read,
            create_cmd_type=MyCreateDoc,
            update_cmd_type=MyUpdateDoc,
        )
        updated, diff = await gw.update(pk, MyUpdateDoc(name="after"))

        assert updated.rev == 2
        assert updated.name == "after"
        update_filter = client.update_one.await_args.args[1]
        update_payload = client.update_one.await_args.args[2]
        assert update_filter == {"_id": str(pk), "rev": 1}
        assert update_payload["$set"]["rev"] == 2
        assert diff["rev"] == 2
        assert diff["name"] == "after"

    @pytest.mark.asyncio
    async def test_update_tenant_aware_includes_tenant_in_filter(self) -> None:
        tid = uuid4()
        pk = uuid4()
        current = _domain_doc(pk, rev=1, name="before")
        after_write = _domain_doc(pk, rev=2, name="after")
        client = _build_client()
        client.update_one.return_value = 1
        read = _build_read(client)
        read.tenant_aware = True
        read.get.side_effect = [current, after_write]

        gw = MongoWriteGateway(
            model_type=MyDoc,
            collection="docs",
            database=None,
            client=client,
            read_gw=read,
            create_cmd_type=MyCreateDoc,
            update_cmd_type=MyUpdateDoc,
            tenant_aware=True,
            tenant_provider=lambda: tid,
        )
        await gw.update(pk, MyUpdateDoc(name="after"))

        update_filter = client.update_one.await_args.args[1]
        assert update_filter[TENANT_ID_FIELD] == tid
        assert update_filter["_id"] == str(pk)
        assert update_filter["rev"] == 1

    @pytest.mark.asyncio
    async def test_update_retries_on_concurrency_error(self) -> None:
        pk = uuid4()
        current = _domain_doc(pk, rev=1, name="before")
        after_write = _domain_doc(pk, rev=2, name="after")
        client = _build_client()
        client.update_one.side_effect = [
            ConcurrencyError("Failed to update record"),
            1,
        ]
        read = _build_read(client)
        read.get.side_effect = [
            current,
            current,
            after_write,
        ]  # attempt 1 before write, attempt 2 before+after write

        gw = MongoWriteGateway(
            model_type=MyDoc,
            collection="docs",
            database=None,
            client=client,
            read_gw=read,
            create_cmd_type=MyCreateDoc,
            update_cmd_type=MyUpdateDoc,
        )
        updated, _ = await gw.update(pk, MyUpdateDoc(name="after"))

        assert updated.name == "after"
        assert client.update_one.await_count == 2

    @pytest.mark.asyncio
    async def test_update_exhausts_retries_and_raises(self) -> None:
        pk = uuid4()
        current = _domain_doc(pk, rev=1, name="before")
        client = _build_client()
        client.update_one.side_effect = ConcurrencyError("Failed to update record")
        read = _build_read(client)
        read.get.return_value = current

        gw = MongoWriteGateway(
            model_type=MyDoc,
            collection="docs",
            database=None,
            client=client,
            read_gw=read,
            create_cmd_type=MyCreateDoc,
            update_cmd_type=MyUpdateDoc,
        )

        with pytest.raises(ConcurrencyError, match="Failed to update record"):
            await gw.update(pk, MyUpdateDoc(name="after"))

        assert client.update_one.await_count == 3


class TestOptimisticRetry:
    def test_optimistic_retry_returns_tenacity_decorator(self) -> None:
        decorator = optimistic_retry(attempts=5)
        assert callable(decorator)
