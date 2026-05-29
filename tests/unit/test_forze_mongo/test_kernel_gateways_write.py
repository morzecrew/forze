"""Unit tests for ``forze_mongo.kernel.gateways.write``."""

from forze.base.exceptions import CoreException, ExceptionKind, exc
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import pytest

from forze.application.contracts.tenancy import TENANT_ID_FIELD, TenantIdentity
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document
from forze_mongo.kernel.gateways import (
    MongoHistoryGateway,
    MongoReadGateway,
    MongoWriteGateway,
)
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

def _build_read(
    client: MagicMock,
    *,
    relation: tuple[str, str] = ("test_db", "docs"),
) -> MagicMock:
    read = MagicMock(spec=MongoReadGateway)
    read.client = client
    read.relation = relation
    read.collection = relation[1]
    read.database = relation[0]
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
            relation=("test_db", "docs"),
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
            relation=("test_db", "docs"),
            client=client,
            read_gw=read,
            create_cmd_type=MyCreateDoc,
            update_cmd_type=MyUpdateDoc,
            tenant_aware=True,
            tenant_provider=lambda: TenantIdentity(tenant_id=tid),
        )
        await gw.update(pk, MyUpdateDoc(name="after"))

        update_filter = client.update_one.await_args.args[1]
        assert update_filter[TENANT_ID_FIELD].tenant_id == tid
        assert update_filter["_id"] == str(pk)
        assert update_filter["rev"] == 1

    @pytest.mark.asyncio
    async def test_update_retries_on_concurrency_error(self) -> None:
        pk = uuid4()
        current = _domain_doc(pk, rev=1, name="before")
        after_write = _domain_doc(pk, rev=2, name="after")
        client = _build_client()
        client.update_one.side_effect = [
            exc.concurrency("Failed to update record"),
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
            relation=("test_db", "docs"),
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
        client.update_one.side_effect = exc.concurrency("Failed to update record")
        read = _build_read(client)
        read.get.return_value = current

        gw = MongoWriteGateway(
            model_type=MyDoc,
            relation=("test_db", "docs"),
            client=client,
            read_gw=read,
            create_cmd_type=MyCreateDoc,
            update_cmd_type=MyUpdateDoc,
        )

        with pytest.raises(CoreException, match="Failed to update record"):
            await gw.update(pk, MyUpdateDoc(name="after"))

        assert client.update_one.await_count == 3

    @pytest.mark.asyncio
    async def test_ensure_many_reads_conflicts_only(self) -> None:
        pk_new = uuid4()
        pk_existing = uuid4()
        now = datetime.now(tz=UTC)
        existing = _domain_doc(pk_existing, name="existing")
        dtos = [
            MyCreateDoc(id=pk_existing, created_at=now, name="try"),
            MyCreateDoc(id=pk_new, created_at=now, name="new"),
        ]
        client = _build_client()
        bulk_result = MagicMock()
        bulk_result.upserted_ids = {1: str(pk_new)}
        client.bulk_write = AsyncMock(return_value=bulk_result)
        read = _build_read(client)
        read.get_many = AsyncMock(return_value=[existing])

        gw = MongoWriteGateway(
            model_type=MyDoc,
            relation=("test_db", "docs"),
            client=client,
            read_gw=read,
            create_cmd_type=MyCreateDoc,
            update_cmd_type=MyUpdateDoc,
        )

        out = await gw.ensure_many(dtos, batch_size=20)

        assert [d.id for d in out] == [pk_existing, pk_new]
        assert out[0].name == "existing"
        assert out[1].name == "new"
        read.get_many.assert_awaited_once_with([pk_existing])

    @pytest.mark.asyncio
    async def test_ensure_many_bulk_duplicate_key_raises_conflict(self) -> None:
        pk = uuid4()
        now = datetime.now(tz=UTC)
        dtos = [MyCreateDoc(id=pk, created_at=now, name="dup")]
        client = _build_client()
        client.bulk_write = AsyncMock(
            side_effect=CoreException.conflict("Duplicate key violation."),
        )
        read = _build_read(client)
        gw = MongoWriteGateway(
            model_type=MyDoc,
            relation=("test_db", "docs"),
            client=client,
            read_gw=read,
            create_cmd_type=MyCreateDoc,
            update_cmd_type=MyUpdateDoc,
        )

        with pytest.raises(CoreException) as err:
            await gw.ensure_many(dtos, batch_size=20)

        assert err.value.kind is ExceptionKind.CONFLICT

    @pytest.mark.asyncio
    async def test_ensure_many_missing_after_bulk_raises_conflict(self) -> None:
        pk = uuid4()
        now = datetime.now(tz=UTC)
        dtos = [MyCreateDoc(id=pk, created_at=now, name="ghost")]
        client = _build_client()
        bulk_result = MagicMock()
        bulk_result.upserted_ids = {}
        client.bulk_write = AsyncMock(return_value=bulk_result)
        read = _build_read(client)
        read.get_many = AsyncMock(
            side_effect=CoreException.not_found("Some records not found"),
        )
        gw = MongoWriteGateway(
            model_type=MyDoc,
            relation=("test_db", "docs"),
            client=client,
            read_gw=read,
            create_cmd_type=MyCreateDoc,
            update_cmd_type=MyUpdateDoc,
        )

        with pytest.raises(CoreException) as err:
            await gw.ensure_many(dtos, batch_size=20)

        assert err.value.kind is ExceptionKind.CONFLICT
        assert err.value.code == "mongo_ensure_bulk_miss"

class TestOptimisticRetry:
    def test_optimistic_retry_returns_tenacity_decorator(self) -> None:
        decorator = optimistic_retry(attempts=5)
        assert callable(decorator)

class TestMongoWriteGatewayPostInit:
    def test_rejects_mismatched_read_collection(self) -> None:
        client = _build_client()
        read = _build_read(client, relation=("test_db", "read_col"))
        with pytest.raises(CoreException, match="Relation mismatch"):
            MongoWriteGateway(
                model_type=MyDoc,
                relation=("test_db", "write_col"),
                client=client,
                read_gw=read,
                create_cmd_type=MyCreateDoc,
                update_cmd_type=MyUpdateDoc,
            )

    def test_rejects_mismatched_read_client(self) -> None:
        c_read = _build_client()
        c_write = _build_client()
        read = _build_read(c_read)
        with pytest.raises(CoreException, match="Client mismatch"):
            MongoWriteGateway(
                model_type=MyDoc,
                relation=("test_db", "docs"),
                client=c_write,
                read_gw=read,
                create_cmd_type=MyCreateDoc,
                update_cmd_type=MyUpdateDoc,
            )

    def test_rejects_mismatched_read_database(self) -> None:
        client = _build_client()
        read = _build_read(client, relation=("db_a", "docs"))
        with pytest.raises(CoreException, match="Relation mismatch"):
            MongoWriteGateway(
                model_type=MyDoc,
                relation=("db_b", "docs"),
                client=client,
                read_gw=read,
                create_cmd_type=MyCreateDoc,
                update_cmd_type=MyUpdateDoc,
            )

    def test_rejects_mismatched_tenant_awareness(self) -> None:
        client = _build_client()
        read = _build_read(client)
        read.tenant_aware = True
        with pytest.raises(CoreException, match="Tenant awareness mismatch"):
            MongoWriteGateway(
                model_type=MyDoc,
                relation=("test_db", "docs"),
                client=client,
                read_gw=read,
                create_cmd_type=MyCreateDoc,
                update_cmd_type=MyUpdateDoc,
                tenant_aware=False,
            )

    def test_rejects_history_gateway_client_mismatch(self) -> None:
        c_main = _build_client()
        c_hist = _build_client()
        read = _build_read(c_main, relation=("db", "docs"))
        hist = MongoHistoryGateway(
            model_type=MyDoc,
            relation=("db", "hist"),
            target_relation=("db", "docs"),
            client=c_hist,
        )
        with pytest.raises(
            CoreException, match="nested history gateway must use the same client"
        ):
            MongoWriteGateway(
                model_type=MyDoc,
                relation=("db", "docs"),
                client=c_main,
                read_gw=read,
                create_cmd_type=MyCreateDoc,
                update_cmd_type=MyUpdateDoc,
                history_gw=hist,
            )
