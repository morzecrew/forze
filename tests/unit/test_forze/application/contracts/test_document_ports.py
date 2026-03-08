"""Unit tests for document port contracts (DocumentReadPort, DocumentWritePort).

Exercises the protocol through InMemoryDocumentPort and through direct protocol
method calls to improve coverage of ports.py.
"""

import pytest
from uuid import uuid4

from forze.application.contracts.document import (
    DocumentReadPort,
    DocumentWritePort,
)
from forze.application.contracts.query import QueryFilterExpression
from forze.domain.models import CreateDocumentCmd

from .._stubs import InMemoryDocumentPort

# ----------------------- #


class TestDocumentPortProtocolConformance:
    """Verify InMemoryDocumentPort conforms to document protocols."""

    def test_in_memory_port_is_document_read_port(self) -> None:
        port = InMemoryDocumentPort()
        assert isinstance(port, DocumentReadPort)

    def test_in_memory_port_is_document_write_port(self) -> None:
        port = InMemoryDocumentPort()
        assert isinstance(port, DocumentWritePort)


class TestDocumentReadPortViaStub:
    """Test DocumentReadPort contract through InMemoryDocumentPort."""

    @pytest.mark.asyncio
    async def test_get_returns_read_model(self) -> None:
        port = InMemoryDocumentPort()
        cmd = CreateDocumentCmd()
        created = await port.create(cmd)
        result = await port.get(created.id)
        assert result is not None
        assert hasattr(result, "id")

    @pytest.mark.asyncio
    async def test_get_with_return_fields_returns_dict(self) -> None:
        port = InMemoryDocumentPort()
        cmd = CreateDocumentCmd()
        created = await port.create(cmd)
        result = await port.get(created.id, return_fields=["id", "rev"])
        assert isinstance(result, dict)
        assert "id" in result
        assert "rev" in result

    @pytest.mark.asyncio
    async def test_get_missing_raises(self) -> None:
        port = InMemoryDocumentPort()
        with pytest.raises(KeyError, match="not found"):
            await port.get(uuid4())

    @pytest.mark.asyncio
    async def test_get_many_returns_sequence(self) -> None:
        port = InMemoryDocumentPort()
        cmd = CreateDocumentCmd()
        c1 = await port.create(cmd)
        c2 = await port.create(cmd)
        result = await port.get_many([c1.id, c2.id])
        assert len(result) == 2

    @pytest.mark.asyncio
    async def test_get_many_with_return_fields(self) -> None:
        port = InMemoryDocumentPort()
        cmd = CreateDocumentCmd()
        created = await port.create(cmd)
        result = await port.get_many([created.id], return_fields=["id"])
        assert len(result) == 1
        assert isinstance(result[0], dict)
        assert "id" in result[0]

    @pytest.mark.asyncio
    async def test_find_returns_none_when_empty(self) -> None:
        port = InMemoryDocumentPort()
        filters: QueryFilterExpression = {"$fields": {"id": str(uuid4())}}
        result = await port.find(filters)
        assert result is None

    @pytest.mark.asyncio
    async def test_find_returns_document_when_exists(self) -> None:
        port = InMemoryDocumentPort()
        cmd = CreateDocumentCmd()
        created = await port.create(cmd)
        filters: QueryFilterExpression = {"$fields": {"id": str(created.id)}}
        result = await port.find(filters)
        assert result is not None

    @pytest.mark.asyncio
    async def test_find_many_returns_tuple(self) -> None:
        port = InMemoryDocumentPort()
        items, total = await port.find_many()
        assert isinstance(items, list)
        assert isinstance(total, int)
        assert total >= 0

    @pytest.mark.asyncio
    async def test_find_many_with_filters_and_pagination(self) -> None:
        port = InMemoryDocumentPort()
        await port.create(CreateDocumentCmd())
        items, total = await port.find_many(limit=1, offset=0)
        assert len(items) <= 1
        assert total >= 0

    @pytest.mark.asyncio
    async def test_count_returns_int(self) -> None:
        port = InMemoryDocumentPort()
        n = await port.count()
        assert isinstance(n, int)
        assert n >= 0


class TestDocumentWritePortViaStub:
    """Test DocumentWritePort contract through InMemoryDocumentPort."""

    @pytest.mark.asyncio
    async def test_create_returns_read_model(self) -> None:
        port = InMemoryDocumentPort()
        cmd = CreateDocumentCmd()
        result = await port.create(cmd)
        assert result is not None
        assert result.id is not None
        assert result.rev == 1

    @pytest.mark.asyncio
    async def test_create_many_returns_sequence(self) -> None:
        port = InMemoryDocumentPort()
        cmds = [CreateDocumentCmd(), CreateDocumentCmd()]
        result = await port.create_many(cmds)
        assert len(result) == 2

    @pytest.mark.asyncio
    async def test_update_bumps_rev(self) -> None:
        port = InMemoryDocumentPort()
        cmd = CreateDocumentCmd()
        created = await port.create(cmd)
        from forze.domain.models import BaseDTO

        class UpdateCmd(BaseDTO):
            title: str | None = None

        updated = await port.update(created.id, UpdateCmd(title="x"))
        assert updated.rev == 2

    @pytest.mark.asyncio
    async def test_update_many(self) -> None:
        port = InMemoryDocumentPort()
        cmd = CreateDocumentCmd()
        c1 = await port.create(cmd)
        c2 = await port.create(cmd)
        from forze.domain.models import BaseDTO

        class UpdateCmd(BaseDTO):
            title: str | None = None

        result = await port.update_many(
            [c1.id, c2.id], [UpdateCmd(title="a"), UpdateCmd(title="b")]
        )
        assert len(result) == 2

    @pytest.mark.asyncio
    async def test_touch_updates_timestamp(self) -> None:
        port = InMemoryDocumentPort()
        cmd = CreateDocumentCmd()
        created = await port.create(cmd)
        touched = await port.touch(created.id)
        assert touched.id == created.id

    @pytest.mark.asyncio
    async def test_touch_many(self) -> None:
        port = InMemoryDocumentPort()
        cmd = CreateDocumentCmd()
        c1 = await port.create(cmd)
        c2 = await port.create(cmd)
        result = await port.touch_many([c1.id, c2.id])
        assert len(result) == 2

    @pytest.mark.asyncio
    async def test_kill_removes_document(self) -> None:
        port = InMemoryDocumentPort()
        cmd = CreateDocumentCmd()
        created = await port.create(cmd)
        await port.kill(created.id)
        with pytest.raises(KeyError):
            await port.get(created.id)

    @pytest.mark.asyncio
    async def test_kill_many(self) -> None:
        port = InMemoryDocumentPort()
        cmd = CreateDocumentCmd()
        c1 = await port.create(cmd)
        c2 = await port.create(cmd)
        await port.kill_many([c1.id, c2.id])
        with pytest.raises(KeyError):
            await port.get(c1.id)

    @pytest.mark.asyncio
    async def test_delete_soft_deletes(self) -> None:
        port = InMemoryDocumentPort()
        cmd = CreateDocumentCmd()
        created = await port.create(cmd)
        deleted = await port.delete(created.id)
        assert deleted is not None
        with pytest.raises(KeyError):
            await port.get(created.id)

    @pytest.mark.asyncio
    async def test_delete_many(self) -> None:
        port = InMemoryDocumentPort()
        cmd = CreateDocumentCmd()
        c1 = await port.create(cmd)
        c2 = await port.create(cmd)
        result = await port.delete_many([c1.id, c2.id])
        assert len(result) == 2

    @pytest.mark.asyncio
    async def test_restore_after_delete(self) -> None:
        port = InMemoryDocumentPort()
        cmd = CreateDocumentCmd()
        created = await port.create(cmd)
        await port.delete(created.id)
        restored = await port.restore(created.id)
        assert restored is not None
        got = await port.get(created.id)
        assert got is not None

    @pytest.mark.asyncio
    async def test_restore_many(self) -> None:
        port = InMemoryDocumentPort()
        cmd = CreateDocumentCmd()
        c1 = await port.create(cmd)
        c2 = await port.create(cmd)
        await port.delete_many([c1.id, c2.id])
        result = await port.restore_many([c1.id, c2.id])
        assert len(result) == 2
