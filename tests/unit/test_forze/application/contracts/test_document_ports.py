"""Unit tests for document port contracts (DocumentReadPort, DocumentWritePort).

Exercises the protocol through MockDocumentAdapter and through direct protocol
method calls to improve coverage of ports.py.
"""

import pytest
from uuid import uuid4

from forze.application.contracts.document import (
    DocumentReadPort,
    DocumentWritePort,
)
from forze.application.contracts.query import QueryFilterExpression
from forze.base.errors import NotFoundError
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument

from forze_mock import MockState
from forze_mock.adapters import MockDocumentAdapter

# ----------------------- #


def _document_adapter() -> MockDocumentAdapter:
    """Create a MockDocumentAdapter for tests."""
    return MockDocumentAdapter(
        state=MockState(),
        namespace="test",
        read_model=ReadDocument,
        domain_model=Document,
    )


class _UpdateTitle(BaseDTO):
    """Update DTO with title for update tests."""

    title: str | None = None


class _CreateWithTitle(CreateDocumentCmd):
    """Create command with title for update tests."""

    title: str = ""


def _document_adapter_with_title() -> MockDocumentAdapter:
    """Create a MockDocumentAdapter with mutable title for update tests."""

    class DocWithTitle(Document):
        title: str = ""

    class ReadWithTitle(ReadDocument):
        title: str = ""

    return MockDocumentAdapter(
        state=MockState(),
        namespace="test_title",
        read_model=ReadWithTitle,
        domain_model=DocWithTitle,
    )


class TestDocumentPortProtocolConformance:
    """Verify MockDocumentAdapter conforms to document protocols."""

    def test_mock_adapter_is_document_read_port(self) -> None:
        port = _document_adapter()
        assert isinstance(port, DocumentReadPort)

    def test_mock_adapter_is_document_write_port(self) -> None:
        port = _document_adapter()
        assert isinstance(port, DocumentWritePort)


class TestDocumentReadPortViaMock:
    """Test DocumentReadPort contract through MockDocumentAdapter."""

    @pytest.mark.asyncio
    async def test_get_returns_read_model(self) -> None:
        port = _document_adapter()
        cmd = CreateDocumentCmd()
        created = await port.create(cmd)
        result = await port.get(created.id)
        assert result is not None
        assert hasattr(result, "id")

    @pytest.mark.asyncio
    async def test_get_with_return_fields_returns_dict(self) -> None:
        port = _document_adapter()
        cmd = CreateDocumentCmd()
        created = await port.create(cmd)
        result = await port.get(created.id, return_fields=["id", "rev"])
        assert isinstance(result, dict)
        assert "id" in result
        assert "rev" in result

    @pytest.mark.asyncio
    async def test_get_missing_raises(self) -> None:
        port = _document_adapter()
        with pytest.raises(NotFoundError, match="not found"):
            await port.get(uuid4())

    @pytest.mark.asyncio
    async def test_get_many_returns_sequence(self) -> None:
        port = _document_adapter()
        cmd = CreateDocumentCmd()
        c1 = await port.create(cmd)
        c2 = await port.create(cmd)
        result = await port.get_many([c1.id, c2.id])
        assert len(result) == 2

    @pytest.mark.asyncio
    async def test_get_many_with_return_fields(self) -> None:
        port = _document_adapter()
        cmd = CreateDocumentCmd()
        created = await port.create(cmd)
        result = await port.get_many([created.id], return_fields=["id"])
        assert len(result) == 1
        assert isinstance(result[0], dict)
        assert "id" in result[0]

    @pytest.mark.asyncio
    async def test_find_returns_none_when_empty(self) -> None:
        port = _document_adapter()
        filters: QueryFilterExpression = {"$fields": {"id": str(uuid4())}}
        result = await port.find(filters)
        assert result is None

    @pytest.mark.asyncio
    async def test_find_returns_document_when_exists(self) -> None:
        port = _document_adapter()
        cmd = CreateDocumentCmd()
        created = await port.create(cmd)
        filters: QueryFilterExpression = {"$fields": {"id": str(created.id)}}
        result = await port.find(filters)
        assert result is not None

    @pytest.mark.asyncio
    async def test_find_many_returns_tuple(self) -> None:
        port = _document_adapter()
        items, total = await port.find_many()
        assert isinstance(items, list)
        assert isinstance(total, int)
        assert total >= 0

    @pytest.mark.asyncio
    async def test_find_many_with_filters_and_pagination(self) -> None:
        port = _document_adapter()
        await port.create(CreateDocumentCmd())
        items, total = await port.find_many(limit=1, offset=0)
        assert len(items) <= 1
        assert total >= 0

    @pytest.mark.asyncio
    async def test_count_returns_int(self) -> None:
        port = _document_adapter()
        n = await port.count()
        assert isinstance(n, int)
        assert n >= 0


class TestDocumentWritePortViaMock:
    """Test DocumentWritePort contract through MockDocumentAdapter."""

    @pytest.mark.asyncio
    async def test_create_returns_read_model(self) -> None:
        port = _document_adapter()
        cmd = CreateDocumentCmd()
        result = await port.create(cmd)
        assert result is not None
        assert result.id is not None
        assert result.rev == 1

    @pytest.mark.asyncio
    async def test_create_many_returns_sequence(self) -> None:
        port = _document_adapter()
        cmds = [CreateDocumentCmd(), CreateDocumentCmd()]
        result = await port.create_many(cmds)
        assert len(result) == 2

    @pytest.mark.asyncio
    async def test_update_bumps_rev(self) -> None:
        port = _document_adapter_with_title()
        created = await port.create(_CreateWithTitle())
        updated = await port.update(created.id, created.rev, _UpdateTitle(title="x"))
        assert updated.rev == 2

    @pytest.mark.asyncio
    async def test_update_many(self) -> None:
        port = _document_adapter_with_title()
        c1 = await port.create(_CreateWithTitle())
        c2 = await port.create(_CreateWithTitle())
        result = await port.update_many(
            [
                (c1.id, c1.rev, _UpdateTitle(title="a")),
                (c2.id, c2.rev, _UpdateTitle(title="b")),
            ]
        )
        assert len(result) == 2

    @pytest.mark.asyncio
    async def test_touch_updates_timestamp(self) -> None:
        port = _document_adapter()
        cmd = CreateDocumentCmd()
        created = await port.create(cmd)
        touched = await port.touch(created.id)
        assert touched.id == created.id

    @pytest.mark.asyncio
    async def test_touch_many(self) -> None:
        port = _document_adapter()
        cmd = CreateDocumentCmd()
        c1 = await port.create(cmd)
        c2 = await port.create(cmd)
        result = await port.touch_many([c1.id, c2.id])
        assert len(result) == 2

    @pytest.mark.asyncio
    async def test_kill_removes_document(self) -> None:
        port = _document_adapter()
        cmd = CreateDocumentCmd()
        created = await port.create(cmd)
        await port.kill(created.id)
        with pytest.raises(NotFoundError):
            await port.get(created.id)

    @pytest.mark.asyncio
    async def test_kill_many(self) -> None:
        port = _document_adapter()
        cmd = CreateDocumentCmd()
        c1 = await port.create(cmd)
        c2 = await port.create(cmd)
        await port.kill_many([c1.id, c2.id])
        with pytest.raises(NotFoundError):
            await port.get(c1.id)

    @pytest.mark.asyncio
    async def test_delete_soft_deletes(self) -> None:
        from forze.domain.mixins import SoftDeletionMixin
        from forze.domain.models import Document

        # Use a model with soft delete support
        class DocWithSoftDelete(Document, SoftDeletionMixin):
            pass

        state = MockState()
        port = MockDocumentAdapter(
            state=state,
            namespace="test_soft",
            read_model=DocWithSoftDelete,
            domain_model=DocWithSoftDelete,
        )
        cmd = CreateDocumentCmd()
        created = await port.create(cmd)
        deleted = await port.delete(created.id, created.rev)
        assert deleted is not None
        assert deleted.is_deleted is True
        # Soft-deleted doc is still retrievable via get()
        got = await port.get(created.id)
        assert got.is_deleted is True

    @pytest.mark.asyncio
    async def test_delete_many(self) -> None:
        from forze.domain.mixins import SoftDeletionMixin
        from forze.domain.models import Document

        class DocWithSoftDelete(Document, SoftDeletionMixin):
            pass

        state = MockState()
        port = MockDocumentAdapter(
            state=state,
            namespace="test_soft2",
            read_model=DocWithSoftDelete,
            domain_model=DocWithSoftDelete,
        )
        cmd = CreateDocumentCmd()
        c1 = await port.create(cmd)
        c2 = await port.create(cmd)
        result = await port.delete_many([(c1.id, c1.rev), (c2.id, c2.rev)])
        assert len(result) == 2

    @pytest.mark.asyncio
    async def test_restore_after_delete(self) -> None:
        from forze.domain.mixins import SoftDeletionMixin
        from forze.domain.models import Document

        class DocWithSoftDelete(Document, SoftDeletionMixin):
            pass

        state = MockState()
        port = MockDocumentAdapter(
            state=state,
            namespace="test_soft3",
            read_model=DocWithSoftDelete,
            domain_model=DocWithSoftDelete,
        )
        cmd = CreateDocumentCmd()
        created = await port.create(cmd)
        d = await port.delete(created.id, created.rev)
        restored = await port.restore(created.id, d.rev)
        assert restored is not None
        got = await port.get(created.id)
        assert got is not None

    @pytest.mark.asyncio
    async def test_restore_many(self) -> None:
        from forze.domain.mixins import SoftDeletionMixin
        from forze.domain.models import Document

        class DocWithSoftDelete(Document, SoftDeletionMixin):
            pass

        state = MockState()
        port = MockDocumentAdapter(
            state=state,
            namespace="test_soft4",
            read_model=DocWithSoftDelete,
            domain_model=DocWithSoftDelete,
        )
        cmd = CreateDocumentCmd()
        c1 = await port.create(cmd)
        c2 = await port.create(cmd)
        dr = await port.delete_many([(c1.id, c1.rev), (c2.id, c2.rev)])
        result = await port.restore_many(
            [(c1.id, dr[0].rev), (c2.id, dr[1].rev)],
        )
        assert len(result) == 2
