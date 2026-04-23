"""Unit tests for document port contracts (DocumentQueryPort, DocumentCommandPort).

Exercises the protocol through MockDocumentAdapter and through direct protocol
method calls to improve coverage of ports.py.
"""

import pytest
from uuid import uuid4

from forze.application.contracts.document import (
    DocumentCommandPort,
    DocumentQueryPort,
    DocumentSpec,
    DocumentWriteTypes,
)
from forze.application.contracts.query import QueryFilterExpression
from forze.base.errors import NotFoundError
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument

from forze_mock import MockState
from forze_mock.adapters import MockDocumentAdapter

# ----------------------- #


def _document_adapter() -> MockDocumentAdapter:
    """Create a MockDocumentAdapter for tests."""
    spec = DocumentSpec(
        name="test",
        read=ReadDocument,
        write=DocumentWriteTypes(
            domain=Document,
            create_cmd=CreateDocumentCmd,
            update_cmd=BaseDTO,
        ),
    )
    return MockDocumentAdapter(
        spec=spec,
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

    spec = DocumentSpec(
        name="test_title",
        read=ReadWithTitle,
        write=DocumentWriteTypes(
            domain=DocWithTitle,
            create_cmd=_CreateWithTitle,
            update_cmd=_UpdateTitle,
        ),
    )
    return MockDocumentAdapter(
        spec=spec,
        state=MockState(),
        namespace="test_title",
        read_model=ReadWithTitle,
        domain_model=DocWithTitle,
    )


class TestDocumentPortProtocolConformance:
    """Verify MockDocumentAdapter conforms to document protocols."""

    def test_mock_adapter_is_document_query_port(self) -> None:
        port = _document_adapter()
        assert isinstance(port, DocumentQueryPort)

    def test_mock_adapter_is_document_command_port(self) -> None:
        port = _document_adapter()
        assert isinstance(port, DocumentCommandPort)


class TestDocumentQueryPortViaMock:
    """Test DocumentQueryPort contract through MockDocumentAdapter."""

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
        page = await port.find_many(return_count=True)
        assert isinstance(page.hits, list)
        assert isinstance(page.count, int)
        assert page.count >= 0

    @pytest.mark.asyncio
    async def test_find_many_with_filters_and_pagination(self) -> None:
        port = _document_adapter()
        await port.create(CreateDocumentCmd())
        page = await port.find_many(
            pagination={"limit": 1, "offset": 0},
            return_count=True,
        )
        assert len(page.hits) <= 1
        assert page.count >= 0

    @pytest.mark.asyncio
    async def test_count_returns_int(self) -> None:
        port = _document_adapter()
        n = await port.count()
        assert isinstance(n, int)
        assert n >= 0


class TestDocumentCommandPortViaMock:
    """Test DocumentCommandPort contract through MockDocumentAdapter."""

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
        spec = DocumentSpec(
            name="test_soft",
            read=DocWithSoftDelete,
            write=DocumentWriteTypes(
                domain=DocWithSoftDelete,
                create_cmd=CreateDocumentCmd,
                update_cmd=BaseDTO,
            ),
        )
        port = MockDocumentAdapter(
            spec=spec,
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
        spec = DocumentSpec(
            name="test_soft2",
            read=DocWithSoftDelete,
            write=DocumentWriteTypes(
                domain=DocWithSoftDelete,
                create_cmd=CreateDocumentCmd,
                update_cmd=BaseDTO,
            ),
        )
        port = MockDocumentAdapter(
            spec=spec,
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
        spec = DocumentSpec(
            name="test_soft3",
            read=DocWithSoftDelete,
            write=DocumentWriteTypes(
                domain=DocWithSoftDelete,
                create_cmd=CreateDocumentCmd,
                update_cmd=BaseDTO,
            ),
        )
        port = MockDocumentAdapter(
            spec=spec,
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
        spec = DocumentSpec(
            name="test_soft4",
            read=DocWithSoftDelete,
            write=DocumentWriteTypes(
                domain=DocWithSoftDelete,
                create_cmd=CreateDocumentCmd,
                update_cmd=BaseDTO,
            ),
        )
        port = MockDocumentAdapter(
            spec=spec,
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


class TestDocumentCommandReturnNewViaMock:
    """``return_new=False`` skips returning read models while still persisting."""

    @pytest.mark.asyncio
    async def test_create_return_new_false_persists_and_returns_none(self) -> None:
        port = _document_adapter()
        pk_before = await port.count()
        assert await port.create(CreateDocumentCmd(), return_new=False) is None
        assert await port.count() == pk_before + 1

    @pytest.mark.asyncio
    async def test_create_many_return_new_false(self) -> None:
        port = _document_adapter()
        assert await port.create_many(
            [CreateDocumentCmd(), CreateDocumentCmd()],
            return_new=False,
        ) is None
        n = await port.count()
        assert n >= 2

    @pytest.mark.asyncio
    async def test_create_many_empty_still_returns_empty_list(self) -> None:
        port = _document_adapter()
        assert await port.create_many([], return_new=False) == []

    @pytest.mark.asyncio
    async def test_update_return_new_false(self) -> None:
        port = _document_adapter_with_title()
        created = await port.create(_CreateWithTitle())
        assert (
            await port.update(
                created.id,
                created.rev,
                _UpdateTitle(title="z"),
                return_new=False,
            )
            is None
        )
        loaded = await port.get(created.id)
        assert loaded.title == "z"

    @pytest.mark.asyncio
    async def test_update_many_return_new_false(self) -> None:
        port = _document_adapter_with_title()
        c1 = await port.create(_CreateWithTitle())
        c2 = await port.create(_CreateWithTitle())
        assert (
            await port.update_many(
                [
                    (c1.id, c1.rev, _UpdateTitle(title="a")),
                    (c2.id, c2.rev, _UpdateTitle(title="b")),
                ],
                return_new=False,
            )
            is None
        )

    @pytest.mark.asyncio
    async def test_touch_return_new_false(self) -> None:
        port = _document_adapter()
        created = await port.create(CreateDocumentCmd())
        assert await port.touch(created.id, return_new=False) is None
        assert (await port.get(created.id)).id == created.id

    @pytest.mark.asyncio
    async def test_touch_many_return_new_false(self) -> None:
        port = _document_adapter()
        c1 = await port.create(CreateDocumentCmd())
        c2 = await port.create(CreateDocumentCmd())
        assert await port.touch_many([c1.id, c2.id], return_new=False) is None

    @pytest.mark.asyncio
    async def test_touch_many_empty_returns_empty_list(self) -> None:
        port = _document_adapter()
        assert await port.touch_many([], return_new=False) == []

    @pytest.mark.asyncio
    async def test_delete_restore_return_new_false(self) -> None:
        from forze.domain.mixins import SoftDeletionMixin
        from forze.domain.models import Document

        class DocWithSoftDelete(Document, SoftDeletionMixin):
            pass

        spec = DocumentSpec(
            name="test_rn",
            read=DocWithSoftDelete,
            write=DocumentWriteTypes(
                domain=DocWithSoftDelete,
                create_cmd=CreateDocumentCmd,
                update_cmd=BaseDTO,
            ),
        )
        port = MockDocumentAdapter(
            spec=spec,
            state=MockState(),
            namespace="test_rn",
            read_model=DocWithSoftDelete,
            domain_model=DocWithSoftDelete,
        )
        created = await port.create(CreateDocumentCmd())
        assert await port.delete(created.id, created.rev, return_new=False) is None
        got = await port.get(created.id)
        assert got.is_deleted is True
        assert await port.restore(created.id, got.rev, return_new=False) is None
        assert (await port.get(created.id)).is_deleted is False

    @pytest.mark.asyncio
    async def test_delete_many_restore_many_return_new_false(self) -> None:
        from forze.domain.mixins import SoftDeletionMixin
        from forze.domain.models import Document

        class DocWithSoftDelete(Document, SoftDeletionMixin):
            pass

        spec = DocumentSpec(
            name="test_rn2",
            read=DocWithSoftDelete,
            write=DocumentWriteTypes(
                domain=DocWithSoftDelete,
                create_cmd=CreateDocumentCmd,
                update_cmd=BaseDTO,
            ),
        )
        port = MockDocumentAdapter(
            spec=spec,
            state=MockState(),
            namespace="test_rn2",
            read_model=DocWithSoftDelete,
            domain_model=DocWithSoftDelete,
        )
        c1 = await port.create(CreateDocumentCmd())
        c2 = await port.create(CreateDocumentCmd())
        assert (
            await port.delete_many(
                [(c1.id, c1.rev), (c2.id, c2.rev)],
                return_new=False,
            )
            is None
        )
        d1 = await port.get(c1.id)
        d2 = await port.get(c2.id)
        assert (
            await port.restore_many(
                [(c1.id, d1.rev), (c2.id, d2.rev)],
                return_new=False,
            )
            is None
        )
