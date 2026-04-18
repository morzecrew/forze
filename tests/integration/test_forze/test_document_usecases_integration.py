"""Integration tests for document usecases with in-memory adapters."""

import pytest

from forze.application.contracts.document import DocumentSpec
from forze.application.dto import DocumentIdDTO, DocumentIdRevDTO, DocumentNumberIdDTO
from forze.application.execution import Deps, ExecutionContext
from forze.application.usecases.document import (
    DeleteDocument,
    GetDocument,
    GetDocumentByNumberId,
    RestoreDocument,
)
from forze.domain.mixins import NumberCreateCmdMixin, NumberMixin, SoftDeletionMixin
from forze.domain.models import CreateDocumentCmd, Document, ReadDocument
from forze_mock import MockDepsModule, MockState


class NumberedDocument(Document, NumberMixin):
    pass


class NumberedCreateCmd(CreateDocumentCmd, NumberCreateCmdMixin):
    pass


class NumberedReadDocument(ReadDocument, NumberMixin):
    pass


class SoftDeletableDocument(Document, SoftDeletionMixin):
    pass


class SoftDeletableReadDocument(ReadDocument, SoftDeletionMixin):
    pass


@pytest.mark.integration
@pytest.mark.asyncio
async def test_get_by_number_id_roundtrip_integration() -> None:
    state = MockState()
    deps = Deps.plain(dict(MockDepsModule(state=state)().plain_deps))
    ctx = ExecutionContext(deps=deps)
    spec = DocumentSpec(
        name="orders",
        read=NumberedReadDocument,
        write={
            "domain": NumberedDocument,
            "create_cmd": NumberedCreateCmd,
            "update_cmd": CreateDocumentCmd,
        },
    )

    doc_command = ctx.doc_command(spec)
    getter = GetDocumentByNumberId[NumberedReadDocument](ctx=ctx, doc=ctx.doc_query(spec))

    created = await doc_command.create(dto=NumberedCreateCmd(number_id=7))
    fetched = await getter(DocumentNumberIdDTO(number_id=7))

    assert fetched.id == created.id
    assert fetched.number_id == created.number_id


@pytest.mark.integration
@pytest.mark.asyncio
async def test_soft_delete_hides_document_until_restore_integration() -> None:
    state = MockState()
    deps = Deps.plain(dict(MockDepsModule(state=state)().plain_deps))
    ctx = ExecutionContext(deps=deps)
    spec = DocumentSpec(
        name="orders",
        read=SoftDeletableReadDocument,
        write={"domain": SoftDeletableDocument, "create_cmd": CreateDocumentCmd, "update_cmd": CreateDocumentCmd},
    )

    doc_command = ctx.doc_command(spec)
    delete_uc = DeleteDocument[SoftDeletableReadDocument](ctx=ctx, doc=ctx.doc_command(spec))
    restore_uc = RestoreDocument[SoftDeletableReadDocument](ctx=ctx, doc=ctx.doc_command(spec))
    get_uc = GetDocument[SoftDeletableReadDocument](ctx=ctx, doc=ctx.doc_query(spec))

    created = await doc_command.create(dto=CreateDocumentCmd())
    deleted = await delete_uc(DocumentIdRevDTO(id=created.id, rev=created.rev))

    assert deleted.is_deleted is True

    fetched_deleted = await get_uc(DocumentIdDTO(id=created.id))
    assert fetched_deleted.is_deleted is True

    restored = await restore_uc(DocumentIdRevDTO(id=created.id, rev=deleted.rev))
    refetched = await get_uc(DocumentIdDTO(id=created.id))

    assert restored.is_deleted is False
    assert refetched.id == created.id
    assert refetched.is_deleted is False
