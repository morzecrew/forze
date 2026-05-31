"""Integration tests for document handlers with in-memory adapters."""

import pytest

from forze.application.contracts.document import DocumentSpec
from forze.application.execution import Deps, ExecutionContext
from forze.application.handlers.document import GetDocument
from forze.application.handlers.document.dto import DocumentIdDTO, DocumentIdRevDTO
from tests.support.execution_context import context_from_deps, context_from_modules, frozen_deps_from_deps
from pydantic import PositiveInt

from forze_patterns.soft_deletion import DeleteDocument, RestoreDocument
from forze_patterns.soft_deletion.models import (
    DocWithSoftDeletion,
    UpdateCmdWithSoftDeletion,
)
from forze.domain.models import CreateDocumentCmd, Document, ReadDocument
from forze_mock import MockDepsModule, MockState

# ----------------------- #


class NumberedDocument(Document):
    number_id: PositiveInt


class NumberedCreateCmd(CreateDocumentCmd):
    number_id: PositiveInt


class NumberedReadDocument(ReadDocument):
    number_id: PositiveInt


class SoftDeletableDocument(DocWithSoftDeletion):
    pass


class SoftDeletableReadDocument(ReadDocument):
    is_deleted: bool = False


class SoftUpdateCmd(UpdateCmdWithSoftDeletion):
    pass


@pytest.mark.integration
@pytest.mark.asyncio
async def test_find_by_number_id_roundtrip_integration() -> None:
    state = MockState()
    deps = Deps.plain(dict(MockDepsModule(state=state)().plain_deps))
    ctx = context_from_deps(deps)
    spec = DocumentSpec(
        name="orders",
        read=NumberedReadDocument,
        write={
            "domain": NumberedDocument,
            "create_cmd": NumberedCreateCmd,
            "update_cmd": CreateDocumentCmd,
        },
    )

    doc_command = ctx.document.command(spec)
    doc_query = ctx.document.query(spec)

    created = await doc_command.create(dto=NumberedCreateCmd(number_id=7))
    found = await doc_query.find(filters={"$values": {"number_id": 7}})

    assert found is not None
    assert found.id == created.id
    assert found.number_id == created.number_id


@pytest.mark.integration
@pytest.mark.asyncio
async def test_soft_delete_hides_document_until_restore_integration() -> None:
    state = MockState()
    deps = Deps.plain(dict(MockDepsModule(state=state)().plain_deps))
    ctx = context_from_deps(deps)
    spec = DocumentSpec(
        name="orders",
        read=SoftDeletableReadDocument,
        write={
            "domain": SoftDeletableDocument,
            "create_cmd": CreateDocumentCmd,
            "update_cmd": SoftUpdateCmd,
        },
    )

    doc_command = ctx.document.command(spec)
    doc_query = ctx.document.query(spec)
    delete_handler = DeleteDocument(doc=doc_command)
    restore_handler = RestoreDocument(doc=doc_command)
    get_handler = GetDocument[SoftDeletableReadDocument](doc=doc_query)

    created = await doc_command.create(dto=CreateDocumentCmd())
    deleted = await delete_handler(
        DocumentIdRevDTO(id=created.id, rev=created.rev),
    )

    assert deleted.is_deleted is True

    fetched_deleted = await get_handler(DocumentIdDTO(id=created.id))
    assert fetched_deleted.is_deleted is True

    restored = await restore_handler(
        DocumentIdRevDTO(id=created.id, rev=deleted.rev),
    )
    refetched = await get_handler(DocumentIdDTO(id=created.id))

    assert restored.is_deleted is False
    assert refetched.id == created.id
    assert refetched.is_deleted is False
