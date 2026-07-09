"""`soft_delete_wiring` — read-side exclusion, delete/restore, and an optional purge (mock).

Builds a plain aggregate on the `is_deleted` mixins, wires it, and drives ops through
`run_operation`: LIST and GET exclude soft-deleted rows, DELETE/RESTORE flip the flag, and an
optional after-commit purge fires when a row is deleted.
"""

from __future__ import annotations

import pytest

from forze import build_runtime
from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.execution.operations import run_operation
from forze.base.exceptions import CoreException, ExceptionKind
from forze.domain.models import CreateDocumentCmd, ReadDocument
from forze_kits.aggregates.document import build_document_registry
from forze_kits.aggregates.document.dto import (
    DocumentIdDTO,
    DocumentIdRevDTO,
    ListRequestDTO,
)
from forze_kits.aggregates.document.operations import DocumentKernelOp
from forze_kits.aggregates.soft_deletion import (
    SoftDeletionKernelOp,
    soft_delete_wiring,
)
from forze_kits.aggregates.soft_deletion.wiring import _merge_exclusion
from forze_kits.domain.soft_deletion import (
    DocWithSoftDeletion,
    UpdateCmdWithSoftDeletion,
)
from forze_mock import MockDepsModule

# ----------------------- #

_MOCK_TX = "mock"


class Note(DocWithSoftDeletion):
    title: str = ""


class NoteCreate(CreateDocumentCmd):
    title: str


class NoteUpdate(UpdateCmdWithSoftDeletion):
    title: str | None = None


class NoteRead(ReadDocument):
    title: str = ""
    is_deleted: bool = False  # exposed so GET can exclude a soft-deleted row


NOTE_SPEC = DocumentSpec(
    name="notes",
    read=NoteRead,
    write=DocumentWriteTypes(
        domain=Note, create_cmd=NoteCreate, update_cmd=NoteUpdate
    ),
)


def _key(op) -> str:
    return NOTE_SPEC.default_namespace.key(op)


def _registry(*, purge=None):
    wiring = soft_delete_wiring(NOTE_SPEC, purge=purge)
    reg = build_document_registry(NOTE_SPEC, mappers=wiring.read_mappers())
    return wiring.bind(reg, tx_route=_MOCK_TX).freeze()


async def _create(reg, ctx, title: str):
    return await run_operation(reg, _key(DocumentKernelOp.CREATE), NoteCreate(title=title), ctx)


async def _delete(reg, ctx, note):
    return await run_operation(
        reg, _key(SoftDeletionKernelOp.DELETE), DocumentIdRevDTO(id=note.id, rev=note.rev), ctx
    )


# ....................... #


class TestMergeExclusion:
    def test_injects_is_deleted_false_and_conjoins_caller_filter(self) -> None:
        assert _merge_exclusion(None) == {"$values": {"is_deleted": False}}

        merged = _merge_exclusion({"$values": {"title": "x"}})
        assert merged == {
            "$and": [
                {"$values": {"is_deleted": False}},
                {"$values": {"title": "x"}},
            ]
        }


# ....................... #


class TestReadSideExclusion:
    async def test_list_excludes_soft_deleted(self) -> None:
        runtime = build_runtime(MockDepsModule())
        reg = _registry()

        async with runtime.scope():
            ctx = runtime.get_context()
            kept = await _create(reg, ctx, "kept")
            gone = await _create(reg, ctx, "gone")
            await _delete(reg, ctx, gone)

            listed = await run_operation(reg, _key(DocumentKernelOp.LIST), ListRequestDTO(), ctx)
            assert listed.count == 1  # only the live note; the soft-deleted one is filtered out
            assert kept.id != gone.id

    async def test_get_rejects_soft_deleted_but_serves_live(self) -> None:
        runtime = build_runtime(MockDepsModule())
        reg = _registry()

        async with runtime.scope():
            ctx = runtime.get_context()
            live = await _create(reg, ctx, "live")
            gone = await _create(reg, ctx, "gone")
            await _delete(reg, ctx, gone)

            fetched = await run_operation(reg, _key(DocumentKernelOp.GET), DocumentIdDTO(id=live.id), ctx)
            assert fetched.id == live.id

            with pytest.raises(CoreException) as ei:
                await run_operation(reg, _key(DocumentKernelOp.GET), DocumentIdDTO(id=gone.id), ctx)
            assert ei.value.kind is ExceptionKind.NOT_FOUND

    async def test_restore_makes_the_row_visible_again(self) -> None:
        runtime = build_runtime(MockDepsModule())
        reg = _registry()

        async with runtime.scope():
            ctx = runtime.get_context()
            note = await _create(reg, ctx, "note")
            deleted = await _delete(reg, ctx, note)

            await run_operation(
                reg,
                _key(SoftDeletionKernelOp.RESTORE),
                DocumentIdRevDTO(id=note.id, rev=deleted.rev),
                ctx,
            )

            listed = await run_operation(reg, _key(DocumentKernelOp.LIST), ListRequestDTO(), ctx)
            assert listed.count == 1  # restored back into the live set
            restored = await run_operation(reg, _key(DocumentKernelOp.GET), DocumentIdDTO(id=note.id), ctx)
            assert restored.id == note.id


# ....................... #


class TestPurgeHook:
    async def test_purge_fires_after_commit_on_delete(self) -> None:
        purged: list = []

        async def _purge(ctx, row) -> None:  # noqa: ARG001
            purged.append(row.id)

        runtime = build_runtime(MockDepsModule())
        reg = _registry(purge=_purge)

        async with runtime.scope():
            ctx = runtime.get_context()
            note = await _create(reg, ctx, "note")
            await _delete(reg, ctx, note)

            assert purged == [note.id]  # purge ran with the soft-deleted read model
