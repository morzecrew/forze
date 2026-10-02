"""A spec declaring ``hard_delete=False`` keeps every row: nothing generated or hand-written erases one.

The document factory stops registering the kill operation, so no route or tool reaches it, and the
command port refuses ``kill``/``kill_many`` for anything that calls it directly.
"""

from __future__ import annotations

import pytest

from forze import build_runtime
from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.execution.operations.registry import OperationRegistry
from forze.base.exceptions import CoreException, ExceptionKind
from forze.domain.models import CreateDocumentCmd, ReadDocument
from forze_kits.aggregates import AggregateKit
from forze_kits.aggregates.document import DocumentKernelOp, build_document_registry
from forze_kits.aggregates.document.handlers import KillDocument
from forze_kits.domain.soft_deletion import DocWithSoftDeletion, UpdateCmdWithSoftDeletion
from forze_mock import MockDepsModule

# ----------------------- #


class Note(DocWithSoftDeletion):
    title: str = ""


class NoteCreate(CreateDocumentCmd):
    title: str


class NoteUpdate(UpdateCmdWithSoftDeletion):
    title: str | None = None


class NoteRead(ReadDocument):
    title: str = ""
    is_deleted: bool = False


def _spec(*, hard_delete: bool = True) -> DocumentSpec[NoteRead, Note, NoteCreate, NoteUpdate]:
    return DocumentSpec(
        name="notes",
        read=NoteRead,
        write=DocumentWriteTypes(domain=Note, create_cmd=NoteCreate, update_cmd=NoteUpdate),
        hard_delete=hard_delete,
    )


_KILL = _spec().default_namespace.key(DocumentKernelOp.KILL)


def _refused(exc_info: pytest.ExceptionInfo[CoreException]) -> tuple[ExceptionKind, str]:
    return exc_info.value.kind, exc_info.value.code


# ....................... #


class TestTheDeclaration:
    def test_rows_may_be_erased_by_default(self) -> None:
        spec = DocumentSpec(
            name="notes",
            read=NoteRead,
            write=DocumentWriteTypes(domain=Note, create_cmd=NoteCreate, update_cmd=NoteUpdate),
        )

        assert spec.hard_delete is True
        assert _KILL in build_document_registry(spec).operation_keys()

    def test_the_refusal_names_the_spec(self) -> None:
        with pytest.raises(CoreException) as ei:
            _spec(hard_delete=False).require_hard_delete()

        assert _refused(ei) == (ExceptionKind.CONFIGURATION, "hard_delete_forbidden")
        assert ei.value.details == {"spec": "notes"}

    def test_an_erasable_spec_passes(self) -> None:
        _spec().require_hard_delete()


# ....................... #


class TestTheGeneratedOperations:
    def test_the_registry_has_no_kill(self) -> None:
        keys = build_document_registry(_spec(hard_delete=False)).operation_keys()

        assert _KILL not in keys
        # Only the erasing write goes; create and update stay.
        assert {
            _spec().default_namespace.key(op)
            for op in (DocumentKernelOp.CREATE, DocumentKernelOp.UPDATE)
        } <= keys

    def test_the_catalog_advertises_no_kill(self) -> None:
        descriptors = build_document_registry(_spec(hard_delete=False)).get_descriptors()

        assert _KILL not in descriptors
        assert _spec().default_namespace.key(DocumentKernelOp.CREATE) in descriptors

    def test_an_erasable_spec_keeps_kill(self) -> None:
        reg = build_document_registry(_spec())

        assert _KILL in reg.operation_keys()
        assert _KILL in reg.get_descriptors()

    def test_the_kit_composes_without_kill(self) -> None:
        # Every arm that binds the write ops checks which ones exist.
        kit = AggregateKit(
            spec=_spec(hard_delete=False), soft_delete=True, transactional_writes=True
        )

        assert _KILL not in kit.registry(tx_route="mock").handlers

    def test_the_kit_refuses_a_kill_handler(self) -> None:
        # The escape hatch would put back the operation the spec declared away.
        spec = _spec(hard_delete=False)

        with pytest.raises(CoreException, match="hard_delete") as ei:
            AggregateKit(
                spec=spec,
                handlers={
                    DocumentKernelOp.KILL: lambda ctx: KillDocument(doc=ctx.doc.command(spec))
                },
            )

        assert ei.value.kind is ExceptionKind.CONFIGURATION

    def test_the_kit_refuses_a_kill_merged_in(self) -> None:
        spec = _spec(hard_delete=False)
        extra = OperationRegistry(
            handlers={_KILL: lambda ctx: KillDocument(doc=ctx.doc.command(spec))}
        )

        with pytest.raises(CoreException, match="hard_delete") as ei:
            AggregateKit(spec=spec, extra_ops=extra)

        assert ei.value.kind is ExceptionKind.CONFIGURATION

    def test_an_erasable_spec_keeps_its_kill_override(self) -> None:
        kit = AggregateKit(
            spec=_spec(),
            handlers={
                DocumentKernelOp.KILL: lambda ctx: KillDocument(doc=ctx.doc.command(_spec()))
            },
        )

        assert _KILL in kit.registry(tx_route="mock").handlers


# ....................... #


class TestThePort:
    async def test_kill_is_refused_and_the_row_stays(self) -> None:
        spec = _spec(hard_delete=False)
        runtime = build_runtime(MockDepsModule())

        async with runtime.scope():
            cmd = runtime.get_context().document.command(spec)
            note = await cmd.create(NoteCreate(title="kept"))

            with pytest.raises(CoreException) as ei:
                await cmd.kill(note.id)

            assert _refused(ei) == (ExceptionKind.CONFIGURATION, "hard_delete_forbidden")

            with pytest.raises(CoreException) as ei:
                await cmd.kill_many([note.id])

            assert _refused(ei) == (ExceptionKind.CONFIGURATION, "hard_delete_forbidden")
            assert (await cmd.get(note.id)).id == note.id

    async def test_an_empty_kill_many_is_refused_too(self) -> None:
        # The declaration is about the spec, not about how many rows a call names.
        runtime = build_runtime(MockDepsModule())

        async with runtime.scope():
            cmd = runtime.get_context().document.command(_spec(hard_delete=False))

            with pytest.raises(CoreException) as ei:
                await cmd.kill_many([])

            assert _refused(ei) == (ExceptionKind.CONFIGURATION, "hard_delete_forbidden")

    async def test_an_erasable_spec_still_kills(self) -> None:
        runtime = build_runtime(MockDepsModule())

        async with runtime.scope():
            cmd = runtime.get_context().document.command(_spec())
            note = await cmd.create(NoteCreate(title="gone"))

            await cmd.kill(note.id)

            assert await cmd.count() == 0
