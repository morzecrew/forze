"""Unit tests for stored-file after_commit stage factories."""

import pytest

from forze.application.contracts.outbox import OutboxSpec
from forze.base.exceptions import CoreException
from forze.base.serialization import PydanticModelCodec
from forze_kits.aggregates.stored_file import (
    SoftDeleteStoredFile,
    StoredFileIdRevDTO,
    StoredFileOutboxPayload,
    UploadStoredFile,
    UploadStoredFileRequestDTO,
    stored_file_complete_upload_after_commit_factory,
    stored_file_purge_blob_after_commit_factory,
)
from forze_kits.domain.stored_file import StoredFileKitSpec, StoredFileStatus


def _kit() -> StoredFileKitSpec:
    return StoredFileKitSpec(
        name="files",
        outbox=OutboxSpec(
            name="files",
            codec=PydanticModelCodec(StoredFileOutboxPayload),
        ),
    )


class TestStoredFileStages:
    @pytest.mark.asyncio
    async def test_complete_upload_after_commit_factory(self, stub_ctx) -> None:
        kit = _kit()
        doc = stub_ctx.doc.command(kit.document)
        args = UploadStoredFileRequestDTO(filename="stage.txt", data=b"payload")
        pending = await UploadStoredFile(
            doc=doc,
            outbox=stub_ctx.outbox.command(kit.outbox),
        )(args)

        hook = stored_file_complete_upload_after_commit_factory(kit)(stub_ctx)
        await hook(args, pending)

        ready = await stub_ctx.doc.query(kit.document).get(pending.id)
        assert ready.status == StoredFileStatus.READY
        assert ready.storage_key is not None

        downloaded = await stub_ctx.storage.query(kit.resolved_storage).download(ready.storage_key)
        assert downloaded.data == b"payload"

    @pytest.mark.asyncio
    async def test_purge_blob_after_commit_factory(self, stub_ctx) -> None:
        kit = _kit()
        doc = stub_ctx.doc.command(kit.document)
        args = UploadStoredFileRequestDTO(filename="purge.txt", data=b"payload")
        pending = await UploadStoredFile(
            doc=doc,
            outbox=stub_ctx.outbox.command(kit.outbox),
        )(args)

        complete = stored_file_complete_upload_after_commit_factory(kit)(stub_ctx)
        await complete(args, pending)
        ready = await stub_ctx.doc.query(kit.document).get(pending.id)
        assert ready.storage_key is not None

        deleted = await SoftDeleteStoredFile(doc=doc)(
            StoredFileIdRevDTO(id=ready.id, rev=ready.rev)
        )
        assert deleted.status == StoredFileStatus.DELETED

        purge = stored_file_purge_blob_after_commit_factory(kit)(stub_ctx)
        await purge(None, deleted)

        with pytest.raises(CoreException):
            await stub_ctx.storage.query(kit.resolved_storage).download(ready.storage_key)

    @pytest.mark.asyncio
    async def test_keeping_the_blob_still_drops_the_index_entry(self, stub_ctx) -> None:
        kit = StoredFileKitSpec(name="files", search=StoredFileKitSpec.default_search("files"))
        doc = stub_ctx.doc.command(kit.document)
        index = stub_ctx.search.query(kit.search_spec)
        args = UploadStoredFileRequestDTO(filename="kept.txt", data=b"payload")
        pending = await UploadStoredFile(doc=doc)(args)

        await stored_file_complete_upload_after_commit_factory(kit)(stub_ctx)(args, pending)
        ready = await stub_ctx.doc.query(kit.document).get(pending.id)
        assert [hit.id for hit in (await index.search("kept")).hits] == [ready.id]

        deleted = await SoftDeleteStoredFile(doc=doc)(StoredFileIdRevDTO(id=ready.id, rev=ready.rev))
        hook = stored_file_purge_blob_after_commit_factory(kit, keep_blob=True)(stub_ctx)
        await hook(None, deleted)

        stored = await stub_ctx.storage.query(kit.resolved_storage).download(ready.storage_key)
        assert stored.data == b"payload"
        assert (await index.search("kept")).hits == []
