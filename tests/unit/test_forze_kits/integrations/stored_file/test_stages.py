"""Unit tests for stored-file after_commit stage factories."""

import pytest

from forze.application.contracts.outbox import OutboxSpec
from forze.base.serialization import PydanticModelCodec
from forze_kits.aggregates.stored_file import (
    StoredFileOutboxPayload,
    UploadStoredFile,
    UploadStoredFileRequestDTO,
    stored_file_complete_upload_after_commit_factory,
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

        downloaded = await stub_ctx.storage.query(kit.resolved_storage).download(
            ready.storage_key
        )
        assert downloaded.data == b"payload"
