"""Unit tests for stored-file handler orchestration."""

import pytest

from forze.application.contracts.outbox import OutboxSpec
from forze.base.exceptions import CoreException
from forze.base.serialization import PydanticModelCodec
from forze_kits.aggregates.stored_file.handlers import (
    DownloadStoredFile,
    GetStoredFile,
    ListStoredFiles,
    ListStoredFilesRequestDTO,
    SoftDeleteStoredFile,
    StoredFileIdDTO,
    StoredFileIdRevDTO,
    UploadStoredFile,
    UploadStoredFileRequestDTO,
)
from forze_kits.aggregates.stored_file.handlers._helpers import (
    complete_stored_file_upload,
    merge_list_filters,
    purge_stored_file_blob,
)
from forze_kits.domain.stored_file import (
    StoredFileKitSpec,
    StoredFileStatus,
)
from forze_kits.domain.stored_file import StoredFileOutboxPayload


def _kit(*, with_search: bool = False, with_outbox: bool = False) -> StoredFileKitSpec:
    search = StoredFileKitSpec.default_search("files") if with_search else None
    outbox = (
        OutboxSpec(name="files", codec=PydanticModelCodec(StoredFileOutboxPayload))
        if with_outbox
        else None
    )
    return StoredFileKitSpec(name="files", search=search, outbox=outbox)


class TestStoredFileHandlers:
    @pytest.mark.asyncio
    async def test_upload_then_complete_and_download(self, stub_ctx) -> None:
        kit = _kit(with_outbox=True)
        doc = stub_ctx.doc.command(kit.document)
        upload = UploadStoredFile(
            doc=doc,
            outbox=stub_ctx.outbox.command(kit.outbox),
        )
        args = UploadStoredFileRequestDTO(
            filename="hello.txt",
            data=b"hello",
            prefix="docs",
        )
        pending = await upload(args)
        assert pending.status == StoredFileStatus.PENDING

        ready = await complete_stored_file_upload(
            kit=kit,
            ctx=stub_ctx,
            args=args,
            pending=pending,
            outbox=stub_ctx.outbox.command(kit.outbox),
        )
        assert ready.status == StoredFileStatus.READY
        assert ready.storage_key is not None

        download = DownloadStoredFile(
            doc=stub_ctx.doc.query(kit.document),
            storage=stub_ctx.storage(kit.resolved_storage),
        )
        result = await download(StoredFileIdDTO(id=ready.id))
        assert result.data == b"hello"
        assert result.filename == "hello.txt"

    @pytest.mark.asyncio
    async def test_soft_delete_purges_blob(self, stub_ctx) -> None:
        kit = _kit(with_search=True, with_outbox=True)
        doc_cmd = stub_ctx.doc.command(kit.document)
        upload = UploadStoredFile(
            doc=doc_cmd,
            outbox=stub_ctx.outbox.command(kit.outbox),
        )
        args = UploadStoredFileRequestDTO(filename="gone.txt", data=b"x")
        pending = await upload(args)
        ready = await complete_stored_file_upload(
            kit=kit,
            ctx=stub_ctx,
            args=args,
            pending=pending,
            search=stub_ctx.search.command(kit.search_spec),
        )

        delete = SoftDeleteStoredFile(
            doc=doc_cmd,
            outbox=stub_ctx.outbox.command(kit.outbox),
        )
        deleted = await delete(StoredFileIdRevDTO(id=ready.id, rev=ready.rev))
        assert deleted.status == StoredFileStatus.DELETED

        await purge_stored_file_blob(
            kit=kit,
            ctx=stub_ctx,
            file_id=deleted.id,
            storage_key=deleted.storage_key,
            search=stub_ctx.search.command(kit.search_spec),
        )

        storage = stub_ctx.storage(kit.resolved_storage)
        _, total = await storage.list(limit=10, offset=0)
        assert total == 0

        get = GetStoredFile(doc=stub_ctx.doc.query(kit.document))
        with pytest.raises(CoreException):
            await get(StoredFileIdDTO(id=ready.id))

    @pytest.mark.asyncio
    async def test_list_excludes_deleted_by_default(self, stub_ctx) -> None:
        kit = _kit()
        doc_cmd = stub_ctx.doc.command(kit.document)
        upload = UploadStoredFile(doc=doc_cmd)
        pending = await upload(
            UploadStoredFileRequestDTO(filename="a.txt", data=b"a")
        )
        ready = await complete_stored_file_upload(
            kit=kit,
            ctx=stub_ctx,
            args=UploadStoredFileRequestDTO(filename="a.txt", data=b"a"),
            pending=pending,
        )
        await SoftDeleteStoredFile(doc=doc_cmd)(
            StoredFileIdRevDTO(id=ready.id, rev=ready.rev)
        )

        listed = await ListStoredFiles(doc=stub_ctx.doc.query(kit.document))(
            ListStoredFilesRequestDTO(page=1, size=10)
        )
        assert listed.count == 0

    @pytest.mark.asyncio
    async def test_list_prefix_filter(self, stub_ctx) -> None:
        kit = _kit()
        doc_cmd = stub_ctx.doc.command(kit.document)
        upload = UploadStoredFile(doc=doc_cmd)

        for name, prefix in (("a.txt", "docs"), ("b.txt", "tmp")):
            args = UploadStoredFileRequestDTO(
                filename=name,
                data=b"x",
                prefix=prefix,
            )
            pending = await upload(args)
            await complete_stored_file_upload(
                kit=kit,
                ctx=stub_ctx,
                args=args,
                pending=pending,
            )

        listed = await ListStoredFiles(doc=stub_ctx.doc.query(kit.document))(
            ListStoredFilesRequestDTO(page=1, size=10, prefix="docs")
        )
        assert listed.count == 1
        assert listed.hits[0].filename == "a.txt"


class TestMergeListFilters:
    def _statuses(self, expr) -> set[str]:
        return set(expr["$values"]["status"]["$in"])

    def test_include_deleted_keeps_deleted_when_pending_excluded(self) -> None:
        # Regression: include_deleted must not be silently dropped when
        # include_pending=False. Both READY and DELETED rows stay allowed.
        expr = merge_list_filters(
            None,
            prefix=None,
            include_deleted=True,
            include_pending=False,
        )
        assert self._statuses(expr) == {
            StoredFileStatus.READY.value,
            StoredFileStatus.DELETED.value,
        }

    def test_default_excludes_only_deleted(self) -> None:
        expr = merge_list_filters(
            None,
            prefix=None,
            include_deleted=False,
            include_pending=True,
        )
        assert StoredFileStatus.DELETED.value not in self._statuses(expr)
        assert StoredFileStatus.PENDING.value in self._statuses(expr)

    def test_all_included_returns_no_status_constraint(self) -> None:
        assert (
            merge_list_filters(
                None,
                prefix=None,
                include_deleted=True,
                include_pending=True,
            )
            is None
        )
