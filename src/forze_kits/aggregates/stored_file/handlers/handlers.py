from collections.abc import Callable, Sequence
from typing import Final

import attrs

from forze.application.contracts.document import DocumentCommandPort, DocumentQueryPort
from forze.application.contracts.execution import Handler
from forze.application.contracts.outbox import OutboxCommandPort
from forze.application.contracts.search import SearchCommandPort, SearchQueryPort
from forze.application.contracts.storage import (
    ObjectHead,
    StorageCommandPort,
    StorageQueryPort,
    StorageUploadSessionPort,
    UploadPart,
    UploadSession,
)
from forze.application.contracts.tenancy import TenantIdentity
from forze.base.exceptions import CoreException, ExceptionKind, exc
from forze.base.primitives import uuid7
from forze_kits.aggregates.search.dto import (
    SearchPaginated,
    SearchRequestDTO,
)
from forze_kits.aggregates.search.handlers import Search
from forze_kits.aggregates.storage.dto import PresignedUrlDTO, UploadPartDTO
from forze_kits.domain.stored_file import (
    StoredFileCreateCmd,
    StoredFileDocument,
    StoredFileOutboxPayload,
    StoredFileRead,
    StoredFileStatus,
    StoredFileUpdateCmd,
)
from forze_kits.dto.paginated import Paginated

from ..dto import (
    CompleteStoredFileUploadRequestDTO,
    DownloadStoredFileRangeRequestDTO,
    InitiateStoredFileUploadRequestDTO,
    ListStoredFilesRequestDTO,
    PresignStoredFileDownloadRequestDTO,
    PresignStoredFileUploadPartRequestDTO,
    StoredFileDownloadDTO,
    StoredFileIdDTO,
    StoredFileIdRevDTO,
    StoredFilePresignedDownloadDTO,
    StoredFileRangeDTO,
    StoredFileStream,
    StoredFileUploadSessionDTO,
    UploadStoredFileRequestDTO,
)
from ._helpers import (
    direct_upload_key,
    direct_upload_refusal,
    ensure_awaiting_upload,
    ensure_downloadable,
    ensure_readable,
    merge_list_filters,
    oversize_refusal,
    presigned_to_dto,
    stage_deleted,
    stage_upload_pending,
    stage_uploaded,
    upload_request_to_create_cmd,
    upload_too_large,
)

# ----------------------- #

_PART_TOUCH_ATTEMPTS: Final[int] = 8
"""Touches one ``presign_upload_part`` tries before it gives up: enough for a client asking
for that many part URLs of one file at once, each lost race being another call's win."""

# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class UploadStoredFile(Handler[UploadStoredFileRequestDTO, StoredFileRead]):
    """Create a pending stored-file row and stage an upload-pending event."""

    doc: DocumentCommandPort[
        StoredFileRead,
        StoredFileDocument,
        StoredFileCreateCmd,
        StoredFileUpdateCmd,
    ]
    outbox: OutboxCommandPort[StoredFileOutboxPayload] | None = attrs.field(default=None)
    max_bytes: int | None = attrs.field(default=None)
    """Largest upload accepted, in bytes; ``None`` sets no limit. A backstop checked on the
    already-buffered ``data``, not a memory guard."""

    # ....................... #

    async def __call__(self, args: UploadStoredFileRequestDTO) -> StoredFileRead:
        if self.max_bytes is not None and len(args.data) > self.max_bytes:
            raise upload_too_large(self.max_bytes)

        created = await self.doc.create(upload_request_to_create_cmd(args))

        if self.outbox is not None:
            await stage_upload_pending(self.outbox, created)

        return created


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class GetStoredFile(Handler[StoredFileIdDTO, StoredFileRead]):
    """Fetch stored-file metadata by id."""

    doc: DocumentQueryPort[StoredFileRead]

    # ....................... #

    async def __call__(self, args: StoredFileIdDTO) -> StoredFileRead:
        file = await self.doc.get(pk=args.id)
        ensure_readable(file)
        return file


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class ListStoredFiles(Handler[ListStoredFilesRequestDTO, Paginated[StoredFileRead]]):
    """List stored files with kit default filters."""

    doc: DocumentQueryPort[StoredFileRead]

    # ....................... #

    async def __call__(self, args: ListStoredFilesRequestDTO) -> Paginated[StoredFileRead]:
        filters = merge_list_filters(
            args.filters,
            prefix=args.prefix,
            include_deleted=args.include_deleted,
            include_pending=args.include_pending,
        )

        res = await self.doc.find_page(
            filters=filters,
            sorts=args.sorts,
            pagination=args.to_offset_expression(),
        )

        return Paginated.from_page(res)


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class DownloadStoredFile(Handler[StoredFileIdDTO, StoredFileDownloadDTO]):
    """Download blob bytes for a ready stored file."""

    doc: DocumentQueryPort[StoredFileRead]
    storage: StorageQueryPort

    # ....................... #

    async def __call__(self, args: StoredFileIdDTO) -> StoredFileDownloadDTO:
        file = await self.doc.get(pk=args.id)
        downloaded = await self.storage.download(ensure_downloadable(file))

        return StoredFileDownloadDTO(
            file=file,
            data=downloaded.data,
            content_type=downloaded.content_type,
            filename=downloaded.filename,
        )


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class DownloadStoredFileStream(Handler[StoredFileIdDTO, StoredFileStream]):
    """Open a bounded-memory download stream for a ready stored file.

    Same checks as :class:`DownloadStoredFile`. The stream's chunks are consumed by the
    transport after this operation returns; the storage client behind them is app-lifetime.
    The object is read in parts as it is consumed, so a file deleted mid-download fails the
    next read with ``not_found`` and the transport cuts the response short. A stored file's
    key is never reused, so a stream cannot mix two files.
    """

    doc: DocumentQueryPort[StoredFileRead]
    storage: StorageQueryPort

    # ....................... #

    async def __call__(self, args: StoredFileIdDTO) -> StoredFileStream:
        file = await self.doc.get(pk=args.id)
        streamed = await self.storage.download_stream(ensure_downloadable(file))

        return StoredFileStream(file=file, stream=attrs.evolve(streamed, filename=file.filename))


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class DownloadStoredFileRange(Handler[DownloadStoredFileRangeRequestDTO, StoredFileRangeDTO]):
    """Read an inclusive byte range of a ready stored file.

    Same checks as :class:`DownloadStoredFile`. A start past the end of the file is a
    precondition error (the HTTP 416 equivalent).
    """

    doc: DocumentQueryPort[StoredFileRead]
    storage: StorageQueryPort

    # ....................... #

    async def __call__(self, args: DownloadStoredFileRangeRequestDTO) -> StoredFileRangeDTO:
        file = await self.doc.get(pk=args.id)
        ranged = await self.storage.download_range(
            ensure_downloadable(file),
            start=args.start,
            end=args.end,
        )

        return StoredFileRangeDTO(
            file=file,
            data=ranged.data,
            content_type=ranged.content_type,
            content_range=ranged.content_range,
            total_size=ranged.total_size,
        )


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class PresignStoredFileDownload(
    Handler[PresignStoredFileDownloadRequestDTO, StoredFilePresignedDownloadDTO]
):
    """Mint a presigned download (GET) URL for a ready stored file (read grant).

    Same checks as :class:`DownloadStoredFile`, so no URL is minted for a row the caller
    cannot read or that is not ready. The URL saves the download under the stored filename.
    The registry resolves its storage as ``direct_transfer``, so a client-side-encrypting
    route refuses to build it.
    """

    doc: DocumentQueryPort[StoredFileRead]
    storage: StorageQueryPort

    # ....................... #

    async def __call__(
        self,
        args: PresignStoredFileDownloadRequestDTO,
    ) -> StoredFilePresignedDownloadDTO:
        file = await self.doc.get(pk=args.id)
        url = await self.storage.presign_download(
            ensure_downloadable(file),
            expires_in=args.expires_in,
            response_filename=file.filename,
        )

        # The URL is a bearer credential: it goes back to the client, never to a log.
        return StoredFilePresignedDownloadDTO(file=file, presigned=presigned_to_dto(url))


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class InitiateStoredFileUpload(
    Handler[InitiateStoredFileUploadRequestDTO, StoredFileUploadSessionDTO]
):
    """Create a pending row for a direct upload and hand back where its bytes go.

    The kit mints the storage key (``{tenant}/{file_id}``); the caller never names one. The
    URL or session is minted before the row is written, so a storage refusal leaves no row
    behind. (The registry resolves its storage as ``direct_transfer``, so a
    client-side-encrypting route refuses to build it at all.) Nothing is trusted yet: the
    declared size is checked against ``max_bytes`` here, and the object itself on completion.
    """

    doc: DocumentCommandPort[
        StoredFileRead,
        StoredFileDocument,
        StoredFileCreateCmd,
        StoredFileUpdateCmd,
    ]
    storage: StorageCommandPort
    uploads: StorageUploadSessionPort
    current_tenant: Callable[[], TenantIdentity | None]
    outbox: OutboxCommandPort[StoredFileOutboxPayload] | None = attrs.field(default=None)
    max_bytes: int | None = attrs.field(default=None)

    # ....................... #

    async def __call__(
        self,
        args: InitiateStoredFileUploadRequestDTO,
    ) -> StoredFileUploadSessionDTO:
        if self.max_bytes is not None and args.size > self.max_bytes:
            raise upload_too_large(self.max_bytes)

        file_id = uuid7()
        key = direct_upload_key(self.current_tenant(), file_id)
        presigned: PresignedUrlDTO | None = None
        upload_id: str | None = None

        if args.multipart:
            session = await self.uploads.begin_upload(key, content_type=args.content_type)
            upload_id = session.upload_id

        else:
            # Create-only: once an object holds the key, the URL cannot replace it, so the
            # bytes complete_upload checked stay the file's for as long as the URL lives.
            url = await self.storage.presign_upload(
                key,
                expires_in=args.expires_in,
                content_type=args.content_type,
                create_only=True,
            )
            presigned = presigned_to_dto(url)

        created = await self.doc.create(
            StoredFileCreateCmd(
                filename=args.filename,
                content_type=args.content_type,
                size=args.size,
                storage_key=key,
                prefix=args.prefix,
                description=args.description,
                tags=dict(args.tags) if args.tags is not None else None,
                status=StoredFileStatus.PENDING,
            ),
            id=file_id,
        )

        if self.outbox is not None:
            await stage_upload_pending(self.outbox, created)

        # A bearer write URL or a session id: back to the client, never to a log.
        return StoredFileUploadSessionDTO(file=created, presigned=presigned, upload_id=upload_id)


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class PresignStoredFileUploadPart(Handler[PresignStoredFileUploadPartRequestDTO, PresignedUrlDTO]):
    """Mint a presigned PUT for one part of a pending direct multipart upload.

    The session targets the key on the row, never one the caller names. The URL is signed
    first and handed out only once a revision-checked touch of the row succeeds, so it expires
    within its lifetime (at most 1 hour) of the row's last update, which the pending sweep
    counts its age from; a row the sweep failed in between hands out none. Parallel calls for
    one file race on the revision, so a lost touch re-reads the row and retries.
    ``complete_upload`` reloads the row, so the revision bump is invisible to it unless it runs
    concurrently.
    """

    doc: DocumentCommandPort[
        StoredFileRead,
        StoredFileDocument,
        StoredFileCreateCmd,
        StoredFileUpdateCmd,
    ]
    reads: DocumentQueryPort[StoredFileRead]
    uploads: StorageUploadSessionPort

    # ....................... #

    async def __call__(self, args: PresignStoredFileUploadPartRequestDTO) -> PresignedUrlDTO:
        file = await self.reads.get(pk=args.id)
        session = UploadSession(
            key=ensure_awaiting_upload(file),
            upload_id=args.upload_id,
            content_type=file.content_type,
        )
        url = await self.uploads.presign_part(
            session,
            args.part_number,
            expires_in=args.expires_in,
        )
        await self._touch(file)

        return presigned_to_dto(url)

    # ....................... #

    async def _touch(self, file: StoredFileRead) -> None:
        """Touch *file* at its revision, re-reading it after each lost race; a row that left
        ``pending`` meanwhile raises ``precondition``."""

        for attempt in range(1, _PART_TOUCH_ATTEMPTS + 1):
            try:
                await self.doc.touch(file.id, rev=file.rev, return_new=False)
                return

            except CoreException as error:
                if error.code != "revision_mismatch" or attempt == _PART_TOUCH_ATTEMPTS:
                    raise

            file = await self.reads.get(pk=file.id, skip_cache=True)
            ensure_awaiting_upload(file)


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class CompleteStoredFileUpload(Handler[CompleteStoredFileUploadRequestDTO, StoredFileRead]):
    """Check a direct upload's object against the row and mark the file ready.

    A multipart upload is bounded before it is assembled: the parts' stored sizes must fit
    ``max_bytes`` and the declared size, or the session is aborted. It is then assembled
    create-only, so it cannot replace an object already at the key. The object's head must
    then match the declared size and content type and fit ``max_bytes``.

    A refused upload marks the row ``failed`` first, with the revision check, and only then
    deletes the object (or aborts the session): a complete that lost the race to another,
    which already made the file ready, raises ``revision_mismatch`` and touches no bytes.
    Run it outside a transaction, as the wiring does, so the failed mark is not rolled back.
    An object not uploaded yet leaves the row pending.
    """

    doc: DocumentCommandPort[
        StoredFileRead,
        StoredFileDocument,
        StoredFileCreateCmd,
        StoredFileUpdateCmd,
    ]
    reads: DocumentQueryPort[StoredFileRead]
    storage: StorageQueryPort
    blobs: StorageCommandPort
    uploads: StorageUploadSessionPort
    outbox: OutboxCommandPort[StoredFileOutboxPayload] | None = attrs.field(default=None)
    search: SearchCommandPort[StoredFileRead] | None = attrs.field(default=None)
    max_bytes: int | None = attrs.field(default=None)

    # ....................... #

    async def __call__(self, args: CompleteStoredFileUploadRequestDTO) -> StoredFileRead:
        file = await self.reads.get(pk=args.id)
        key = ensure_awaiting_upload(file)

        if args.upload_id is not None and args.parts is not None:
            head = await self._assemble(file, key, args.upload_id, args.parts)

        else:
            head = await self._head(key)

        if refusal := direct_upload_refusal(file, head, self.max_bytes):
            await self._fail(file)
            await self.blobs.delete(key)
            raise refusal

        updated = await self.doc.update(
            pk=file.id,
            rev=file.rev,
            dto=StoredFileUpdateCmd(
                content_type=head.content_type,
                size=head.size,
                status=StoredFileStatus.READY,
            ),
        )

        if self.outbox is not None:
            await stage_uploaded(self.outbox, updated)

        if self.search is not None:
            await self.search.upsert([updated])

        return updated

    # ....................... #

    async def _fail(self, file: StoredFileRead) -> None:
        """Mark *file* failed at the revision this complete read; a lost race raises."""

        await self.doc.update(
            pk=file.id,
            rev=file.rev,
            dto=StoredFileUpdateCmd(status=StoredFileStatus.FAILED),
        )

    # ....................... #

    async def _assemble(
        self,
        file: StoredFileRead,
        key: str,
        upload_id: str,
        parts: Sequence[UploadPartDTO],
    ) -> ObjectHead:
        """Bound the named parts by their stored sizes, then assemble them create-only."""

        session = UploadSession(key=key, upload_id=upload_id, content_type=file.content_type)
        stored = {p.part_number: p.size for p in await self.uploads.list_parts(session)}
        total = sum(stored.get(p.part_number, 0) for p in parts)

        if refusal := oversize_refusal(file, total, self.max_bytes):
            await self._fail(file)
            await self.uploads.abort_upload(session)
            raise refusal

        return await self.uploads.complete_upload(
            session,
            [UploadPart(part_number=p.part_number, etag=p.etag, size=p.size) for p in parts],
            create_only=True,
        )

    # ....................... #

    async def _head(self, key: str) -> ObjectHead:
        """The single-PUT object's head; ``upload_incomplete`` when nothing landed yet."""

        try:
            return await self.storage.head(key)

        except CoreException as e:
            if e.kind is not ExceptionKind.NOT_FOUND:
                raise

            raise exc.precondition(
                "The stored file's bytes have not been uploaded yet",
                code="upload_incomplete",
            ) from e


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class SoftDeleteStoredFile(Handler[StoredFileIdRevDTO, StoredFileRead]):
    """Soft-delete a stored file and stage a deleted integration event."""

    doc: DocumentCommandPort[
        StoredFileRead,
        StoredFileDocument,
        StoredFileCreateCmd,
        StoredFileUpdateCmd,
    ]
    outbox: OutboxCommandPort[StoredFileOutboxPayload] | None = attrs.field(default=None)

    # ....................... #

    async def __call__(self, args: StoredFileIdRevDTO) -> StoredFileRead:
        updated = await self.doc.update(
            pk=args.id,
            rev=args.rev,
            dto=StoredFileUpdateCmd(status=StoredFileStatus.DELETED),
        )

        if self.outbox is not None:
            await stage_deleted(self.outbox, updated)

        return updated


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class SearchStoredFiles(Handler[SearchRequestDTO, SearchPaginated[StoredFileRead]]):
    """Search stored files by filename and description."""

    search: SearchQueryPort[StoredFileRead]

    # ....................... #

    async def __call__(self, args: SearchRequestDTO) -> SearchPaginated[StoredFileRead]:
        return await Search(search=self.search)(args)
