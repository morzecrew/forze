import attrs

from forze.application.contracts.document import DocumentCommandPort, DocumentQueryPort
from forze.application.contracts.execution import Handler
from forze.application.contracts.outbox import OutboxCommandPort
from forze.application.contracts.search import SearchQueryPort
from forze.application.contracts.storage import StorageQueryPort
from forze.base.exceptions import exc
from forze_kits.aggregates.search.dto import (
    SearchPaginated,
    SearchRequestDTO,
)
from forze_kits.aggregates.search.handlers import Search
from forze_kits.aggregates.storage.dto import PresignedUrlDTO
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
    ListStoredFilesRequestDTO,
    PresignStoredFileDownloadRequestDTO,
    StoredFileDownloadDTO,
    StoredFileIdDTO,
    StoredFileIdRevDTO,
    StoredFilePresignedDownloadDTO,
    StoredFileStream,
    UploadStoredFileRequestDTO,
)
from ._helpers import (
    ensure_downloadable,
    ensure_readable,
    merge_list_filters,
    stage_deleted,
    stage_upload_pending,
    upload_request_to_create_cmd,
)

# ----------------------- #


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
            raise exc.validation(
                f"Uploaded file exceeds the maximum allowed size of {self.max_bytes} bytes",
                code="upload_too_large",
                details={"max_bytes": self.max_bytes},
            )

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
class PresignStoredFileDownload(
    Handler[PresignStoredFileDownloadRequestDTO, StoredFilePresignedDownloadDTO]
):
    """Mint a presigned download (GET) URL for a ready stored file (read grant).

    Same checks as :class:`DownloadStoredFile`, so no URL is minted for a row the caller
    cannot read or that is not ready. The URL saves the download under the stored filename.
    The storage adapter refuses on a client-side-encrypting route.
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
        return StoredFilePresignedDownloadDTO(
            file=file,
            presigned=PresignedUrlDTO(
                url=url.url,
                method=url.method,
                expires_at=url.expires_at,
                headers=dict(url.headers),
            ),
        )


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
