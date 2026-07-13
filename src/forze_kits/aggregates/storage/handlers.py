import attrs

from forze.application.contracts.execution import Handler
from forze.application.contracts.storage import (
    DownloadedObject,
    ObjectHead,
    PresignedUrl,
    RangedDownload,
    StorageCommandPort,
    StorageQueryPort,
    StorageUploadSessionPort,
    StoredObject,
    StreamedDownload,
    UploadedObject,
    UploadPart,
    UploadSession,
)

from .dto import (
    BeginUploadRequestDTO,
    CompleteUploadRequestDTO,
    ListedObjects,
    ListedPartsDTO,
    ListObjectsRequestDTO,
    ObjectHeadDTO,
    PresignDownloadRequestDTO,
    PresignedUrlDTO,
    PresignPartRequestDTO,
    PresignUploadRequestDTO,
    StoredObjectDTO,
    UploadObjectRequestDTO,
    UploadPartDTO,
    UploadSessionDTO,
    UploadSessionRequestDTO,
)

# ----------------------- #


def _stored_object_to_dto(obj: StoredObject) -> StoredObjectDTO:
    return StoredObjectDTO(
        key=obj.key,
        filename=obj.filename,
        created_at=obj.created_at,
        size=obj.size,
        content_type=obj.content_type,
        description=obj.description,
        tags=dict(obj.tags) if obj.tags is not None else None,
    )


# ....................... #


def _presigned_to_dto(url: PresignedUrl) -> PresignedUrlDTO:
    # The URL is a bearer credential: it MUST appear in the response body the
    # client needs, but it is never logged (the access-log middleware logs only
    # request path/status/duration, never the response body) and never placed
    # in a descriptor example. See PresignedUrlDTO's warning.
    return PresignedUrlDTO(
        url=url.url,
        method=url.method,
        expires_at=url.expires_at,
        headers=dict(url.headers),
    )


# ....................... #


def _session_to_dto(session: UploadSession) -> UploadSessionDTO:
    return UploadSessionDTO(
        key=session.key,
        upload_id=session.upload_id,
        bucket=session.bucket,
        content_type=session.content_type,
    )


# ....................... #


def _session_from_dto(dto: UploadSessionDTO) -> UploadSession:
    return UploadSession(
        key=dto.key,
        upload_id=dto.upload_id,
        # Never propagate a client-supplied bucket: the adapter always resolves
        # the tenant bucket from the route, so a forged bucket must not redirect
        # the operation to another bucket.
        bucket=None,
        content_type=dto.content_type,
    )


# ....................... #


def _part_to_dto(part: UploadPart) -> UploadPartDTO:
    return UploadPartDTO(
        part_number=part.part_number,
        etag=part.etag,
        size=part.size,
    )


# ....................... #


def _part_from_dto(dto: UploadPartDTO) -> UploadPart:
    return UploadPart(
        part_number=dto.part_number,
        etag=dto.etag,
        size=dto.size,
    )


# ....................... #


def _head_to_dto(head: ObjectHead) -> ObjectHeadDTO:
    return ObjectHeadDTO(
        content_type=head.content_type,
        size=head.size,
        etag=head.etag,
        last_modified=head.last_modified,
        metadata=dict(head.metadata),
        tags=dict(head.tags),
    )


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class DeleteObject(Handler[str, None]):
    """Handler that deletes an object from storage."""

    storage: StorageCommandPort
    """Storage port for object operations."""

    # ....................... #

    async def __call__(self, args: str) -> None:
        """Delete an object by storage key."""

        return await self.storage.delete(args)


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class DownloadObject(Handler[str, DownloadedObject]):
    """Handler that downloads an object from storage."""

    storage: StorageQueryPort
    """Storage port for object operations."""

    # ....................... #

    async def __call__(self, args: str) -> DownloadedObject:
        """Download an object by storage key."""

        return await self.storage.download(args)


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class HeadObject(Handler[str, ObjectHeadDTO]):
    """Handler that fetches an object's metadata (size / etag / … ) without its body."""

    storage: StorageQueryPort
    """Storage port for object operations."""

    # ....................... #

    async def __call__(self, args: str) -> ObjectHeadDTO:
        """Head an object by storage key."""

        return _head_to_dto(await self.storage.head(args))


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class DownloadObjectStream(Handler[str, StreamedDownload]):
    """Handler that opens a bounded-memory download stream for an object.

    Returns a :class:`StreamedDownload` whose ``chunks`` async iterator is consumed by the
    transport (e.g. a FastAPI ``StreamingResponse``) *after* this operation returns — the
    storage client backing it is app-lifetime, so the stream outlives the invocation.
    """

    storage: StorageQueryPort
    """Storage port for object operations."""

    # ....................... #

    async def __call__(self, args: str) -> StreamedDownload:
        """Open a streaming download for an object by storage key."""

        return await self.storage.download_stream(args)


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class DownloadRangeArgs:
    """Args for a ranged download — an HTTP ``Range``-derived byte window, not a client DTO.

    ``start``/``end`` are inclusive byte offsets (``end=None`` reads to EOF), matching
    :meth:`StorageQueryPort.download_range` and HTTP ``bytes=start-end`` semantics.
    """

    key: str
    """The storage key of the object."""

    start: int
    """Inclusive start byte offset."""

    end: int | None = None
    """Inclusive end byte offset, or ``None`` to read to EOF."""


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class DownloadObjectRange(Handler[DownloadRangeArgs, RangedDownload]):
    """Handler that fetches a byte range of an object via a backend-ranged read."""

    storage: StorageQueryPort
    """Storage port for object operations."""

    # ....................... #

    async def __call__(self, args: DownloadRangeArgs) -> RangedDownload:
        """Fetch the requested byte range of an object."""

        return await self.storage.download_range(args.key, start=args.start, end=args.end)


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class ListObjects(Handler[ListObjectsRequestDTO, ListedObjects]):
    """Handler that lists objects in storage."""

    storage: StorageQueryPort
    """Storage port for object operations."""

    # ....................... #

    async def __call__(self, args: ListObjectsRequestDTO) -> ListedObjects:
        """List objects for the requested page and optional prefix."""

        limit, offset = args.offset_limit

        hits, count = await self.storage.list(
            limit=limit,
            offset=offset,
            prefix=args.prefix,
        )

        return ListedObjects(
            hits=[_stored_object_to_dto(h) for h in hits],
            page=args.page,
            size=args.size,
            count=count,
        )


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class UploadObject(Handler[UploadObjectRequestDTO, StoredObjectDTO]):
    """Handler that uploads an object to storage."""

    storage: StorageCommandPort
    """Storage port for object operations."""

    # ....................... #

    async def __call__(self, args: UploadObjectRequestDTO) -> StoredObjectDTO:
        """Upload an object and return stored object metadata."""

        obj = UploadedObject(
            filename=args.filename,
            data=args.data,
            description=args.description,
            tags=args.tags,
            prefix=args.prefix,
        )

        stored = await self.storage.upload(obj)
        return _stored_object_to_dto(stored)


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class PresignDownload(Handler[PresignDownloadRequestDTO, PresignedUrlDTO]):
    """Handler minting a presigned download (GET) URL (read grant)."""

    storage: StorageQueryPort
    """Storage query port (presign download is a read grant)."""

    # ....................... #

    async def __call__(self, args: PresignDownloadRequestDTO) -> PresignedUrlDTO:
        """Mint a time-limited GET URL for the requested key."""

        url = await self.storage.presign_download(
            args.key,
            expires_in=args.expires_in,
        )
        return _presigned_to_dto(url)


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class PresignUpload(Handler[PresignUploadRequestDTO, PresignedUrlDTO]):
    """Handler minting a presigned upload (PUT) URL (write grant → command)."""

    storage: StorageCommandPort
    """Storage command port (minting an upload URL is a write grant)."""

    # ....................... #

    async def __call__(self, args: PresignUploadRequestDTO) -> PresignedUrlDTO:
        """Mint a time-limited PUT URL for the requested key."""

        url = await self.storage.presign_upload(
            args.key,
            expires_in=args.expires_in,
            content_type=args.content_type,
        )
        return _presigned_to_dto(url)


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class BeginUpload(Handler[BeginUploadRequestDTO, UploadSessionDTO]):
    """Handler opening a resumable multipart upload session (write → command)."""

    storage: StorageUploadSessionPort
    """Storage upload-session port (all multipart ops are writes)."""

    # ....................... #

    async def __call__(self, args: BeginUploadRequestDTO) -> UploadSessionDTO:
        """Open a session and return the round-trippable handle."""

        session = await self.storage.begin_upload(
            args.key,
            content_type=args.content_type,
        )
        return _session_to_dto(session)


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class PresignPart(Handler[PresignPartRequestDTO, PresignedUrlDTO]):
    """Handler minting a presigned URL for one multipart part (write → command)."""

    storage: StorageUploadSessionPort
    """Storage upload-session port (all multipart ops are writes)."""

    # ....................... #

    async def __call__(self, args: PresignPartRequestDTO) -> PresignedUrlDTO:
        """Mint a time-limited PUT URL for the requested part."""

        url = await self.storage.presign_part(
            _session_from_dto(args.session),
            args.part_number,
            expires_in=args.expires_in,
        )
        return _presigned_to_dto(url)


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class ListParts(Handler[UploadSessionRequestDTO, ListedPartsDTO]):
    """Handler listing the parts already uploaded for a session (resume primitive)."""

    storage: StorageUploadSessionPort
    """Storage upload-session port (acquired write-guarded with the session ops)."""

    # ....................... #

    async def __call__(self, args: UploadSessionRequestDTO) -> ListedPartsDTO:
        """List the already-uploaded parts of the reconstructed session."""

        parts = await self.storage.list_parts(_session_from_dto(args.session))
        return ListedPartsDTO(parts=[_part_to_dto(p) for p in parts])


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class CompleteUpload(Handler[CompleteUploadRequestDTO, ObjectHeadDTO]):
    """Handler assembling the uploaded parts into the final object (write → command)."""

    storage: StorageUploadSessionPort
    """Storage upload-session port (all multipart ops are writes)."""

    # ....................... #

    async def __call__(self, args: CompleteUploadRequestDTO) -> ObjectHeadDTO:
        """Complete the reconstructed session and return the object's head."""

        head = await self.storage.complete_upload(
            _session_from_dto(args.session),
            [_part_from_dto(p) for p in args.parts],
        )
        return _head_to_dto(head)


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class AbortUpload(Handler[UploadSessionRequestDTO, None]):
    """Handler discarding an unfinished multipart upload session (write → command)."""

    storage: StorageUploadSessionPort
    """Storage upload-session port (all multipart ops are writes)."""

    # ....................... #

    async def __call__(self, args: UploadSessionRequestDTO) -> None:
        """Abort the reconstructed session (best-effort idempotent)."""

        await self.storage.abort_upload(_session_from_dto(args.session))
