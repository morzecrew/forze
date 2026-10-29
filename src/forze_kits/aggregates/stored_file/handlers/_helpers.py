"""Shared helpers for stored-file handlers and integration stages."""

from typing import TYPE_CHECKING, Any
from uuid import UUID

from forze.application.contracts.outbox import OutboxCommandPort
from forze.application.contracts.querying import QueryFilterExpression
from forze.application.contracts.search import SearchCommandPort
from forze.application.contracts.storage import (
    ObjectHead,
    PresignedUrl,
    UploadedObject,
    tenant_key_prefix,
)
from forze.application.contracts.tenancy import TenantIdentity
from forze.base.exceptions import CoreException, exc
from forze_kits.aggregates.storage.dto import PresignedUrlDTO
from forze_kits.domain.stored_file import (
    StoredFileCreateCmd,
    StoredFileEventType,
    StoredFileKitSpec,
    StoredFileOutboxPayload,
    StoredFileRead,
    StoredFileStatus,
    StoredFileUpdateCmd,
)

from ..dto import UploadStoredFileRequestDTO

if TYPE_CHECKING:
    from forze.application.execution.context import ExecutionContext

# ----------------------- #


def merge_list_filters(
    args_filters: QueryFilterExpression | None,
    *,
    prefix: str | None,
    include_deleted: bool,
    include_pending: bool,
) -> QueryFilterExpression | None:
    """Build list filters excluding deleted/pending rows unless requested."""

    parts: list[QueryFilterExpression] = []

    allowed = [
        s.value
        for s in StoredFileStatus
        if (include_deleted or s is not StoredFileStatus.DELETED)
        and (include_pending or s not in (StoredFileStatus.PENDING, StoredFileStatus.FAILED))
    ]
    if len(allowed) < len(StoredFileStatus):
        parts.append({"$values": {"status": {"$in": allowed}}})

    if prefix is not None:
        parts.append({"$values": {"prefix": prefix}})

    if args_filters is not None:
        parts.append(args_filters)

    if not parts:
        return None

    return parts[0] if len(parts) == 1 else {"$and": parts}


# ....................... #


def ensure_readable(file: StoredFileRead) -> None:
    """Reject soft-deleted stored files."""

    if file.status == StoredFileStatus.DELETED:
        raise exc.not_found("Stored file was deleted")


# ....................... #


def ensure_downloadable(file: StoredFileRead) -> str:
    """Reject stored files that are not ready for download.

    :returns: The ready file's storage key.
    """

    ensure_readable(file)

    if file.status != StoredFileStatus.READY:
        raise exc.precondition(f"Stored file is not ready for download (status={file.status!s})")

    if not file.storage_key:
        raise exc.internal("Ready stored file is missing storage_key")

    return file.storage_key


# ....................... #


def upload_too_large(max_bytes: int) -> CoreException:
    """The refusal of an upload larger than the kit's ``max_bytes``."""

    return exc.validation(
        f"Uploaded file exceeds the maximum allowed size of {max_bytes} bytes",
        code="upload_too_large",
        details={"max_bytes": max_bytes},
    )


# ....................... #


def presigned_to_dto(url: PresignedUrl) -> PresignedUrlDTO:
    """A presigned URL as the client receives it. A bearer credential: never log it."""

    return PresignedUrlDTO(
        url=url.url,
        method=url.method,
        expires_at=url.expires_at,
        headers=dict(url.headers),
    )


# ....................... #


def direct_upload_key(tenant: TenantIdentity | None, file_id: UUID) -> str:
    """The storage key the kit mints for a direct upload: ``{tenant}/{file_id}``.

    Never client-chosen. The tenant segment is the one a tenant-aware storage route requires,
    and the file id is never reused, so the key names exactly one stored file.
    """

    if tenant is None:
        return str(file_id)

    return f"{tenant_key_prefix(tenant.tenant_id)}/{file_id}"


# ....................... #


def ensure_awaiting_upload(file: StoredFileRead) -> str:
    """Reject a stored file that is not a pending direct upload.

    :returns: The key the kit minted for its bytes.
    """

    ensure_readable(file)

    # A buffered upload's pending row has no key yet: only initiate_upload sets one up front.
    if file.status != StoredFileStatus.PENDING or not file.storage_key:
        raise exc.precondition(
            f"Stored file is not awaiting a direct upload (status={file.status!s})",
        )

    return file.storage_key


# ....................... #


def _upload_mismatch(file: StoredFileRead) -> CoreException:
    return exc.validation(
        "Uploaded object does not match the declared size and content type",
        code="upload_mismatch",
        details={"size": file.size, "content_type": file.content_type},
    )


# ....................... #


def oversize_refusal(
    file: StoredFileRead,
    size: int,
    max_bytes: int | None,
) -> CoreException | None:
    """Why *size* bytes cannot become *file*'s, or ``None``: over the cap or the declared size."""

    if max_bytes is not None and size > max_bytes:
        return upload_too_large(max_bytes)

    if size > file.size:
        return _upload_mismatch(file)

    return None


# ....................... #


def direct_upload_refusal(
    file: StoredFileRead,
    head: ObjectHead,
    max_bytes: int | None,
) -> CoreException | None:
    """Why the uploaded object must not become *file*'s bytes, or ``None`` when it may.

    The client chose what it sent, so the object is checked against the kit's cap and against
    the size and content type the row was initiated with.
    """

    if refusal := oversize_refusal(file, head.size, max_bytes):
        return refusal

    if head.size != file.size or head.content_type != file.content_type:
        return _upload_mismatch(file)

    return None


# ....................... #


def upload_request_to_create_cmd(
    args: UploadStoredFileRequestDTO,
) -> StoredFileCreateCmd:
    """Map an upload request to a pending create command."""

    return StoredFileCreateCmd(
        filename=args.filename,
        size=len(args.data),
        prefix=args.prefix,
        description=args.description,
        tags=dict(args.tags) if args.tags is not None else None,
        status=StoredFileStatus.PENDING,
    )


# ....................... #


async def stage_upload_pending(
    outbox: OutboxCommandPort[Any],
    file: StoredFileRead,
) -> None:
    """Stage ``upload_pending`` integration event."""

    await outbox.stage(
        StoredFileEventType.UPLOAD_PENDING,
        StoredFileOutboxPayload.from_read(file),
    )


# ....................... #


async def stage_uploaded(
    outbox: OutboxCommandPort[Any],
    file: StoredFileRead,
) -> None:
    """Stage ``uploaded`` integration event and flush immediately."""

    await outbox.stage(
        StoredFileEventType.UPLOADED,
        StoredFileOutboxPayload.from_read(file),
    )
    await outbox.flush()


# ....................... #


async def stage_deleted(
    outbox: OutboxCommandPort[Any],
    file: StoredFileRead,
) -> None:
    """Stage ``deleted`` integration event."""

    await outbox.stage(
        StoredFileEventType.DELETED,
        StoredFileOutboxPayload.from_read(file),
    )


# ....................... #


async def complete_stored_file_upload(
    *,
    kit: StoredFileKitSpec,
    ctx: "ExecutionContext",
    args: UploadStoredFileRequestDTO,
    pending: StoredFileRead,
    outbox: OutboxCommandPort[Any] | None = None,
    search: SearchCommandPort[StoredFileRead] | None = None,
) -> StoredFileRead:
    """Upload blob bytes and transition a pending row to ``ready``."""

    doc = ctx.doc.command(kit.document)
    storage = ctx.storage.command(kit.resolved_storage)

    try:
        stored = await storage.upload(
            UploadedObject(
                filename=args.filename,
                data=args.data,
                description=args.description,
                tags=args.tags,
                prefix=args.prefix,
            )
        )

        updated = await doc.update(
            pk=pending.id,
            rev=pending.rev,
            dto=StoredFileUpdateCmd(
                storage_key=stored.key,
                content_type=stored.content_type,
                size=stored.size,
                status=StoredFileStatus.READY,
            ),
        )

    except Exception:
        await doc.update(
            pk=pending.id,
            rev=pending.rev,
            dto=StoredFileUpdateCmd(status=StoredFileStatus.FAILED),
        )
        raise

    if outbox is not None:
        await stage_uploaded(outbox, updated)

    if search is not None:
        await search.upsert([updated])

    return updated


# ....................... #


async def purge_stored_file_blob(
    *,
    kit: StoredFileKitSpec,
    ctx: "ExecutionContext",
    file_id: UUID,
    storage_key: str | None,
    search: SearchCommandPort[StoredFileRead] | None = None,
) -> None:
    """Delete blob and search index entry after soft delete."""

    if storage_key:
        storage = ctx.storage.command(kit.resolved_storage)
        await storage.delete(storage_key)

    if search is not None:
        await search.delete([str(file_id)])
