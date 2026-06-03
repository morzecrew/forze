"""Shared helpers for stored-file handlers and integration stages."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any
from uuid import UUID

from forze.application.contracts.outbox import OutboxCommandPort
from forze.application.contracts.querying import QueryFilterExpression
from forze.application.contracts.search import SearchCommandPort
from forze.application.contracts.storage import UploadedObject
from forze.base.exceptions import exc
from forze_kits.domain.stored_file import (
    StoredFileCreateCmd,
    StoredFileEventType,
    StoredFileKitSpec,
    StoredFileOutboxPayload,
    StoredFileRead,
    StoredFileStatus,
    StoredFileUpdateCmd,
)

from .dto import UploadStoredFileRequestDTO

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

    if not include_deleted:
        parts.append({"$values": {"status": {"$neq": StoredFileStatus.DELETED.value}}})

    if not include_pending:
        parts.append({"$values": {"status": StoredFileStatus.READY.value}})

    if prefix is not None:
        parts.append({"$values": {"prefix": prefix}})

    if args_filters is not None:
        parts.append(args_filters)

    if not parts:
        return None

    if len(parts) == 1:
        return parts[0]

    return {"$and": parts}


# ....................... #


def ensure_readable(file: StoredFileRead) -> None:
    """Reject soft-deleted stored files."""

    if file.status == StoredFileStatus.DELETED:
        raise exc.not_found("Stored file was deleted")


# ....................... #


def ensure_downloadable(file: StoredFileRead) -> None:
    """Reject stored files that are not ready for download."""

    ensure_readable(file)

    if file.status != StoredFileStatus.READY:
        raise exc.precondition(
            f"Stored file is not ready for download (status={file.status!s})"
        )

    if not file.storage_key:
        raise exc.internal("Ready stored file is missing storage_key")


# ....................... #


def upload_request_to_create_cmd(args: UploadStoredFileRequestDTO) -> StoredFileCreateCmd:
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
    ctx: ExecutionContext,
    args: UploadStoredFileRequestDTO,
    pending: StoredFileRead,
    outbox: OutboxCommandPort[Any] | None = None,
    search: SearchCommandPort[StoredFileRead] | None = None,
) -> StoredFileRead:
    """Upload blob bytes and transition a pending row to ``ready``."""

    doc = ctx.doc.command(kit.document)
    storage = ctx.storage(kit.resolved_storage)

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
    ctx: ExecutionContext,
    file_id: UUID,
    storage_key: str | None,
    search: SearchCommandPort[StoredFileRead] | None = None,
) -> None:
    """Delete blob and search index entry after soft delete."""

    if storage_key:
        storage = ctx.storage(kit.resolved_storage)
        await storage.delete(storage_key)

    if search is not None:
        await search.delete([str(file_id)])
