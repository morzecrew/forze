import attrs

from forze.application.contracts.execution import Handler
from forze.application.contracts.storage import (
    DownloadedObject,
    StorageCommandPort,
    StorageQueryPort,
    StoredObject,
    UploadedObject,
)

from .dto import (
    ListedObjects,
    ListObjectsRequestDTO,
    StoredObjectDTO,
    UploadObjectRequestDTO,
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
