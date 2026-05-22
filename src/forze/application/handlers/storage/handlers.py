import attrs

from forze.application.contracts.execution import Handler
from forze.application.contracts.storage import (
    DownloadedObject,
    StoragePort,
    StoredObject,
    UploadedObject,
)
from forze.domain.models import BaseDTO

from .dto import ListObjectsRequestDTO, StoredObjectDTO, UploadObjectRequestDTO


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

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class DeleteObject(Handler[str, None]):
    """Handler that deletes an object from storage."""

    storage: StoragePort
    """Storage port for object operations."""

    # ....................... #

    async def __call__(self, args: str) -> None:
        """Delete an object by storage key."""

        return await self.storage.delete(args)


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class DownloadObject(Handler[str, DownloadedObject]):
    """Handler that downloads an object from storage."""

    storage: StoragePort
    """Storage port for object operations."""

    # ....................... #

    async def __call__(self, args: str) -> DownloadedObject:
        """Download an object by storage key."""

        return await self.storage.download(args)


# ....................... #


class ListedObjects(BaseDTO):
    """Paginated listing response for storage objects."""

    hits: list[StoredObjectDTO]
    """Objects for the current page."""

    page: int
    """One-based page number."""

    size: int
    """Page size (number of records per page)."""

    count: int
    """Total number of matching objects."""


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class ListObjects(Handler[ListObjectsRequestDTO, ListedObjects]):
    """Handler that lists objects in storage."""

    storage: StoragePort
    """Storage port for object operations."""

    # ....................... #

    async def __call__(self, args: ListObjectsRequestDTO) -> ListedObjects:
        """List objects for the requested page and optional prefix."""

        page = args.page
        size = args.size
        limit = size
        offset = (page - 1) * limit

        hits, count = await self.storage.list(
            limit=limit,
            offset=offset,
            prefix=args.prefix,
        )

        return ListedObjects(
            hits=[_stored_object_to_dto(h) for h in hits],
            page=page,
            size=size,
            count=count,
        )


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class UploadObject(Handler[UploadObjectRequestDTO, StoredObjectDTO]):
    """Handler that uploads an object to storage."""

    storage: StoragePort
    """Storage port for object operations."""

    # ....................... #

    async def __call__(self, args: UploadObjectRequestDTO) -> StoredObjectDTO:
        """Upload an object and return stored object metadata."""

        obj = UploadedObject(
            filename=args.filename,
            data=args.data,
            description=args.description,
            prefix=args.prefix,
        )

        stored = await self.storage.upload(obj)
        return _stored_object_to_dto(stored)
