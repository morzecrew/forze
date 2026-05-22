import attrs

from forze.application.contracts.execution import Handler
from forze.application.contracts.storage import (
    DownloadedObject,
    StoragePort,
    StoredObject,
    UploadedObject,
)
from forze.domain.models import BaseDTO

from .dto import ListObjectsRequestDTO, UploadObjectRequestDTO

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

    hits: list[StoredObject]
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

        return ListedObjects(hits=hits, page=page, size=size, count=count)


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class UploadObject(Handler[UploadObjectRequestDTO, StoredObject]):
    """Handler that uploads an object to storage."""

    storage: StoragePort
    """Storage port for object operations."""

    # ....................... #

    async def __call__(self, args: UploadObjectRequestDTO) -> StoredObject:
        """Upload an object and return stored object metadata."""

        obj = UploadedObject(
            filename=args.filename,
            data=args.data,
            description=args.description,
            prefix=args.prefix,
        )

        return await self.storage.upload(obj)
