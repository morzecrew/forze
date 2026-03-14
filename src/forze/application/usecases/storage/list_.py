from typing import Optional

import attrs

from forze.application.contracts.storage import StoragePort, StoredObject
from forze.application.execution import Usecase
from forze.application.mapping import DTOMapper
from forze.domain.models import BaseDTO

# ----------------------- #


class ListObjectsArgs(BaseDTO):
    """Arguments for object listing with pagination."""

    page: int = 1
    """One-based page number."""

    size: int = 10
    """Page size (number of records per page)."""

    prefix: Optional[str] = None
    """Optional key prefix filter."""


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
class ListObjects[In: ListObjectsArgs](Usecase[In, ListedObjects]):
    """Usecase that lists objects in storage."""

    storage: StoragePort
    """Storage port for object operations."""

    mapper: Optional[DTOMapper[In, ListObjectsArgs]] = None
    """Optional mapper to transform incoming request DTO."""

    # ....................... #

    async def main(self, args: In) -> ListedObjects:
        """List objects for the requested page and optional prefix."""

        body = args

        if self.mapper:
            body = await self.mapper(self.ctx, body)  # type: ignore[assignment]

        page = body.page
        size = body.size
        limit = size
        offset = (page - 1) * limit

        hits, count = await self.storage.list(
            limit=limit,
            offset=offset,
            prefix=body.prefix,
        )

        return ListedObjects(hits=hits, page=page, size=size, count=count)
