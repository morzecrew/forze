import attrs

from forze.application.contracts.storage import StoragePort, StoredObject
from forze.application.dto import ListObjectsRequestDTO
from forze.application.execution import Usecase
from forze.domain.models import BaseDTO

# ----------------------- #


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
class ListObjects(Usecase[ListObjectsRequestDTO, ListedObjects]):
    """Usecase that lists objects in storage."""

    storage: StoragePort
    """Storage port for object operations."""

    # ....................... #

    async def main(self, args: ListObjectsRequestDTO) -> ListedObjects:
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
