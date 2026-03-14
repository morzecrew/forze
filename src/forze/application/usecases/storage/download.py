from typing import Optional

import attrs

from forze.application.contracts.storage import DownloadedObject, StoragePort
from forze.application.execution import Usecase
from forze.application.mapping import DTOMapper
from forze.domain.models import BaseDTO

# ----------------------- #


class DownloadObjectArgs(BaseDTO):
    """Arguments for object download."""

    key: str
    """Storage key identifying the object to download."""


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class DownloadObject[In: DownloadObjectArgs](Usecase[In, DownloadedObject]):
    """Usecase that downloads an object from storage."""

    storage: StoragePort
    """Storage port for object operations."""

    mapper: Optional[DTOMapper[In, DownloadObjectArgs]] = None
    """Optional mapper to transform incoming request DTO."""

    # ....................... #

    async def main(self, args: In) -> DownloadedObject:
        """Download an object by storage key."""

        body = args

        if self.mapper:
            body = await self.mapper(self.ctx, body)  # type: ignore[assignment]

        return await self.storage.download(body.key)
