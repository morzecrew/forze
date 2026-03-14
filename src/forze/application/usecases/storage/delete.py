from typing import Optional

import attrs

from forze.application.contracts.storage import StoragePort
from forze.application.execution import Usecase
from forze.application.mapping import DTOMapper
from forze.domain.models import BaseDTO

# ----------------------- #


class DeleteObjectArgs(BaseDTO):
    """Arguments for object delete."""

    key: str
    """Storage key identifying the object to delete."""


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class DeleteObject[In: DeleteObjectArgs](Usecase[In, None]):
    """Usecase that deletes an object from storage."""

    storage: StoragePort
    """Storage port for object operations."""

    mapper: Optional[DTOMapper[In, DeleteObjectArgs]] = None
    """Optional mapper to transform incoming request DTO."""

    # ....................... #

    async def main(self, args: In) -> None:
        """Delete an object by storage key."""

        body = args

        if self.mapper:
            body = await self.mapper(self.ctx, body)  # type: ignore[assignment]

        return await self.storage.delete(body.key)
