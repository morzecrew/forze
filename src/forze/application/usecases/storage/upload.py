from typing import Optional

import attrs

from forze.application.contracts.storage import StoragePort, StoredObject
from forze.application.execution import Usecase
from forze.application.mapping import DTOMapper
from forze.domain.models import BaseDTO

# ----------------------- #


class UploadObjectArgs(BaseDTO):
    """Arguments for object upload."""

    filename: str
    """Original filename for the object."""

    data: bytes
    """Raw bytes payload to store."""

    description: Optional[str] = None
    """Optional human-readable description."""

    prefix: Optional[str] = None
    """Optional key prefix (folder-like namespace)."""


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class UploadObject[In: UploadObjectArgs](Usecase[In, StoredObject]):
    """Usecase that uploads an object to storage."""

    storage: StoragePort
    """Storage port for object operations."""

    mapper: Optional[DTOMapper[In, UploadObjectArgs]] = None
    """Optional mapper to transform incoming request DTO."""

    # ....................... #

    async def main(self, args: In) -> StoredObject:
        """Upload an object and return stored object metadata."""

        body = args

        if self.mapper:
            body = await self.mapper(self.ctx, body)  # type: ignore[assignment]

        return await self.storage.upload(
            filename=body.filename,
            data=body.data,
            description=body.description,
            prefix=body.prefix,
        )
