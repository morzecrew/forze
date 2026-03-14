import attrs

from forze.application.contracts.storage import StoragePort, StoredObject
from forze.application.dto import UploadObjectRequestDTO
from forze.application.execution import Usecase

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class UploadObject(Usecase[UploadObjectRequestDTO, StoredObject]):
    """Usecase that uploads an object to storage."""

    storage: StoragePort
    """Storage port for object operations."""

    # ....................... #

    async def main(self, args: UploadObjectRequestDTO) -> StoredObject:
        """Upload an object and return stored object metadata."""

        return await self.storage.upload(
            filename=args.filename,
            data=args.data,
            description=args.description,
            prefix=args.prefix,
        )
