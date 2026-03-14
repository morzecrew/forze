import attrs

from forze.application.contracts.storage import DownloadedObject, StoragePort
from forze.application.execution import Usecase

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class DownloadObject(Usecase[str, DownloadedObject]):
    """Usecase that downloads an object from storage."""

    storage: StoragePort
    """Storage port for object operations."""

    # ....................... #

    async def main(self, args: str) -> DownloadedObject:
        """Download an object by storage key."""

        return await self.storage.download(args)
