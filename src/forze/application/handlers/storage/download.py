import attrs

from forze.application.contracts.storage import DownloadedObject, StoragePort
from forze.application.execution.core import Handler

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class DownloadObject(Handler[str, DownloadedObject]):
    """Handler that downloads an object from storage."""

    storage: StoragePort
    """Storage port for object operations."""

    # ....................... #

    async def __call__(self, args: str) -> DownloadedObject:
        """Download an object by storage key."""

        return await self.storage.download(args)
