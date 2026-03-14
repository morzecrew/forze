import attrs

from forze.application.contracts.storage import StoragePort
from forze.application.execution import Usecase

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class DeleteObject(Usecase[str, None]):
    """Usecase that deletes an object from storage."""

    storage: StoragePort
    """Storage port for object operations."""

    # ....................... #

    async def main(self, args: str) -> None:
        """Delete an object by storage key."""

        return await self.storage.delete(args)
