from datetime import datetime
from typing import NotRequired, Optional, Protocol, TypedDict, runtime_checkable

# ----------------------- #


class StoredObject(TypedDict):
    key: str
    filename: str
    description: Optional[str]
    content_type: str
    size: int
    created_at: datetime


class ObjectMetadata(TypedDict):
    filename: str
    created_at: str
    size: str
    description: NotRequired[str]


class DownloadedObject(TypedDict):
    data: bytes
    content_type: str
    filename: str


# ....................... #


@runtime_checkable
class StoragePort(Protocol):
    async def upload(
        self,
        filename: str,
        data: bytes,
        description: Optional[str] = None,
        *,
        prefix: Optional[str] = None
    ) -> StoredObject: ...

    async def download(self, key: str) -> DownloadedObject: ...
    async def delete(self, key: str) -> None: ...
    async def list(
        self,
        limit: int,
        offset: int,
        *,
        prefix: Optional[str] = None,
    ) -> tuple[list[StoredObject], int]: ...
