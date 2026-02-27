"""In-memory stub for StoragePort."""

from datetime import datetime
from typing import Optional, final

from forze.application.contracts._ports.storage import (
    DownloadedObject,
    StoragePort,
    StoredObject,
)
from forze.base.primitives import utcnow

# ----------------------- #


@final
class InMemoryStoragePort(StoragePort):
    """In-memory object storage for unit tests."""

    def __init__(self) -> None:
        self._objects: dict[str, tuple[bytes, str, str, Optional[str]]] = {}
        self._key_counter = 0

    def _next_key(self, prefix: Optional[str], filename: str) -> str:
        self._key_counter += 1
        base = f"{prefix or ''}{filename}"
        return f"{base}#{self._key_counter}"

    async def upload(
        self,
        filename: str,
        data: bytes,
        description: Optional[str] = None,
        *,
        prefix: Optional[str] = None,
    ) -> StoredObject:
        key = self._next_key(prefix, filename)
        content_type = "application/octet-stream"
        self._objects[key] = (data, filename, content_type, description)
        return StoredObject(
            key=key,
            filename=filename,
            description=description,
            content_type=content_type,
            size=len(data),
            created_at=utcnow(),
        )

    async def download(self, key: str) -> DownloadedObject:
        if key not in self._objects:
            raise KeyError(f"Object not found: {key}")
        data, filename, content_type, _ = self._objects[key]
        return DownloadedObject(
            data=data,
            content_type=content_type,
            filename=filename,
        )

    async def delete(self, key: str) -> None:
        self._objects.pop(key, None)

    async def list(
        self,
        limit: int,
        offset: int,
        *,
        prefix: Optional[str] = None,
    ) -> tuple[list[StoredObject], int]:
        matching = [k for k in self._objects if prefix is None or k.startswith(prefix)]
        total = len(matching)
        slice_keys = matching[offset : offset + limit]
        results: list[StoredObject] = []
        for k in slice_keys:
            data, filename, content_type, desc = self._objects[k]
            results.append(
                StoredObject(
                    key=k,
                    filename=filename,
                    description=desc,
                    content_type=content_type,
                    size=len(data),
                    created_at=datetime.fromisoformat("1970-01-01T00:00:00"),
                )
            )
        return results, total
