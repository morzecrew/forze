"""In-memory object storage adapter."""

from __future__ import annotations

import mimetypes
from typing import (
    final,
)
import attrs
from forze.application.contracts.storage import (
    DownloadedObject,
    StoragePort,
    StoredObject,
    UploadedObject,
)
from forze.base.exceptions import exc
from forze.base.primitives import utcnow, uuid7
from forze_mock.state import MockState
from forze_mock.tenancy import MockTenancyMixin, partition_namespace

@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MockStorageAdapter(MockTenancyMixin, StoragePort):
    """In-memory object storage adapter."""

    state: MockState
    bucket: str

    # ....................... #

    def _bucket(self) -> str:
        return partition_namespace(self.require_tenant_if_aware(), self.bucket)

    def _objects(self) -> dict[str, StoredObject]:
        return self.state.storage.setdefault(self._bucket(), {})

    # ....................... #

    def _payloads(self) -> dict[str, bytes]:
        return self.state.storage_bytes.setdefault(self._bucket(), {})

    # ....................... #

    async def upload(self, obj: UploadedObject) -> StoredObject:
        filename = obj.filename
        data = obj.data
        prefix = obj.prefix
        description = obj.description
        key = f"{prefix.strip('/') + '/' if prefix else ''}{uuid7()}"
        content_type = mimetypes.guess_type(filename)[0] or "application/octet-stream"
        stored = StoredObject(
            key=key,
            filename=filename,
            description=description,
            content_type=content_type,
            size=len(data),
            created_at=utcnow(),
        )
        with self.state.lock:
            self._objects()[key] = stored
            self._payloads()[key] = bytes(data)
        return stored

    # ....................... #

    async def download(self, key: str) -> DownloadedObject:
        with self.state.lock:
            if key not in self._objects() or key not in self._payloads():
                raise exc.not_found(f"Object not found: {key}")

            obj = self._objects()[key]
            payload = self._payloads()[key]

        return DownloadedObject(
            data=payload,
            content_type=obj.content_type,
            filename=obj.filename,
        )

    # ....................... #

    async def delete(self, key: str) -> None:
        with self.state.lock:
            self._objects().pop(key, None)
            self._payloads().pop(key, None)

    # ....................... #

    async def list(
        self,
        limit: int,
        offset: int,
        *,
        prefix: str | None = None,
    ) -> tuple[list[StoredObject], int]:
        with self.state.lock:
            rows = list(self._objects().values())
        if prefix:
            rows = [row for row in rows if row.key.startswith(prefix)]
        total = len(rows)
        return rows[offset : offset + limit], total
