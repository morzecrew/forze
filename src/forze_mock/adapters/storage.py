"""In-memory object storage adapter."""

from __future__ import annotations

import mimetypes
from datetime import timedelta
from typing import (
    Literal,
    final,
)
import attrs
from forze.application.contracts.storage import (
    DownloadedObject,
    PresignedUrl,
    StorageCommandPort,
    StorageQueryPort,
    StoredObject,
    UploadedObject,
)
from forze.application.integrations.storage.client import presign_expiry_seconds
from forze.base.exceptions import exc
from forze.base.primitives import utcnow, uuid7
from forze_mock.state import MockState
from forze_mock.tenancy import MockTenancyMixin, partition_namespace

@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MockStorageAdapter(MockTenancyMixin, StorageQueryPort, StorageCommandPort):
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
            tags=dict(obj.tags) if obj.tags else None,
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

    async def presign_download(
        self,
        key: str,
        *,
        expires_in: timedelta,
    ) -> PresignedUrl:
        """Issue a deterministic fake download URL and record it on the state.

        Mirrors the real backends: signing is local, existence is not
        checked, and the 7-day S3/GCS expiry cap is enforced. ``expires_at``
        derives from the ambient
        :class:`~forze.base.primitives.TimeSource`, so frozen time makes the
        URL fully deterministic.
        """

        return self.__presign(key, expires_in=expires_in, method="GET")

    # ....................... #

    async def presign_upload(
        self,
        key: str,
        *,
        expires_in: timedelta,
        content_type: str | None = None,
    ) -> PresignedUrl:
        """Issue a deterministic fake upload URL and record it on the state.

        Mirrors the real backends: the 7-day S3/GCS expiry cap is enforced
        and a bound *content_type* is echoed in
        :attr:`PresignedUrl.headers`. ``expires_at`` derives from the ambient
        :class:`~forze.base.primitives.TimeSource`, so frozen time makes the
        URL fully deterministic.
        """

        return self.__presign(
            key,
            expires_in=expires_in,
            method="PUT",
            content_type=content_type,
        )

    # ....................... #

    def __presign(
        self,
        key: str,
        *,
        expires_in: timedelta,
        method: Literal["GET", "PUT"],
        content_type: str | None = None,
    ) -> PresignedUrl:
        seconds = presign_expiry_seconds(expires_in)
        bucket = self._bucket()

        expires_at = utcnow() + timedelta(seconds=seconds)
        op = "get" if method == "GET" else "put"
        url = f"mock://{bucket}/{key}?op={op}&expires={expires_at.isoformat()}"

        headers: dict[str, str] = {}

        if content_type is not None:
            headers["Content-Type"] = content_type

        with self.state.lock:
            self.state.storage_presigns.append(
                {
                    "bucket": bucket,
                    "key": key,
                    "method": method,
                    "expires_at": expires_at,
                    "content_type": content_type,
                }
            )

        return PresignedUrl(
            url=url,
            method=method,
            expires_at=expires_at,
            headers=headers,
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
        include_tags: bool = False,
    ) -> tuple[list[StoredObject], int]:
        """List stored objects with pagination.

        ``include_tags`` is accepted for port compatibility but adds nothing
        here: the mock stores tags in-memory and always includes them, so
        the guarantee is already satisfied (no extra work either way).
        """

        _ = include_tags  # tags are always included for free in the mock

        with self.state.lock:
            rows = list(self._objects().values())
        if prefix:
            rows = [row for row in rows if row.key.startswith(prefix)]
        total = len(rows)
        return rows[offset : offset + limit], total
