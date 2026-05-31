from forze.base.primitives import JsonDict
from forze_gcs._compat import require_gcs

require_gcs()

# ....................... #

import json
import os
from contextlib import asynccontextmanager
from contextvars import ContextVar
from datetime import datetime
from typing import Any, AsyncGenerator, cast, final

import aiohttp
import attrs
from gcloud.aio.storage import Storage

from forze.base.exceptions import exc
from forze.base.primitives.gcp_service_file import release_service_file

from .errors import exc_interceptor
from .port import GCSClientPort
from .value_objects import DEFAULT_TIMEOUT, GCSConfig, GCSHead, GCSListedObject

# ----------------------- #


@final
@attrs.define(slots=True)
class GCSClient(GCSClientPort):
    """Async GCS client backed by :class:`gcloud.aio.storage.Storage`.

    Must be :meth:`initialize`d with a project id before use. The :meth:`client`
    context manager yields the shared storage client for nested adapter
    operations (depth tracking only; teardown happens in :meth:`close`).
    """

    __storage: Storage | None = attrs.field(default=None, init=False)
    __project_id: str | None = attrs.field(default=None, init=False)
    __config: GCSConfig | None = attrs.field(default=None, init=False)
    __service_file: str | None = attrs.field(default=None, init=False)
    __service_file_owned: bool = attrs.field(default=False, init=False)

    __ctx_depth: ContextVar[int] = attrs.field(
        factory=lambda: ContextVar("gcs_depth", default=0),
        init=False,
    )

    # ....................... #

    async def initialize(
        self,
        project_id: str,
        *,
        service_file: str | None = None,
        service_file_owned: bool = False,
        config: GCSConfig | None = None,
    ) -> None:
        """Configure the client with project id and shared storage client.

        :param project_id: GCP project id used for bucket operations.
        :param service_file: Optional path to a service account JSON key file.
        :param config: Optional client configuration overrides.
        """

        if self.__storage is not None:
            return

        self.__project_id = project_id
        self.__config = config

        api_root: str | None = None
        if host := os.environ.get("STORAGE_EMULATOR_HOST"):
            api_root = host.rstrip("/")

        key_file = service_file

        if key_file is None and config is not None:
            key_file = config.service_file

        self.__service_file = key_file
        self.__service_file_owned = service_file_owned

        self.__storage = Storage(
            service_file=key_file,
            api_root=api_root,
        )

    # ....................... #

    async def close(self) -> None:
        """Release the underlying storage client and HTTP session."""

        storage = self.__storage

        if storage is not None:
            await storage.close()
            self.__storage = None

        release_service_file(
            self.__service_file,
            owned=self.__service_file_owned,
        )
        self.__service_file = None
        self.__service_file_owned = False
        self.__project_id = None
        self.__config = None

    # ....................... #

    def __require_storage(self) -> Storage:
        if self.__storage is None:
            raise exc.internal("GCS client is not initialized")

        return self.__storage

    def __require_project_id(self) -> str:
        if self.__project_id is None:
            raise exc.internal("GCS project id is not configured")

        return self.__project_id

    def __timeout(self) -> int:
        if self.__config is not None:
            return max(1, int(self.__config.timeout.total_seconds()))

        return max(1, int(DEFAULT_TIMEOUT.total_seconds()))

    # ....................... #

    @asynccontextmanager
    async def client(self) -> AsyncGenerator[Storage]:
        """Yield the shared storage client (depth-tracked nested scopes)."""

        depth = self.__ctx_depth.get()

        if depth > 0:
            self.__ctx_depth.set(depth + 1)

            try:
                yield self.__require_storage()

            finally:
                self.__ctx_depth.set(depth)

            return

        token = self.__ctx_depth.set(1)

        try:
            yield self.__require_storage()

        finally:
            self.__ctx_depth.reset(token)

    # ....................... #

    async def health(self) -> tuple[str, bool]:
        """Check GCS connectivity by listing buckets."""

        try:
            await self.list_buckets_first_page()
            return "ok", True

        except Exception as e:
            return str(e), False

    async def list_buckets_first_page(self) -> None:
        """List the first page of buckets (health / diagnostics)."""

        storage = self.__require_storage()
        await storage.list_buckets(
            self.__require_project_id(),
            timeout=self.__timeout(),
        )

    # ....................... #

    @exc_interceptor.coroutine("gcs.bucket_exists")  # type: ignore[untyped-decorator]
    async def bucket_exists(self, bucket: str) -> bool:
        storage = self.__require_storage()

        try:
            await storage.get_bucket_metadata(
                bucket,
                timeout=self.__timeout(),
            )
            return True

        except aiohttp.ClientResponseError as e:
            if _response_is_not_found(e):
                return False

            raise

    # ....................... #

    @exc_interceptor.coroutine("gcs.create_bucket")  # type: ignore[untyped-decorator]
    async def create_bucket(self, bucket: str) -> None:
        await self._insert_bucket(bucket)

    # ....................... #

    @exc_interceptor.coroutine("gcs.ensure_bucket")  # type: ignore[untyped-decorator]
    async def ensure_bucket(self, bucket: str) -> None:
        if not await self.bucket_exists(bucket):
            await self.create_bucket(bucket)

    # ....................... #

    @exc_interceptor.coroutine("gcs.object_exists")  # type: ignore[untyped-decorator]
    async def object_exists(self, bucket: str, key: str) -> bool:
        storage = self.__require_storage()
        bucket_ref = storage.get_bucket(bucket)

        return await bucket_ref.blob_exists(key)

    # ....................... #

    @exc_interceptor.coroutine("gcs.upload_bytes")  # type: ignore[untyped-decorator]
    async def upload_bytes(
        self,
        bucket: str,
        key: str,
        data: bytes,
        *,
        content_type: str | None = None,
        metadata: dict[str, str] | None = None,
    ) -> None:
        storage = self.__require_storage()
        upload_metadata: dict[str, object] | None = None

        if metadata is not None:
            upload_metadata = {"metadata": metadata}

        await storage.upload(
            bucket,
            key,
            data,
            content_type=content_type,
            metadata=upload_metadata,
            timeout=self.__timeout(),
        )

    # ....................... #

    @exc_interceptor.coroutine("gcs.download_bytes")  # type: ignore[untyped-decorator]
    async def download_bytes(self, bucket: str, key: str) -> bytes:
        storage = self.__require_storage()

        return await storage.download(
            bucket,
            key,
            timeout=self.__timeout(),
        )

    # ....................... #

    @exc_interceptor.coroutine("gcs.delete_object")  # type: ignore[untyped-decorator]
    async def delete_object(self, bucket: str, key: str) -> None:
        storage = self.__require_storage()

        await storage.delete(
            bucket,
            key,
            timeout=self.__timeout(),
        )

    # ....................... #

    @exc_interceptor.coroutine("gcs.list_objects")  # type: ignore[untyped-decorator]
    async def list_objects(
        self,
        bucket: str,
        prefix: str | None = None,
        *,
        limit: int | None = None,
        offset: int | None = None,
    ) -> tuple[list[GCSListedObject], int]:
        if limit is not None and limit <= 0:
            raise exc.internal("limit must be > 0")

        if offset is not None and offset < 0:
            raise exc.internal("offset must be >= 0")

        _prefix = prefix or ""
        _limit = limit if limit is not None else 10_000_000
        _offset = offset or 0

        storage = self.__require_storage()
        bucket_ref = storage.get_bucket(bucket)
        keys = await bucket_ref.list_blobs(prefix=_prefix)

        total_count = len(keys)
        window = keys[_offset : _offset + _limit]
        items: list[GCSListedObject] = [GCSListedObject(Key=key) for key in window]

        return items, total_count

    # ....................... #

    @exc_interceptor.coroutine("gcs.head_object")  # type: ignore[untyped-decorator]
    async def head_object(self, bucket: str, key: str) -> GCSHead:
        storage = self.__require_storage()
        raw = await storage.download_metadata(
            bucket,
            key,
            timeout=self.__timeout(),
        )

        return _head_from_object_json(raw)

    # ....................... #

    async def _insert_bucket(self, bucket: str) -> None:
        storage = self.__require_storage()
        project_id = self.__require_project_id()
        url = f"{storage._api_root}/storage/v1/b?project={project_id}"  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
        body = json.dumps({"name": bucket}).encode()
        headers = (
            await storage._headers()  # pyright: ignore[reportPrivateUsage]
        )  # noqa: SLF001
        headers["Content-Type"] = "application/json"

        await storage.session.post(
            url,
            data=body,
            headers=headers,
            timeout=self.__timeout(),
        )


# ....................... #


def _response_is_not_found(exc: aiohttp.ClientResponseError) -> bool:
    status = getattr(exc, "status", None)

    if status in {404, 410}:
        return True

    code = getattr(exc, "code", None)

    return code in {404, 410}


# ....................... #


def _head_from_object_json(raw: dict[str, object]) -> GCSHead:
    custom: Any = raw.get("metadata") or {}
    meta_dict: dict[str, str] = {}

    if isinstance(custom, dict):
        meta_dict = {str(k): str(v) for k, v in cast(JsonDict, custom).items()}

    updated = raw.get("updated")
    last_modified: datetime | None = None

    if isinstance(updated, str):
        ts = updated

        if ts.endswith("Z"):
            ts = f"{ts[:-1]}+00:00"

        last_modified = datetime.fromisoformat(ts)

    elif isinstance(updated, datetime):
        last_modified = updated

    etag = raw.get("etag", "")
    etag_str = str(etag).strip('"') if etag is not None else ""

    size_raw = raw.get("size", 0)
    size = int(size_raw) if isinstance(size_raw, (int, float, str)) else 0

    return GCSHead(
        content_type=str(raw.get("contentType") or "application/octet-stream"),
        metadata=meta_dict,
        size=size,
        etag=etag_str,
        last_modified=last_modified,
    )
