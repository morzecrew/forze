from forze.base.primitives import JsonDict
from forze_gcs._compat import require_gcs

require_gcs()

# ....................... #

import json
import os
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import Any, AsyncGenerator, Literal, cast, final

import aiohttp
import attrs
from gcloud.aio.storage import Blob, Storage

from forze.application.contracts.storage import PresignedUrl
from forze.application.integrations.storage.client import (
    PRESIGN_MAX_EXPIRY,
    ObjectStorageHead,
    ObjectStorageListedObject,
    normalize_list_window,
    presign_expiry_seconds,
)
from forze.base.exceptions import exc
from forze.base.primitives import ContextScopedResource, GuardedLifecycle, utcnow
from forze.base.primitives.owned_temp_path import OwnedTempPath

from .errors import exc_interceptor
from .port import GCSClientPort
from .value_objects import DEFAULT_TIMEOUT, GCSConfig

# ----------------------- #

TAG_METADATA_PREFIX = "forze-tag-"
"""Custom-metadata key prefix used to emulate object tags on GCS.

GCS has no S3-style tag API; tags are persisted as namespaced custom metadata
keys and split back out by :meth:`GCSClient.head_object`. User metadata keys
that happen to start with this prefix would be surfaced as tags on read-back.
"""

# ....................... #


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
    __credential_path: OwnedTempPath = attrs.field(
        factory=OwnedTempPath.empty,
        init=False,
    )

    __scope: ContextScopedResource[Storage] = attrs.field(
        factory=lambda: ContextScopedResource[Storage]("gcs"),
        init=False,
    )

    __lifecycle: GuardedLifecycle = attrs.field(factory=GuardedLifecycle, init=False)

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

        async def setup() -> None:
            self.__project_id = project_id
            self.__config = config

            api_root: str | None = None
            if host := os.environ.get("STORAGE_EMULATOR_HOST"):
                api_root = host.rstrip("/")

            key_file = service_file

            if key_file is None and config is not None:
                key_file = config.service_file

            self.__credential_path = OwnedTempPath(
                path=key_file, owned=service_file_owned
            )

            self.__storage = Storage(
                service_file=key_file,
                api_root=api_root,
            )

        await self.__lifecycle.initialize(
            setup,
            ready=lambda: self.__storage is not None,
        )

    # ....................... #

    async def close(self) -> None:
        """Release the underlying storage client and HTTP session."""

        await self.__lifecycle.close(self.__teardown)

    # ....................... #

    async def __teardown(self) -> None:
        storage = self.__storage
        close_error: Exception | None = None
        cred_error: Exception | None = None

        if storage is not None:
            try:
                await storage.close()

            except Exception as exc:
                close_error = exc

            finally:
                self.__storage = None

        try:
            self.__credential_path.release()
            self.__credential_path = OwnedTempPath.empty()
            self.__project_id = None
            self.__config = None

        except Exception as exc:
            cred_error = exc

        errors = [e for e in (close_error, cred_error) if e is not None]

        if len(errors) == 1:
            raise errors[0]

        if len(errors) > 1:
            raise ExceptionGroup("GCS client close failed", errors) from errors[0]

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

        async def acquire() -> Storage:
            return self.__require_storage()

        async with self.__scope.scope(
            acquire,
            reusable=lambda storage: self.__storage is storage,
        ) as storage:
            yield storage

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
        """Create the bucket when it does not exist (idempotent)."""

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
        tags: dict[str, str] | None = None,
    ) -> None:
        """Upload raw bytes to a GCS object.

        Tags are persisted as custom metadata keys namespaced with
        :data:`TAG_METADATA_PREFIX` (GCS has no S3-style tag API) and are
        round-tripped back into :attr:`ObjectStorageHead.tags` by
        :meth:`head_object`.
        """

        storage = self.__require_storage()
        custom: dict[str, str] = dict(metadata) if metadata is not None else {}

        if tags:
            for tag_key, tag_value in tags.items():
                custom[f"{TAG_METADATA_PREFIX}{tag_key}"] = tag_value

        upload_metadata: dict[str, object] | None = None

        if metadata is not None or tags:
            upload_metadata = {"metadata": custom}

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
        include_tags: bool = False,
    ) -> tuple[list[ObjectStorageListedObject], int]:
        """List blob keys under *prefix* with an offset/limit window.

        ``include_tags`` is accepted for port compatibility but adds nothing
        on GCS: tags live in namespaced custom metadata and surface for free
        on :meth:`head_object` (no extra calls either way), while the listing
        API only returns keys.
        """

        _ = include_tags  # guarantee already satisfied via head metadata

        _prefix = prefix or ""
        _limit, _offset = normalize_list_window(limit, offset)

        storage = self.__require_storage()
        bucket_ref = storage.get_bucket(bucket)
        keys = await bucket_ref.list_blobs(prefix=_prefix)

        total_count = len(keys)
        window = keys[_offset : _offset + _limit]
        items: list[ObjectStorageListedObject] = [
            ObjectStorageListedObject(key=key) for key in window
        ]

        return items, total_count

    # ....................... #

    @exc_interceptor.coroutine("gcs.head_object")  # type: ignore[untyped-decorator]
    async def head_object(
        self,
        bucket: str,
        key: str,
        *,
        include_tags: bool = False,
    ) -> ObjectStorageHead:
        """Fetch object metadata, splitting namespaced tags out of it.

        ``include_tags`` is accepted for port compatibility but adds nothing
        on GCS: tags are round-tripped from custom metadata for free, so
        :attr:`ObjectStorageHead.tags` is populated regardless of the flag
        (no extra calls).
        """

        _ = include_tags  # tags are always included for free on GCS

        storage = self.__require_storage()
        raw = await storage.download_metadata(
            bucket,
            key,
            timeout=self.__timeout(),
        )

        return _head_from_object_json(raw)

    # ....................... #

    @exc_interceptor.coroutine("gcs.presign_download_url")  # type: ignore[untyped-decorator]
    async def presign_download_url(
        self,
        bucket: str,
        key: str,
        *,
        expires_in: timedelta,
    ) -> PresignedUrl:
        """Sign a time-limited ``GET`` URL for the blob (V4 query auth).

        See :meth:`__presign` for the credential requirements and limits.

        :param bucket: Bucket name.
        :param key: Blob name.
        :param expires_in: URL lifetime (positive, at most 7 days).
        :raises CoreException: ``validation`` when *expires_in* is out of
            range; ``configuration`` when the bound credentials cannot sign.
        """

        return await self.__presign(
            bucket,
            key,
            expires_in=expires_in,
            method="GET",
            content_type=None,
        )

    # ....................... #

    @exc_interceptor.coroutine("gcs.presign_upload_url")  # type: ignore[untyped-decorator]
    async def presign_upload_url(
        self,
        bucket: str,
        key: str,
        *,
        expires_in: timedelta,
        content_type: str | None = None,
    ) -> PresignedUrl:
        """Sign a time-limited ``PUT`` URL for the blob (V4 query auth).

        When *content_type* is given it becomes a **signed header**, so the
        uploader must send it verbatim — the returned
        :attr:`PresignedUrl.headers` carries it. See :meth:`__presign` for
        the credential requirements and limits.

        :param bucket: Bucket name.
        :param key: Blob name to upload to.
        :param expires_in: URL lifetime (positive, at most 7 days).
        :param content_type: Optional MIME type to bind into the signature.
        :raises CoreException: ``validation`` when *expires_in* is out of
            range; ``configuration`` when the bound credentials cannot sign.
        """

        return await self.__presign(
            bucket,
            key,
            expires_in=expires_in,
            method="PUT",
            content_type=content_type,
        )

    # ....................... #

    async def __presign(
        self,
        bucket: str,
        key: str,
        *,
        expires_in: timedelta,
        method: Literal["GET", "PUT"],
        content_type: str | None,
    ) -> PresignedUrl:
        """Build a V4 signed URL via :meth:`gcloud.aio.storage.Blob.get_signed_url`.

        **Credential requirements.** V4 signing needs signing material:

        - With an explicit service-account JSON key (``service_file`` /
          ``GOOGLE_APPLICATION_CREDENTIALS`` carrying ``client_email`` +
          ``private_key``) the URL is signed **locally** — no API round-trip.
        - Without a private key (ADC / metadata-server tokens) the installed
          ``gcloud-aio-storage`` can sign **remotely** via the IAM
          Credentials API (``signBlob``), but only for an explicitly named
          account: set :attr:`GCSConfig.signing_service_account_email`. The
          ambient credentials must hold ``iam.serviceAccounts.signBlob`` on
          that account; this path costs one IAM API round-trip per URL.
        - ``AUTHORIZED_USER`` (gcloud user) credentials cannot sign at all —
          the library rejects them (mapped to a configuration error).

        When neither a private key nor a configured signing account is
        available this raises ``exc.configuration`` instead of producing an
        unverifiable URL. GCS V4 caps expiries at 7 days. The URL host is
        resolved by the library at import time (``STORAGE_EMULATOR_HOST`` or
        ``storage.googleapis.com``) with a fixed ``https`` scheme.
        """

        seconds = presign_expiry_seconds(expires_in, max_expiry=PRESIGN_MAX_EXPIRY)
        storage = self.__require_storage()

        token = storage.token
        service_data: dict[str, Any] = getattr(token, "service_data", None) or {}
        has_local_key = bool(service_data.get("client_email")) and bool(
            service_data.get("private_key")
        )

        signing_email = (
            self.__config.signing_service_account_email
            if self.__config is not None
            else None
        )

        if not has_local_key and not signing_email:
            raise exc.configuration(
                "GCS presigned URLs require signing material: provide a "
                "service-account JSON key (client_email + private_key) via "
                "service_file / GOOGLE_APPLICATION_CREDENTIALS for local "
                "signing, or set GCSConfig.signing_service_account_email to "
                "sign remotely via the IAM Credentials API (signBlob). The "
                "bound credentials (ADC/metadata token without a private "
                "key) cannot sign URLs."
            )

        signed_headers: dict[str, str] | None = None
        out_headers: dict[str, str] = {}

        if content_type is not None:
            signed_headers = {"content-type": content_type}
            out_headers["Content-Type"] = content_type

        blob = Blob(storage.get_bucket(bucket), key, {"size": 0})

        sign_kwargs: dict[str, Any] = {}

        if not has_local_key:
            # IAM signBlob path: name the account and reuse the pooled session
            # (also keeps the library from closing it after the call).
            sign_kwargs["service_account_email"] = signing_email
            sign_kwargs["session"] = storage.session.session

        expires_at = utcnow() + timedelta(seconds=seconds)

        try:
            url = await blob.get_signed_url(
                seconds,
                headers=signed_headers,
                http_method=method,
                **sign_kwargs,
            )

        except ValueError as e:
            # Defensive: the library enforces the same 7-day V4 cap.
            raise exc.validation(str(e)) from e

        except TypeError as e:
            raise exc.configuration(
                "GCS presigned URLs cannot be signed with the bound "
                "credentials (AUTHORIZED_USER tokens are not supported by "
                "gcloud-aio-storage signing)"
            ) from e

        return PresignedUrl(
            url=url,
            method=method,
            expires_at=expires_at,
            headers=out_headers,
        )

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


def _head_from_object_json(raw: dict[str, object]) -> ObjectStorageHead:
    custom: Any = raw.get("metadata") or {}
    meta_dict: dict[str, str] = {}
    tags: dict[str, str] = {}

    if isinstance(custom, dict):
        for k, v in cast(JsonDict, custom).items():
            key, value = str(k), str(v)

            if key.startswith(TAG_METADATA_PREFIX):
                tags[key[len(TAG_METADATA_PREFIX) :]] = value

            else:
                meta_dict[key] = value

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

    return ObjectStorageHead(
        content_type=str(raw.get("contentType") or "application/octet-stream"),
        metadata=meta_dict,
        size=size,
        etag=etag_str,
        last_modified=last_modified,
        tags=tags,
    )
