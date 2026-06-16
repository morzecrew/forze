from forze.base.primitives import JsonDict
from forze_gcs._compat import require_gcs

require_gcs()

# ....................... #

import json
import os
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, AsyncGenerator, Literal, Mapping, Sequence, cast, final

import aiohttp
import attrs
from gcloud.aio.storage import Blob, Storage

from forze.application.contracts.storage import PresignedUrl
from forze.application.integrations.storage.client import (
    PRESIGN_MAX_EXPIRY,
    ObjectBody,
    ObjectStorageHead,
    ObjectStorageListedObject,
    ObjectStoragePartInfo,
    ObjectStorageSSE,
    build_range_header,
    normalize_list_window,
    presign_expiry_seconds,
    unsatisfiable_range,
    validate_range,
)
from forze.base.exceptions import exc
from forze.base.primitives import (
    ContextScopedResource,
    GuardedLifecycle,
    utcnow,
    uuid7,
)
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

MPU_NAMESPACE = "__forze_mpu__"
"""Path segment under which GCS compose-based multipart temp parts live.

GCS has no native multipart-session API, so resumable multipart is emulated:
each part is uploaded to a temp object at
``<key>.__forze_mpu__/<session>/<part_number>`` and the final object is
assembled by composing the temps in part-number order. The session id is the
``upload_id`` the port threads through every call."""

COMPOSE_MAX_SOURCES = 32
"""Maximum source objects GCS ``compose`` accepts in a single call.

Multipart completions with more parts than this are assembled by **chaining**
composes (compose accumulated result + next batch), so arbitrarily many parts
are supported within the standard 32-per-call limit."""

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
        sse: ObjectStorageSSE | None = None,
    ) -> None:
        """Upload raw bytes to a GCS object.

        Tags are persisted as custom metadata keys namespaced with
        :data:`TAG_METADATA_PREFIX` (GCS has no S3-style tag API) and are
        round-tripped back into :attr:`ObjectStorageHead.tags` by
        :meth:`head_object`.

        When *sse* carries a CMEK ``key_id`` it is passed as the ``kmsKeyName``
        query parameter so this **app-path** object is encrypted at rest under
        the customer-managed key (GCS has no SSE-S3 analog; the Google-managed
        default is always on otherwise).
        """

        storage = self.__require_storage()
        custom: dict[str, str] = dict(metadata) if metadata is not None else {}

        if tags:
            for tag_key, tag_value in tags.items():
                custom[f"{TAG_METADATA_PREFIX}{tag_key}"] = tag_value

        upload_metadata: dict[str, object] | None = None

        if metadata is not None or tags:
            upload_metadata = {"metadata": custom}

        kwargs: dict[str, Any] = {
            "content_type": content_type,
            "metadata": upload_metadata,
            "timeout": self.__timeout(),
        }

        if cmek := _gcs_cmek_params(sse):
            kwargs["parameters"] = cmek

        await storage.upload(bucket, key, data, **kwargs)

    # ....................... #

    @exc_interceptor.coroutine("gcs.download_bytes")  # type: ignore[untyped-decorator]
    async def download_bytes(self, bucket: str, key: str) -> ObjectBody:
        """Download a GCS object's full body plus its metadata.

        ``Storage.download`` returns only the bytes, so the content type and
        user metadata are read with a single ``download_metadata`` call and
        consolidated onto the returned :class:`ObjectBody`. (The adapter used to
        issue its own ``head_object`` here; the metadata call replaces it — same
        round-trip count, one place.)
        """

        storage = self.__require_storage()
        timeout = self.__timeout()

        data = await storage.download(bucket, key, timeout=timeout)
        raw = await storage.download_metadata(bucket, key, timeout=timeout)
        head = _head_from_object_json(raw)

        return ObjectBody(
            data=data,
            content_type=head.content_type,
            metadata=dict(head.metadata),
        )

    # ....................... #

    @exc_interceptor.coroutine("gcs.download_range_bytes")  # type: ignore[untyped-decorator]
    async def download_range_bytes(
        self,
        bucket: str,
        key: str,
        *,
        start: int,
        end: int | None = None,
    ) -> tuple[ObjectBody, str, int]:
        """Download an inclusive byte range via a ranged media ``GET``.

        Sends ``Range: bytes=start-end`` (``end`` inclusive; ``end=None`` reads
        to EOF). The object's total size is read from a metadata head (GCS does
        not return a ``Content-Range`` total uniformly via this client), and the
        satisfied ``Content-Range`` is synthesized from the returned slice. An
        unsatisfiable range (``start`` beyond the object) raises a precondition
        error (the 416/``RequestedRangeNotSatisfiable`` equivalent).

        :returns: ``(body, content_range, total_size)`` — the body's content
            type comes from the already-fetched metadata head; its metadata is
            left empty for ranges.
        """

        validate_range(start, end)
        storage = self.__require_storage()

        raw = await storage.download_metadata(bucket, key, timeout=self.__timeout())
        head = _head_from_object_json(raw)
        total = head.size

        if start >= total > 0:
            raise unsatisfiable_range(start, total)

        try:
            data = await storage.download(
                bucket,
                key,
                headers={"Range": build_range_header(start, end)},
                timeout=self.__timeout(),
            )

        except aiohttp.ClientResponseError as e:
            if getattr(e, "status", None) == 416 or getattr(e, "code", None) == 416:
                raise unsatisfiable_range(start, total) from e

            raise

        end_byte = start + len(data) - 1 if data else start
        content_range = f"bytes {start}-{end_byte}/{total}"

        body = ObjectBody(data=data, content_type=head.content_type)

        return body, content_range, total

    # ....................... #

    @exc_interceptor.coroutine("gcs.download_bytes_conditional")  # type: ignore[untyped-decorator]
    async def download_bytes_conditional(
        self,
        bucket: str,
        key: str,
        *,
        if_none_match: str | None = None,
        if_modified_since: datetime | None = None,
    ) -> ObjectBody | None:
        """Conditional media ``GET`` returning ``None`` when not modified.

        Sends ``If-None-Match`` / ``If-Modified-Since``; a ``304 Not Modified``
        (or ``412 Precondition Failed``) response maps to ``None``. The content
        type and user metadata come from a metadata head on the changed path
        (already fetched here), so the caller needs no separate head.
        """

        storage = self.__require_storage()

        headers: dict[str, Any] = {}

        if if_none_match is not None:
            headers["If-None-Match"] = if_none_match

        if if_modified_since is not None:
            headers["If-Modified-Since"] = _http_date(if_modified_since)

        try:
            data = await storage.download(
                bucket,
                key,
                headers=headers,
                timeout=self.__timeout(),
            )

        except aiohttp.ClientResponseError as e:
            status = getattr(e, "status", None) or getattr(e, "code", None)

            if status in {304, 412}:
                return None

            raise

        raw = await storage.download_metadata(bucket, key, timeout=self.__timeout())
        head = _head_from_object_json(raw)

        return ObjectBody(
            data=data,
            content_type=head.content_type,
            metadata=dict(head.metadata),
        )

    # ....................... #

    @exc_interceptor.coroutine("gcs.copy_object")  # type: ignore[untyped-decorator]
    async def copy_object(
        self,
        bucket: str,
        src_key: str,
        dst_key: str,
        *,
        sse: ObjectStorageSSE | None = None,
    ) -> None:
        """Server-side copy within *bucket* via the GCS rewrite API.

        Uses :meth:`gcloud.aio.storage.Storage.copy`, which drives the
        ``rewriteTo`` endpoint and loops the rewrite token, so it handles
        arbitrarily large objects (no single-call size cap). Same-bucket only.

        When *sse* carries a CMEK ``key_id`` it rides as the ``destinationKmsKeyName``
        rewrite parameter, so the destination is encrypted at rest under the
        customer-managed key.
        """

        storage = self.__require_storage()

        params = _gcs_cmek_rewrite_params(sse)

        await storage.copy(
            bucket,
            src_key,
            bucket,
            new_name=dst_key,
            params=params or None,
            timeout=self.__timeout(),
        )

    # ....................... #

    @exc_interceptor.coroutine("gcs.put_object_tags")  # type: ignore[untyped-decorator]
    async def put_object_tags(
        self,
        bucket: str,
        key: str,
        tags: Mapping[str, str],
    ) -> None:
        """Replace the object's namespaced tag custom-metadata (full replace).

        GCS has no S3-style tag API; tags live as custom-metadata keys prefixed
        with :data:`TAG_METADATA_PREFIX`. This reads current metadata, clears
        every existing namespaced tag key (PATCH ``null`` deletes a custom key),
        and writes the new set — non-tag user metadata is left untouched.
        """

        storage = self.__require_storage()

        raw = await storage.download_metadata(bucket, key, timeout=self.__timeout())
        existing: Any = raw.get("metadata") or {}

        new_custom: dict[str, Any] = {}

        if isinstance(existing, dict):
            for k in cast(dict[str, Any], existing):
                if str(k).startswith(TAG_METADATA_PREFIX):
                    # null deletes the custom-metadata key on PATCH.
                    new_custom[str(k)] = None

        for tag_key, tag_value in tags.items():
            new_custom[f"{TAG_METADATA_PREFIX}{tag_key}"] = tag_value

        await storage.patch_metadata(
            bucket,
            key,
            {"metadata": new_custom},
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

        # Drop in-flight/orphaned multipart scaffolding: compose-based MPU stores
        # temp parts at ``<key>.__forze_mpu__/<session>/<n>`` (no metadata
        # envelope), which would otherwise surface as bogus listed objects and
        # break the adapter's per-object metadata read.
        keys = [
            k
            for k in keys
            if not (
                f".{MPU_NAMESPACE}/" in k
                and (tail := k.rsplit(f".{MPU_NAMESPACE}/", 1)[1]).count("/") == 1
                and (
                    tail.split("/", 1)[1].isdigit()
                    or tail.split("/", 1)[1] == "__compose__"
                )
            )
        ]

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
        sse: ObjectStorageSSE | None = None,
    ) -> PresignedUrl:
        """Sign a time-limited ``PUT`` URL for the blob (V4 query auth).

        When *content_type* is given it becomes a **signed header**, so the
        uploader must send it verbatim — the returned
        :attr:`PresignedUrl.headers` carries it. See :meth:`__presign` for
        the credential requirements and limits.

        *sse* is accepted for port symmetry but **does not** add a header:
        unlike S3, GCS cannot carry a per-object CMEK key on a raw signed
        ``PUT``. CMEK for presigned (and resumable/multipart) direct uploads
        relies on the **bucket's default encryption** config (``encryption.
        defaultKmsKeyName``), set out-of-band on the bucket; per-object CMEK
        only covers the app-path :meth:`upload_bytes` / ``compose``.

        :param bucket: Bucket name.
        :param key: Blob name to upload to.
        :param expires_in: URL lifetime (positive, at most 7 days).
        :param content_type: Optional MIME type to bind into the signature.
        :param sse: Accepted for port symmetry; not bound on GCS presign (see
            above — CMEK rides the bucket default for direct PUTs).
        :raises CoreException: ``validation`` when *expires_in* is out of
            range; ``configuration`` when the bound credentials cannot sign.
        """

        _ = sse  # GCS presigned PUTs cannot carry a CMEK header (bucket default)

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

    # ....................... #
    # Resumable multipart upload primitives (compose-based, see MPU_NAMESPACE).

    @staticmethod
    def _mpu_prefix(key: str, upload_id: str) -> str:
        return f"{key}.{MPU_NAMESPACE}/{upload_id}/"

    @classmethod
    def _mpu_part_key(cls, key: str, upload_id: str, part_number: int) -> str:
        return f"{cls._mpu_prefix(key, upload_id)}{part_number}"

    # ....................... #

    @exc_interceptor.coroutine("gcs.create_multipart_upload")  # type: ignore[untyped-decorator]
    async def create_multipart_upload(
        self,
        bucket: str,
        key: str,
        *,
        content_type: str | None = None,
        sse: ObjectStorageSSE | None = None,
    ) -> str:
        """Allocate a temp part-key namespace and return its session token.

        GCS has no native multipart session, so this mints a session id; the
        temp parts land under ``<key>.__forze_mpu__/<session>/<n>`` and the
        final object is assembled by :meth:`complete_multipart_upload` via
        ``compose``. *content_type* and *sse* are bound at completion time
        (the compose destination), not here, so *sse* is accepted for port
        compatibility and ignored on this no-op token mint.
        """

        _ = (bucket, key, content_type, sse)

        return str(uuid7())

    # ....................... #

    @exc_interceptor.coroutine("gcs.presign_multipart_part")  # type: ignore[untyped-decorator]
    async def presign_multipart_part(
        self,
        bucket: str,
        key: str,
        *,
        upload_id: str,
        part_number: int,
        expires_in: timedelta,
    ) -> PresignedUrl:
        """Sign a ``PUT`` URL for a temp part object (reuses V4 upload signing)."""

        part_key = self._mpu_part_key(key, upload_id, part_number)

        return await self.__presign(
            bucket,
            part_key,
            expires_in=expires_in,
            method="PUT",
            content_type=None,
        )

    # ....................... #

    @exc_interceptor.coroutine("gcs.list_multipart_parts")  # type: ignore[untyped-decorator]
    async def list_multipart_parts(
        self,
        bucket: str,
        key: str,
        *,
        upload_id: str,
    ) -> list[ObjectStoragePartInfo]:
        """List the temp part objects of the session (the resume primitive)."""

        storage = self.__require_storage()
        bucket_ref = storage.get_bucket(bucket)
        prefix = self._mpu_prefix(key, upload_id)

        keys = await bucket_ref.list_blobs(prefix=prefix)

        parts: list[ObjectStoragePartInfo] = []

        for blob_key in keys:
            suffix = blob_key[len(prefix) :]

            if not suffix.isdigit():
                continue

            parts.append(ObjectStoragePartInfo(part_number=int(suffix)))

        parts.sort(key=lambda p: p.part_number)

        return parts

    # ....................... #

    @exc_interceptor.coroutine("gcs.complete_multipart_upload")  # type: ignore[untyped-decorator]
    async def complete_multipart_upload(
        self,
        bucket: str,
        key: str,
        *,
        upload_id: str,
        parts: Sequence[ObjectStoragePartInfo],
        content_type: str | None = None,
        sse: ObjectStorageSSE | None = None,
    ) -> None:
        """Compose the temp parts in order into *key*, then delete the temps.

        ``compose`` takes at most :data:`COMPOSE_MAX_SOURCES` sources per call,
        so larger part sets are assembled by chaining: compose the running
        result with the next batch, repeatedly, until all parts are folded in.
        The temp part objects (and any intermediate temp) are cleaned up after.

        *content_type* (bound by the caller at ``begin_upload``) is applied to
        the **final** destination object only: temp parts are uploaded via bare
        presigned ``PUT`` URLs that carry no type, so without this the composed
        object would inherit ``application/octet-stream`` from the first part.
        Intermediate accumulators keep the default type (they are deleted).

        When *sse* carries a CMEK ``key_id`` it is passed as the ``kmsKeyName``
        compose parameter so the **final** destination object is encrypted at
        rest under the customer-managed key. (Intermediate compose accumulators
        for >32-part sets get the same key.)
        """

        storage = self.__require_storage()
        timeout = self.__timeout()

        cmek_params = _gcs_cmek_params(sse) or None

        ordered = sorted(parts, key=lambda p: p.part_number)

        if not ordered:
            raise exc.validation("complete_multipart_upload requires at least one part")

        part_keys = [self._mpu_part_key(key, upload_id, p.part_number) for p in ordered]

        cleanup: set[str] = set(part_keys)

        # ``compose`` rejects a single-source request (it requires >= 2 sources).
        # Two code paths can otherwise produce one — a 1-part upload and the
        # final fold of a >32-part chain — so both are handled explicitly. Note:
        # fake-gcs-server is lenient and accepts single-source compose, which
        # masks this; real GCS returns 400.
        if len(part_keys) == 1:
            # Rewrite the lone part to the destination (compose can't take one
            # source). Mirror copy_object's CMEK-aware ``storage.copy`` usage,
            # including destinationKmsKeyName via the rewrite params.
            await storage.copy(
                bucket,
                part_keys[0],
                bucket,
                new_name=key,
                metadata={"contentType": content_type} if content_type else None,
                params=_gcs_cmek_rewrite_params(sse) or None,
                timeout=timeout,
            )

        elif len(part_keys) <= COMPOSE_MAX_SOURCES:
            await storage.compose(
                bucket,
                key,
                part_keys,
                content_type=content_type,
                params=cmek_params,
                timeout=timeout,
            )

        else:
            # Chain composes when there are more than COMPOSE_MAX_SOURCES parts.
            # The accumulated result is written to a temp object, then folded
            # with the next batch, so no single compose exceeds the source cap.
            # Each fold includes the accumulator itself plus >= 1 further part,
            # so every compose always has >= 2 sources (never single-source).
            acc_key = f"{self._mpu_prefix(key, upload_id)}__compose__"
            cleanup.add(acc_key)

            await storage.compose(
                bucket,
                acc_key,
                part_keys[:COMPOSE_MAX_SOURCES],
                # Intermediate accumulator: default type, deleted after assembly.
                content_type=None,
                params=cmek_params,
                timeout=timeout,
            )

            rest = part_keys[COMPOSE_MAX_SOURCES:]

            while rest:
                # compose includes the accumulator itself, so each step folds in
                # at most (cap - 1) further parts.
                batch = rest[: COMPOSE_MAX_SOURCES - 1]
                rest = rest[COMPOSE_MAX_SOURCES - 1 :]

                # The final fold writes straight to the destination key; earlier
                # folds round-trip through the accumulator.
                dest = acc_key if rest else key

                await storage.compose(
                    bucket,
                    dest,
                    [acc_key, *batch],
                    # Only the final destination object gets the caller's type;
                    # intermediate accumulators keep the default and are deleted.
                    content_type=content_type if dest == key else None,
                    params=cmek_params,
                    timeout=timeout,
                )

        await self.__delete_mpu_keys(bucket, cleanup)

    # ....................... #

    @exc_interceptor.coroutine("gcs.abort_multipart_upload")  # type: ignore[untyped-decorator]
    async def abort_multipart_upload(
        self,
        bucket: str,
        key: str,
        *,
        upload_id: str,
    ) -> None:
        """Delete every temp part object of the session (best-effort idempotent)."""

        storage = self.__require_storage()
        bucket_ref = storage.get_bucket(bucket)
        prefix = self._mpu_prefix(key, upload_id)

        keys = await bucket_ref.list_blobs(prefix=prefix)

        await self.__delete_mpu_keys(bucket, set(keys))

    # ....................... #

    async def __delete_mpu_keys(self, bucket: str, keys: set[str]) -> None:
        """Delete temp multipart objects, tolerating already-gone keys."""

        storage = self.__require_storage()
        timeout = self.__timeout()

        for blob_key in keys:
            try:
                await storage.delete(bucket, blob_key, timeout=timeout)

            except aiohttp.ClientResponseError as e:
                if _response_is_not_found(e):
                    continue

                raise

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


def _gcs_cmek_params(sse: ObjectStorageSSE | None) -> dict[str, str]:
    """Map a neutral SSE descriptor to GCS ``kmsKeyName`` query params.

    GCS has no SSE-S3 analog (Google-managed default encryption is always on),
    so only a CMEK ``key_id`` is meaningful: it becomes the per-object
    ``kmsKeyName`` parameter on ``insert``/``compose``. Returns ``{}`` when no
    CMEK key is requested.
    """

    return {} if sse is None or not sse.key_id else {"kmsKeyName": sse.key_id}


# ....................... #


def _gcs_cmek_rewrite_params(sse: ObjectStorageSSE | None) -> dict[str, str]:
    """Map a neutral SSE descriptor to the GCS rewrite CMEK param.

    The rewrite (``copy``) endpoint names the destination key differently:
    ``destinationKmsKeyName`` rather than ``kmsKeyName``. Returns ``{}`` when no
    CMEK key is requested.
    """

    if sse is None or not sse.key_id:
        return {}

    return {"destinationKmsKeyName": sse.key_id}


# ....................... #


def _http_date(value: datetime) -> str:
    """Format a datetime as an RFC 7231 IMF-fixdate for ``If-Modified-Since``."""

    from email.utils import format_datetime

    # ``usegmt=True`` requires a UTC datetime: stamp naive values as UTC and
    # convert tz-aware non-UTC values, so a +02:00 input formats correctly.
    value = (
        value.replace(tzinfo=timezone.utc)
        if value.tzinfo is None
        else value.astimezone(timezone.utc)
    )

    return format_datetime(value, usegmt=True)


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
