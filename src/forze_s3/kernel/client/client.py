from __future__ import annotations

from forze_s3._compat import require_s3

require_s3()

# ....................... #

import asyncio
import io
from collections.abc import Mapping as MappingABC
from contextlib import AsyncExitStack, asynccontextmanager
from contextvars import ContextVar
from datetime import datetime, timedelta
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncContextManager,
    AsyncGenerator,
    Final,
    Mapping,
    Sequence,
    cast,
    final,
)
from urllib.parse import urlencode

import aioboto3
import attrs
from pydantic import SecretStr

if TYPE_CHECKING:
    # Type-only: ``types-aiobotocore-s3`` is a stub package with no runtime value, so
    # keep it off the import path (saves ~40 ms of cold-start import).
    from types_aiobotocore_s3.client import S3Client as AsyncS3Client

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
from forze.base.primitives import JsonDict, utcnow

from .errors import exc_interceptor
from .port import S3ClientPort
from .value_objects import S3Config, S3ConnectionOpts

# ----------------------- #

GET_OBJECT_TAGGING_CONCURRENCY: Final[int] = 8
"""Max concurrent ``GetObjectTagging`` calls when :meth:`S3Client.list_objects`
fans out per-object tag fetches for ``include_tags=True``."""

# ....................... #


@final
@attrs.define(slots=True)
class S3Client(S3ClientPort):
    """Async S3 client with a long-lived connection and context-scoped reuse.

    Must be :meth:`initialize`d with an endpoint before use; ``initialize``
    opens a single long-lived ``aioboto3`` client that all :meth:`client`
    scopes share until :meth:`close`. Nested entries reuse the same client
    via context variables.
    """

    __opts: S3ConnectionOpts | None = attrs.field(default=None, init=False)
    __session: aioboto3.Session | None = attrs.field(default=None, init=False)

    __persistent_client: AsyncS3Client | None = attrs.field(default=None, init=False)
    """Long-lived S3 client opened by :meth:`initialize`, shared by all scopes.

    aiobotocore clients are coroutine-safe for concurrent calls: each client
    owns one ``aiohttp.ClientSession`` with a pooled ``TCPConnector``
    (bounded by ``max_pool_connections``) and serializes credential refresh
    behind an ``asyncio.Lock``, so a single instance can serve concurrent
    operations from multiple tasks.
    """

    __exit_stack: AsyncExitStack | None = attrs.field(default=None, init=False)
    """Owns the persistent client's async context; closed by :meth:`close`."""

    __ctx_client: ContextVar[AsyncS3Client | None] = attrs.field(
        factory=lambda: ContextVar("s3_client", default=None),
        init=False,
    )
    __ctx_depth: ContextVar[int] = attrs.field(
        factory=lambda: ContextVar("s3_depth", default=0),
        init=False,
    )
    __init_lock: asyncio.Lock = attrs.field(factory=asyncio.Lock, init=False)

    # ....................... #
    # Lifecycle

    async def initialize(
        self,
        endpoint: str,
        access_key_id: str | None = None,
        secret_access_key: str | SecretStr | None = None,
        config: S3Config | None = None,
    ) -> None:
        """Configure the client and open a long-lived ``aioboto3`` client.

        No-ops if the session is already initialized. The underlying
        ``aiobotocore`` client is created **once** here; subsequent
        :meth:`client` scopes reuse it (depth tracking only) until
        :meth:`close` releases it. When no ``retries`` key is present in
        *config*, a default adaptive retry strategy with up to 3 attempts is
        applied automatically. Concurrent calls serialize on an internal lock
        so only one coroutine performs the setup.

        Credentials are optional: when *access_key_id* and
        *secret_access_key* are both ``None`` (the default), they are **not**
        passed to the client and botocore's default credential chain resolves
        them instead (environment variables, shared config/credentials files,
        container/instance roles — ECS task roles, EC2 instance profiles,
        EKS IRSA). Passing explicit static credentials keeps the previous
        behavior. Providing only one of the two raises a configuration error.

        :param endpoint: S3-compatible endpoint URL.
        :param access_key_id: AWS access key identifier, or ``None`` to defer
            to the default credential chain.
        :param secret_access_key: AWS secret access key (plain or
            :class:`SecretStr`), or ``None`` to defer to the chain.
        :param config: Optional botocore configuration overrides.
        """

        async with self.__init_lock:
            if self.__session is not None:
                return

            cfg = config if config is not None else S3Config()
            aio_config = cfg.to_aio_config()

            self.__opts = S3ConnectionOpts(
                endpoint=endpoint,
                access_key_id=access_key_id,
                secret_access_key=secret_access_key,
                config=aio_config,
            )
            self.__session = aioboto3.Session()

            stack = AsyncExitStack()

            try:
                self.__persistent_client = await stack.enter_async_context(
                    self.__create_client_cm()
                )

            except BaseException:
                await stack.aclose()
                self.__persistent_client = None
                self.__session = None
                self.__opts = None
                raise

            self.__exit_stack = stack

    # ....................... #

    async def close(self) -> None:
        """Close the long-lived client and release session and options.

        ``close()`` invalidates ambient scopes: scopes still nested when it
        runs keep their (now closed) client reference and fail on next use,
        but exit cleanly — they only reset context variables and never
        re-exit the shared client, so there is no deadlock or double-close.
        """

        async with self.__init_lock:
            stack = self.__exit_stack
            self.__exit_stack = None
            self.__persistent_client = None

            try:
                if stack is not None:
                    await stack.aclose()

            finally:
                self.__session = None
                self.__opts = None

    # ....................... #

    def __require_session(self) -> aioboto3.Session:
        if self.__session is None:
            raise exc.internal("S3 session is not initialized")

        return self.__session

    # ....................... #

    def __current_client(self) -> AsyncS3Client | None:
        return self.__ctx_client.get()

    # ....................... #

    def __require_client(self) -> AsyncS3Client:
        c = self.__current_client()

        if c is None:
            raise exc.internal("S3 client is not initialized")

        return c

    # ....................... #

    def __create_client_cm(self) -> AsyncContextManager[AsyncS3Client]:
        """Build the ``aiobotocore`` client async context manager from opts.

        When credentials are absent in the options, the ``aws_*`` kwargs are
        omitted so botocore's default credential chain resolves them.
        """

        session = self.__require_session()
        opts = self.__opts

        if opts is None:
            raise exc.internal("S3 client options are not initialized")

        kwargs: dict[str, Any] = {
            "endpoint_url": opts.endpoint,
            "config": opts.config,
        }

        if opts.access_key_id is not None and opts.secret_access_key is not None:
            kwargs["aws_access_key_id"] = opts.access_key_id
            kwargs["aws_secret_access_key"] = opts.secret_access_key.get_secret_value()

        cm = session.client("s3", **kwargs)  # type: ignore

        return cast("AsyncContextManager[AsyncS3Client]", cm)

    # ....................... #

    @asynccontextmanager
    async def client(self) -> AsyncGenerator[AsyncS3Client]:
        """Yield a context-scoped S3 client.

        When :meth:`initialize` has opened the long-lived client, scopes are
        cheap: they bind the shared client to the current context (depth
        tracking only), and nested calls reuse it until the outermost context
        exits. Lazy per-scope client construction remains as a fallback for
        instances whose options were configured without the persistent client
        (un-lifecycled usage).
        """

        depth = self.__ctx_depth.get()
        parent = self.__current_client()

        if depth > 0 and parent is not None:
            self.__ctx_depth.set(depth + 1)

            try:
                yield parent

            finally:
                self.__ctx_depth.set(depth)

            return

        persistent = self.__persistent_client

        if persistent is not None:
            token_client = self.__ctx_client.set(persistent)
            token_depth = self.__ctx_depth.set(1)

            try:
                yield persistent

            finally:
                self.__ctx_client.reset(token_client)
                self.__ctx_depth.reset(token_depth)

            return

        async with self.__create_client_cm() as c:
            token_client = self.__ctx_client.set(c)
            token_depth = self.__ctx_depth.set(1)

            try:
                yield c

            finally:
                self.__ctx_client.reset(token_client)
                self.__ctx_depth.reset(token_depth)

    # ....................... #

    async def health(self) -> tuple[str, bool]:
        """Check S3 connectivity by listing buckets.

        :returns: A tuple of ``("ok", True)`` on success, or
            ``(error_message, False)`` on failure.
        """

        c = self.__require_client()

        try:
            await c.list_buckets()
            return "ok", True

        except Exception as e:
            return str(e), False

    # ....................... #

    @exc_interceptor.coroutine("s3.bucket_exists")
    async def bucket_exists(self, bucket: str) -> bool:
        """Return whether the given bucket exists.

        :param bucket: Bucket name to probe.
        """

        c = self.__require_client()

        try:
            await c.head_bucket(Bucket=bucket)
            return True

        except c.exceptions.ClientError as e:  # type: ignore[attr-defined]
            code = (e.response or {}).get("Error", {}).get("Code")

            if code in {"404", "NoSuchBucket", "NotFound"}:
                return False

            raise

    # ....................... #

    def __region_name(self) -> str | None:
        opts = self.__opts

        if opts is None or opts.config is None:
            return None

        region = getattr(opts.config, "region_name", None)

        return cast(str | None, region)

    # ....................... #

    @exc_interceptor.coroutine("s3.create_bucket")
    async def create_bucket(self, bucket: str) -> None:
        """Create a bucket, silently succeeding if it already exists.

        S3 requires a ``LocationConstraint`` for every region except
        ``us-east-1``, where it must be omitted. The configured region is
        forwarded when set; with no configured region the **resolved** region
        of the live client (``client.meta.region_name``, i.e. what botocore's
        chain resolved from env/profile/IMDS) is used, so the bucket lands in
        the region the client actually targets.

        :param bucket: Bucket name to create.
        """

        c = self.__require_client()
        region = self.__region_name()

        if region is None:
            meta = getattr(c, "meta", None)
            region = getattr(meta, "region_name", None) if meta is not None else None

        try:
            if region and region != "us-east-1":
                await c.create_bucket(
                    Bucket=bucket,
                    CreateBucketConfiguration=cast(
                        Any,
                        {"LocationConstraint": region},
                    ),
                )

            else:
                await c.create_bucket(Bucket=bucket)

        except c.exceptions.ClientError as e:  # type: ignore[attr-defined]
            code = (e.response or {}).get("Error", {}).get("Code")

            if code in {"409", "BucketAlreadyExists", "BucketAlreadyOwnedByYou"}:
                return

            raise

    # ....................... #

    @exc_interceptor.coroutine("s3.ensure_bucket")
    async def ensure_bucket(self, bucket: str) -> None:
        """Create the bucket when it does not exist (idempotent).

        Concurrent creation races are tolerated: ``BucketAlreadyOwnedByYou``
        (and equivalent conflicts) from :meth:`create_bucket` are treated as
        success.

        :param bucket: Bucket name to ensure.
        """

        if not await self.bucket_exists(bucket):
            await self.create_bucket(bucket)

    # ....................... #

    @exc_interceptor.coroutine("s3.object_exists")
    async def object_exists(self, bucket: str, key: str) -> bool:
        """Return whether the given object key exists in the bucket.

        :param bucket: Bucket name.
        :param key: Object key to probe.
        """

        c = self.__require_client()

        try:
            await c.head_object(Bucket=bucket, Key=key)
            return True

        except c.exceptions.ClientError as e:
            code = (e.response or {}).get("Error", {}).get("Code")

            if code in {"404", "NoSuchKey", "NotFound"}:
                return False

            raise

    # ....................... #

    @exc_interceptor.coroutine("s3.upload_bytes")
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
        """Upload raw bytes to an S3 object.

        :param bucket: Target bucket name.
        :param key: Object key.
        :param data: Raw bytes to upload.
        :param content_type: Optional MIME type.
        :param metadata: Optional user-defined metadata.
        :param tags: Optional object tags, encoded as URL query parameters.
        :param sse: Optional server-side-encryption request. ``"s3"`` sets
            ``ServerSideEncryption=AES256``; ``"kms"`` sets
            ``ServerSideEncryption=aws:kms`` with ``SSEKMSKeyId``.
        """

        c = self.__require_client()

        extra: dict[str, Any] = {}

        if content_type is not None:
            extra["ContentType"] = content_type

        if metadata is not None:
            extra["Metadata"] = metadata

        if tags:
            extra["Tagging"] = urlencode(tags)

        extra |= _s3_sse_extra_args(sse)

        fileobj = io.BytesIO(data)

        if extra:
            await c.upload_fileobj(fileobj, Bucket=bucket, Key=key, ExtraArgs=extra)

        else:
            await c.upload_fileobj(fileobj, Bucket=bucket, Key=key)

    # ....................... #

    @exc_interceptor.coroutine("s3.download_bytes")
    async def download_bytes(self, bucket: str, key: str) -> ObjectBody:
        """Download the full content of an S3 object plus its metadata.

        A single ``GetObject`` already returns the content type and user
        metadata alongside the body, so they are surfaced on the returned
        :class:`ObjectBody` (no separate ``HeadObject`` round-trip needed).

        :param bucket: Bucket name.
        :param key: Object key.
        :returns: The object body with content type and user metadata.
        """

        c = self.__require_client()

        resp = await c.get_object(Bucket=bucket, Key=key)
        body = resp["Body"]
        data = await body.read()

        return ObjectBody(
            data=data,
            content_type=resp.get("ContentType", "application/octet-stream"),
            metadata=resp.get("Metadata", {}),
        )

    # ....................... #

    @exc_interceptor.coroutine("s3.download_range_bytes")
    async def download_range_bytes(
        self,
        bucket: str,
        key: str,
        *,
        start: int,
        end: int | None = None,
    ) -> tuple[ObjectBody, str, int]:
        """Download an inclusive byte range via a ranged ``GetObject``.

        Sends ``Range: bytes=start-end`` (``end`` inclusive; ``end=None`` reads
        to EOF). The total object size and satisfied range are parsed from the
        response's ``ContentRange`` header (e.g. ``bytes 0-499/1234``). An
        unsatisfiable range (``start`` beyond the object) raises a precondition
        error mirroring S3's ``InvalidRange`` / 416 response.

        :param bucket: Bucket name.
        :param key: Object key.
        :param start: First byte offset (inclusive, ``>= 0``).
        :param end: Last byte offset (inclusive), or ``None`` for EOF.
        :returns: ``(body, content_range, total_size)`` where *body* carries the
            range slice and its content type (metadata may be empty for ranges).
        """

        validate_range(start, end)
        c = self.__require_client()

        try:
            resp = await c.get_object(
                Bucket=bucket,
                Key=key,
                Range=build_range_header(start, end),
            )

        except c.exceptions.ClientError as e:  # type: ignore[attr-defined]
            code = (e.response or {}).get("Error", {}).get("Code")

            if code in {"InvalidRange", "416"}:
                total = _content_length_from_error(e)
                raise unsatisfiable_range(start, total) from e

            raise

        body = resp["Body"]
        data = await body.read()

        content_range = resp.get("ContentRange", "")
        parsed_total = _parse_total_from_content_range(content_range)

        # Unknown total: ContentRange absent (non-conforming backend) or an
        # ``.../*`` unknown-length form (S3-compatible gateways). Derive a
        # best-effort total from the satisfied range rather than returning 0.
        total = parsed_total if parsed_total is not None else (start + len(data))

        if not content_range:
            # S3 always returns ContentRange for a satisfied range; synthesize
            # defensively if a non-conforming backend omits it.
            end_byte = start + len(data) - 1 if data else start
            content_range = f"bytes {start}-{end_byte}/{total}"

        object_body = ObjectBody(
            data=data,
            content_type=resp.get("ContentType", "application/octet-stream"),
            metadata=resp.get("Metadata", {}),
        )

        return object_body, content_range, total

    # ....................... #

    @exc_interceptor.coroutine("s3.download_bytes_conditional")
    async def download_bytes_conditional(
        self,
        bucket: str,
        key: str,
        *,
        if_none_match: str | None = None,
        if_modified_since: datetime | None = None,
    ) -> ObjectBody | None:
        """Conditional ``GetObject`` returning ``None`` when not modified.

        Passes ``IfNoneMatch`` / ``IfModifiedSince``. When the object is
        unchanged S3 answers ``304 Not Modified`` (surfaced as a ``ClientError``
        with code ``304``/``NotModified``/``PreconditionFailed``), which maps to
        ``None``. Any other error propagates.

        :returns: an :class:`ObjectBody` (bytes + content type + user metadata,
            all from the same ``GET``) when changed, else ``None``.
        """

        c = self.__require_client()

        kwargs: dict[str, Any] = {"Bucket": bucket, "Key": key}

        if if_none_match is not None:
            kwargs["IfNoneMatch"] = if_none_match

        if if_modified_since is not None:
            kwargs["IfModifiedSince"] = if_modified_since

        try:
            resp = await c.get_object(**kwargs)

        except c.exceptions.ClientError as e:  # type: ignore[attr-defined]
            status = (
                (e.response or {}).get("ResponseMetadata", {}).get("HTTPStatusCode")
            )
            code = (e.response or {}).get("Error", {}).get("Code")

            if status == 304 or code in {"304", "NotModified", "PreconditionFailed"}:
                return None

            raise

        body = resp["Body"]
        data = await body.read()

        return ObjectBody(
            data=data,
            content_type=resp.get("ContentType", "application/octet-stream"),
            metadata=resp.get("Metadata", {}),
        )

    # ....................... #

    @exc_interceptor.coroutine("s3.copy_object")
    async def copy_object(
        self,
        bucket: str,
        src_key: str,
        dst_key: str,
        *,
        sse: ObjectStorageSSE | None = None,
    ) -> None:
        """Server-side copy within *bucket* via ``CopyObject``.

        Single-copy is capped at **5 GiB** by S3; objects larger than that need
        multipart copy (out of scope) and surface S3's ``InvalidRequest`` error.

        When *sse* requests SSE, ``CopyObject`` **re-encrypts** the destination
        with the supplied SSE params (the destination object is written
        encrypted at rest regardless of the source's encryption).

        :param bucket: Bucket name (same bucket for source and destination).
        :param src_key: Source object key.
        :param dst_key: Destination object key.
        :param sse: Optional server-side-encryption request for the destination.
        """

        c = self.__require_client()

        await c.copy_object(
            Bucket=bucket,
            Key=dst_key,
            CopySource={"Bucket": bucket, "Key": src_key},
            **cast(Any, _s3_sse_extra_args(sse)),
        )

    # ....................... #

    @exc_interceptor.coroutine("s3.put_object_tags")
    async def put_object_tags(
        self,
        bucket: str,
        key: str,
        tags: Mapping[str, str],
    ) -> None:
        """Replace an object's tag set via ``PutObjectTagging`` (full replace).

        :param bucket: Bucket name.
        :param key: Object key.
        :param tags: Complete tag set to store (empties the set when empty).
        """

        c = self.__require_client()

        tag_set = [{"Key": k, "Value": v} for k, v in tags.items()]

        await c.put_object_tagging(
            Bucket=bucket,
            Key=key,
            Tagging=cast(Any, {"TagSet": tag_set}),
        )

    # ....................... #

    @exc_interceptor.coroutine("s3.delete_object")
    async def delete_object(self, bucket: str, key: str) -> None:
        """Delete an object from the bucket.

        :param bucket: Bucket name.
        :param key: Object key to delete.
        """

        c = self.__require_client()

        await c.delete_object(Bucket=bucket, Key=key)

    # ....................... #

    @exc_interceptor.coroutine("s3.list_objects")
    async def list_objects(
        self,
        bucket: str,
        prefix: str | None = None,
        *,
        limit: int | None = None,
        offset: int | None = None,
        include_tags: bool = False,
    ) -> tuple[list[ObjectStorageListedObject], int]:
        """List objects in a bucket with optional pagination.

        Streams all pages from the ``list_objects_v2`` paginator and applies
        the requested offset/limit window in memory.

        ``include_tags`` is a guarantee, not a filter: ``ListObjectsV2`` never
        returns tags, so ``include_tags=True`` fans out one extra
        ``GetObjectTagging`` call **per listed object** (N extra calls for N
        items in the window), bounded by
        :data:`GET_OBJECT_TAGGING_CONCURRENCY` concurrent requests, and
        requires the ``s3:GetObjectTagging`` permission. A failing tagging
        call propagates through the normal error mapping (the caller asked
        for a guarantee).

        :param bucket: Bucket name.
        :param prefix: Key prefix filter.
        :param limit: Maximum number of objects to return.
        :param offset: Number of objects to skip before collecting results.
        :param include_tags: When ``True``, guarantee
            :attr:`~forze.application.integrations.storage.client.ObjectStorageListedObject.tags`
            is populated for every returned item (at the cost of N extra
            ``GetObjectTagging`` calls).
        :returns: A tuple of ``(items, total_count)`` where *total_count*
            reflects the full (unpaginated) result set.
        :raises exc.internal: If *limit* is non-positive or *offset* is negative.
        """

        c = self.__require_client()

        paginator = c.get_paginator("list_objects_v2")
        _prefix = prefix or ""

        _limit, _offset = normalize_list_window(limit, offset)

        items: list[ObjectStorageListedObject] = []
        total_count = 0

        # We will take objects in the requested window as we stream pages
        start = _offset
        end = _offset + _limit  # exclusive

        iterator = paginator.paginate(Bucket=bucket, Prefix=_prefix)
        collected_enough = False

        async for page in iterator:
            contents = page.get("Contents") or []
            if not contents:
                continue

            for obj in contents:
                idx = total_count
                total_count += 1

                if start <= idx < end:
                    key = obj.get("Key")

                    if not key:
                        raise exc.internal("Invalid object key")

                    items.append(ObjectStorageListedObject(key=key))

                    if len(items) >= _limit:
                        collected_enough = True

            if collected_enough and limit is not None:
                break

        if include_tags and items:
            items = await self.__attach_tags(c, bucket, items)

        return items, total_count

    # ....................... #

    @staticmethod
    async def __attach_tags(
        c: AsyncS3Client,
        bucket: str,
        items: list[ObjectStorageListedObject],
    ) -> list[ObjectStorageListedObject]:
        """Fan out ``GetObjectTagging`` per item with bounded concurrency.

        Concurrency is capped by :data:`GET_OBJECT_TAGGING_CONCURRENCY`. Any
        failure propagates (cancelling the remaining fetches) — the caller
        asked for the tag guarantee.
        """

        semaphore = asyncio.Semaphore(GET_OBJECT_TAGGING_CONCURRENCY)

        async def _tags_for(item: ObjectStorageListedObject) -> Mapping[str, str]:
            async with semaphore:
                resp = await c.get_object_tagging(Bucket=bucket, Key=item.key)

                return _decode_tag_set(resp)

        try:
            async with asyncio.TaskGroup() as tg:
                tasks = [tg.create_task(_tags_for(item)) for item in items]

        except BaseExceptionGroup as eg:
            # Unwrap so the original botocore error reaches the normal error
            # mapping (code-specific mapping instead of a generic fallback).
            raise eg.exceptions[0] from eg

        return [
            ObjectStorageListedObject(key=item.key, tags=task.result())
            for item, task in zip(items, tasks, strict=True)
        ]

    # ....................... #

    @exc_interceptor.coroutine("s3.head_object")
    async def head_object(
        self,
        bucket: str,
        key: str,
        *,
        include_tags: bool = False,
    ) -> ObjectStorageHead:
        """Retrieve object metadata without downloading the body.

        ``include_tags`` is a guarantee, not a filter: ``HeadObject`` never
        returns tags, so ``include_tags=True`` issues one extra
        ``GetObjectTagging`` call and requires the ``s3:GetObjectTagging``
        permission. A failing tagging call propagates through the normal
        error mapping (the caller asked for a guarantee). With the default
        ``False``, :attr:`ObjectStorageHead.tags` stays empty on S3.

        :param bucket: Bucket name.
        :param key: Object key.
        :param include_tags: When ``True``, guarantee
            :attr:`ObjectStorageHead.tags` is populated (one extra
            ``GetObjectTagging`` call).
        :returns: An :class:`S3Head` with content type, metadata, size, last
            modified timestamp, and ETag.
        """

        c = self.__require_client()
        head = await c.head_object(Bucket=bucket, Key=key)

        tags: Mapping[str, str] = {}

        if include_tags:
            resp = await c.get_object_tagging(Bucket=bucket, Key=key)
            tags = _decode_tag_set(resp)

        return ObjectStorageHead(
            content_type=head.get("ContentType", "application/octet-stream"),
            metadata=head.get("Metadata", {}),
            size=head.get("ContentLength", 0),
            last_modified=head.get("LastModified"),
            etag=head.get("ETag", "").strip('"'),
            tags=tags,
        )

    # ....................... #

    @exc_interceptor.coroutine("s3.presign_download_url")
    async def presign_download_url(
        self,
        bucket: str,
        key: str,
        *,
        expires_in: timedelta,
    ) -> PresignedUrl:
        """Sign a time-limited ``GET`` URL for the object (SigV4 query auth).

        Signing is **local** (``generate_presigned_url`` computes the
        signature in-process; no S3 round-trip) and does not check that the
        object exists. SigV4 caps ``expires_in`` at 7 days; with chain or
        temporary credentials (STS, instance roles) the effective lifetime is
        further bounded by the session token's expiry, whichever comes first.

        :param bucket: Bucket name.
        :param key: Object key.
        :param expires_in: URL lifetime (positive, at most 7 days).
        :raises CoreException: ``validation`` when *expires_in* is out of range.
        """

        c = self.__require_client()
        seconds = presign_expiry_seconds(expires_in, max_expiry=PRESIGN_MAX_EXPIRY)

        expires_at = utcnow() + timedelta(seconds=seconds)
        url = await c.generate_presigned_url(
            "get_object",
            Params={"Bucket": bucket, "Key": key},
            ExpiresIn=seconds,
        )

        return PresignedUrl(url=url, method="GET", expires_at=expires_at)

    # ....................... #

    @exc_interceptor.coroutine("s3.presign_upload_url")
    async def presign_upload_url(
        self,
        bucket: str,
        key: str,
        *,
        expires_in: timedelta,
        content_type: str | None = None,
        sse: ObjectStorageSSE | None = None,
    ) -> PresignedUrl:
        """Sign a time-limited ``PUT`` URL for the object (SigV4 query auth).

        Signing is **local** (no S3 round-trip). When *content_type* is given
        it is added to the signed parameters (``ContentType``), so SigV4 binds
        it — the returned :attr:`PresignedUrl.headers` then carries the
        ``Content-Type`` header the uploader must send verbatim. SigV4 caps
        ``expires_in`` at 7 days; with chain or temporary credentials (STS,
        instance roles) the effective lifetime is further bounded by the
        session token's expiry, whichever comes first.

        When *sse* requests SSE, the SSE headers are bound **into** the
        signature (via ``put_object`` ``ServerSideEncryption`` /
        ``SSEKMSKeyId`` params, which generate_presigned_url renders as the
        ``x-amz-server-side-encryption`` query/header binding) and echoed in
        :attr:`PresignedUrl.headers` so the uploader sends them verbatim.
        SSE-KMS **requires** the client to send those headers; SSE-S3 works off
        a bucket default but binding the header is portable and correct.

        :param bucket: Bucket name.
        :param key: Object key to upload to.
        :param expires_in: URL lifetime (positive, at most 7 days).
        :param content_type: Optional MIME type to bind into the signature.
        :param sse: Optional server-side-encryption request to bind into the
            signature and surface in the returned headers.
        :raises CoreException: ``validation`` when *expires_in* is out of range.
        """

        c = self.__require_client()
        seconds = presign_expiry_seconds(expires_in, max_expiry=PRESIGN_MAX_EXPIRY)

        params: dict[str, Any] = {"Bucket": bucket, "Key": key}
        headers: dict[str, str] = {}

        if content_type is not None:
            params["ContentType"] = content_type
            headers["Content-Type"] = content_type

        params |= _s3_sse_extra_args(sse)
        headers |= _s3_sse_request_headers(sse)

        expires_at = utcnow() + timedelta(seconds=seconds)
        url = await c.generate_presigned_url(
            "put_object",
            Params=params,
            ExpiresIn=seconds,
        )

        return PresignedUrl(
            url=url,
            method="PUT",
            expires_at=expires_at,
            headers=headers,
        )

    # ....................... #
    # Resumable multipart upload primitives.

    @exc_interceptor.coroutine("s3.create_multipart_upload")
    async def create_multipart_upload(
        self,
        bucket: str,
        key: str,
        *,
        content_type: str | None = None,
        sse: ObjectStorageSSE | None = None,
    ) -> str:
        """Open a native S3 multipart upload via ``CreateMultipartUpload``.

        When *sse* requests SSE it is set **here**, on the multipart create;
        all parts inherit the upload's encryption. The per-part presigned
        ``UploadPart`` URLs therefore do **not** repeat the SSE headers (see
        :meth:`presign_multipart_part`), so for SSE the part PresignedUrl's
        ``headers`` stay empty while encryption is still applied at rest.

        :returns: The S3 ``UploadId`` addressing the in-progress upload.
        """

        c = self.__require_client()

        kwargs: dict[str, Any] = {"Bucket": bucket, "Key": key}

        if content_type is not None:
            kwargs["ContentType"] = content_type

        kwargs.update(_s3_sse_extra_args(sse))

        resp = await c.create_multipart_upload(**kwargs)

        if upload_id := resp.get("UploadId"):
            return upload_id

        else:
            raise exc.internal("S3 CreateMultipartUpload returned no UploadId")

    # ....................... #

    @exc_interceptor.coroutine("s3.presign_multipart_part")
    async def presign_multipart_part(
        self,
        bucket: str,
        key: str,
        *,
        upload_id: str,
        part_number: int,
        expires_in: timedelta,
    ) -> PresignedUrl:
        """Sign a time-limited ``UploadPart`` ``PUT`` URL (SigV4 query auth).

        Signing is **local** (no S3 round-trip). The client ``PUT``\\ s the part
        bytes to this URL and reads the ``ETag`` from the response header, which
        the application carries into ``CompleteMultipartUpload``. SigV4 caps
        ``expires_in`` at 7 days.

        SSE is **not** bound here: S3 applies the upload's encryption (set on
        ``CreateMultipartUpload``) to every part automatically, and
        ``UploadPart`` rejects per-part SSE headers, so the returned
        :attr:`PresignedUrl.headers` carries no SSE header even on an SSE route.
        """

        c = self.__require_client()
        seconds = presign_expiry_seconds(expires_in, max_expiry=PRESIGN_MAX_EXPIRY)

        expires_at = utcnow() + timedelta(seconds=seconds)
        url = await c.generate_presigned_url(
            "upload_part",
            Params={
                "Bucket": bucket,
                "Key": key,
                "UploadId": upload_id,
                "PartNumber": part_number,
            },
            ExpiresIn=seconds,
        )

        return PresignedUrl(url=url, method="PUT", expires_at=expires_at)

    # ....................... #

    @exc_interceptor.coroutine("s3.list_multipart_parts")
    async def list_multipart_parts(
        self,
        bucket: str,
        key: str,
        *,
        upload_id: str,
    ) -> list[ObjectStoragePartInfo]:
        """List uploaded parts via the ``ListParts`` paginator (the resume primitive)."""

        c = self.__require_client()

        paginator = c.get_paginator("list_parts")
        iterator = paginator.paginate(Bucket=bucket, Key=key, UploadId=upload_id)

        parts: list[ObjectStoragePartInfo] = []

        async for page in iterator:
            for entry in page.get("Parts") or []:
                number = entry.get("PartNumber")

                if number is None:
                    continue

                parts.append(
                    ObjectStoragePartInfo(
                        part_number=int(number),
                        etag=str(entry.get("ETag", "")).strip('"'),
                        size=int(entry.get("Size", 0) or 0),
                    )
                )

        parts.sort(key=lambda p: p.part_number)

        return parts

    # ....................... #

    @exc_interceptor.coroutine("s3.complete_multipart_upload")
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
        """Assemble the parts via ``CompleteMultipartUpload``.

        Requires the ``{PartNumber, ETag}`` list in ascending part order; the
        ETags come from the clients' part ``PUT`` responses (carried back by the
        application). ETags are sent quoted, as S3 expects.

        *content_type* and *sse* are ignored here: both bind on
        ``CreateMultipartUpload`` (see :meth:`create_multipart_upload`) and the
        completed object inherits them; ``CompleteMultipartUpload`` takes no
        such params. Accepted for port symmetry (GCS consumes them on
        ``compose``, having no native session).
        """

        # S3 inherits content type and SSE from CreateMultipartUpload; neither
        # is settable on complete.
        _ = (content_type, sse)

        c = self.__require_client()

        ordered = sorted(parts, key=lambda p: p.part_number)

        completed = [
            {
                "PartNumber": p.part_number,
                "ETag": _quote_etag(p.etag),
            }
            for p in ordered
        ]

        await c.complete_multipart_upload(
            Bucket=bucket,
            Key=key,
            UploadId=upload_id,
            MultipartUpload=cast(Any, {"Parts": completed}),
        )

    # ....................... #

    @exc_interceptor.coroutine("s3.abort_multipart_upload")
    async def abort_multipart_upload(
        self,
        bucket: str,
        key: str,
        *,
        upload_id: str,
    ) -> None:
        """Abort the in-progress upload via ``AbortMultipartUpload``."""

        c = self.__require_client()

        await c.abort_multipart_upload(
            Bucket=bucket,
            Key=key,
            UploadId=upload_id,
        )


# ....................... #


def _s3_sse_extra_args(sse: ObjectStorageSSE | None) -> dict[str, str]:
    """Map a neutral SSE descriptor to S3 request params (``ExtraArgs``/kwargs).

    Returns ``{}`` for ``None`` / ``mode == "none"`` (no SSE requested). ``"s3"``
    yields ``ServerSideEncryption=AES256``; ``"kms"`` yields
    ``ServerSideEncryption=aws:kms`` plus ``SSEKMSKeyId``. These keys are shared
    by ``PutObject``/``upload_fileobj``, ``CopyObject``,
    ``CreateMultipartUpload``, and ``generate_presigned_url`` ``Params``.
    """

    if sse is None or sse.mode == "none":
        return {}

    if sse.mode == "s3":
        return {"ServerSideEncryption": "AES256"}

    # mode == "kms"
    args: dict[str, str] = {"ServerSideEncryption": "aws:kms"}

    if sse.key_id:
        args["SSEKMSKeyId"] = sse.key_id

    return args


# ....................... #


def _s3_sse_request_headers(sse: ObjectStorageSSE | None) -> dict[str, str]:
    """Map a neutral SSE descriptor to the request headers a presigned ``PUT``
    must send verbatim.

    Mirrors :func:`_s3_sse_extra_args` as HTTP headers
    (``x-amz-server-side-encryption`` [+ ``...-aws-kms-key-id``]). Binding these
    into the signature (via the ``put_object`` SSE ``Params``) requires the
    uploader to send them; SSE-KMS is rejected without them, SSE-S3 tolerates
    them (and they make the URL portable across buckets without a default).
    """

    if sse is None or sse.mode == "none":
        return {}

    if sse.mode == "s3":
        return {"x-amz-server-side-encryption": "AES256"}

    # mode == "kms"
    headers = {"x-amz-server-side-encryption": "aws:kms"}

    if sse.key_id:
        headers["x-amz-server-side-encryption-aws-kms-key-id"] = sse.key_id

    return headers


# ....................... #


def _quote_etag(etag: str) -> str:
    """Normalize a part ETag to exactly one pair of surrounding quotes.

    ``CompleteMultipartUpload`` requires quoted part ETags. Trims surrounding
    whitespace and avoids double-wrapping an already-quoted value (the old
    ``startswith('"')``-only check mangled a whitespace-padded ETag into
    ``" "abc""``).
    """

    etag = etag.strip()
    if etag.startswith('"') and etag.endswith('"') and len(etag) >= 2:
        return etag
    return f'"{etag}"'


# ....................... #


def _parse_total_from_content_range(content_range: str) -> int | None:
    """Parse the total object size out of a ``bytes start-end/total`` header.

    Returns ``None`` when the total is unknown (``bytes start-end/*``) or the
    header is absent/non-conforming, so the caller can synthesize a best-effort
    total instead of mistaking it for an explicit ``0`` (an empty object).
    """

    if not content_range or "/" not in content_range:
        return None

    total_part = content_range.rsplit("/", 1)[-1].strip()

    return int(total_part) if total_part.isdigit() else None


# ....................... #


def _content_length_from_error(e: Any) -> int:
    """Best-effort object size from an ``InvalidRange`` error response (0 if absent)."""

    resp = cast(JsonDict, getattr(e, "response", None) or {})
    actual = resp.get("Error", {}).get("ActualObjectSize")

    if isinstance(actual, str) and actual.isdigit():
        return int(actual)

    return actual if isinstance(actual, int) else 0


# ....................... #


def _decode_tag_set(resp: Mapping[str, Any]) -> dict[str, str]:
    """Decode a ``GetObjectTagging`` response ``TagSet`` into a mapping."""

    tags: dict[str, str] = {}

    tag_set = cast(list[dict[str, str]], resp.get("TagSet") or [])

    for entry in tag_set:
        if not isinstance(
            entry, MappingABC
        ):  # pyright: ignore[reportUnnecessaryIsInstance]
            continue

        tag_key = entry.get("Key")
        tag_value = entry.get("Value")

        if isinstance(tag_key, str) and isinstance(tag_value, str):
            tags[tag_key] = tag_value

    return tags
