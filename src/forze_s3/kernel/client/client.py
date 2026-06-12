from forze_s3._compat import require_s3

require_s3()

# ....................... #

import asyncio
import io
from collections.abc import Mapping as MappingABC
from contextlib import AsyncExitStack, asynccontextmanager
from contextvars import ContextVar
from datetime import timedelta
from typing import Any, AsyncContextManager, AsyncGenerator, Final, Mapping, cast, final
from urllib.parse import urlencode

import aioboto3
import attrs
from pydantic import SecretStr
from types_aiobotocore_s3.client import S3Client as AsyncS3Client

from forze.application.contracts.storage import PresignedUrl
from forze.application.integrations.storage.client import (
    PRESIGN_MAX_EXPIRY,
    ObjectStorageHead,
    ObjectStorageListedObject,
    normalize_list_window,
    presign_expiry_seconds,
)
from forze.base.exceptions import exc
from forze.base.primitives import utcnow

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

        return cast(AsyncContextManager[AsyncS3Client], cm)

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

    @exc_interceptor.coroutine("s3.bucket_exists")  # type: ignore[untyped-decorator]
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

    @exc_interceptor.coroutine("s3.create_bucket")  # type: ignore[untyped-decorator]
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

    @exc_interceptor.coroutine("s3.ensure_bucket")  # type: ignore[untyped-decorator]
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

    @exc_interceptor.coroutine("s3.object_exists")  # type: ignore[untyped-decorator]
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

    @exc_interceptor.coroutine("s3.upload_bytes")  # type: ignore[untyped-decorator]
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
        """Upload raw bytes to an S3 object.

        :param bucket: Target bucket name.
        :param key: Object key.
        :param data: Raw bytes to upload.
        :param content_type: Optional MIME type.
        :param metadata: Optional user-defined metadata.
        :param tags: Optional object tags, encoded as URL query parameters.
        """

        c = self.__require_client()

        extra: dict[str, Any] = {}

        if content_type is not None:
            extra["ContentType"] = content_type

        if metadata is not None:
            extra["Metadata"] = metadata

        if tags:
            extra["Tagging"] = urlencode(tags)

        fileobj = io.BytesIO(data)

        if extra:
            await c.upload_fileobj(fileobj, Bucket=bucket, Key=key, ExtraArgs=extra)

        else:
            await c.upload_fileobj(fileobj, Bucket=bucket, Key=key)

    # ....................... #

    @exc_interceptor.coroutine("s3.download_bytes")  # type: ignore[untyped-decorator]
    async def download_bytes(self, bucket: str, key: str) -> bytes:
        """Download the full content of an S3 object as bytes.

        :param bucket: Bucket name.
        :param key: Object key.
        :returns: Raw object bytes.
        """

        c = self.__require_client()

        resp = await c.get_object(Bucket=bucket, Key=key)
        body = resp["Body"]

        return await body.read()

    # ....................... #

    @exc_interceptor.coroutine("s3.delete_object")  # type: ignore[untyped-decorator]
    async def delete_object(self, bucket: str, key: str) -> None:
        """Delete an object from the bucket.

        :param bucket: Bucket name.
        :param key: Object key to delete.
        """

        c = self.__require_client()

        await c.delete_object(Bucket=bucket, Key=key)

    # ....................... #

    @exc_interceptor.coroutine("s3.list_objects")  # type: ignore[untyped-decorator]
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

    async def __attach_tags(
        self,
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

    @exc_interceptor.coroutine("s3.head_object")  # type: ignore[untyped-decorator]
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

    @exc_interceptor.coroutine("s3.presign_download_url")  # type: ignore[untyped-decorator]
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

    @exc_interceptor.coroutine("s3.presign_upload_url")  # type: ignore[untyped-decorator]
    async def presign_upload_url(
        self,
        bucket: str,
        key: str,
        *,
        expires_in: timedelta,
        content_type: str | None = None,
    ) -> PresignedUrl:
        """Sign a time-limited ``PUT`` URL for the object (SigV4 query auth).

        Signing is **local** (no S3 round-trip). When *content_type* is given
        it is added to the signed parameters (``ContentType``), so SigV4 binds
        it — the returned :attr:`PresignedUrl.headers` then carries the
        ``Content-Type`` header the uploader must send verbatim. SigV4 caps
        ``expires_in`` at 7 days; with chain or temporary credentials (STS,
        instance roles) the effective lifetime is further bounded by the
        session token's expiry, whichever comes first.

        :param bucket: Bucket name.
        :param key: Object key to upload to.
        :param expires_in: URL lifetime (positive, at most 7 days).
        :param content_type: Optional MIME type to bind into the signature.
        :raises CoreException: ``validation`` when *expires_in* is out of range.
        """

        c = self.__require_client()
        seconds = presign_expiry_seconds(expires_in, max_expiry=PRESIGN_MAX_EXPIRY)

        params: dict[str, Any] = {"Bucket": bucket, "Key": key}
        headers: dict[str, str] = {}

        if content_type is not None:
            params["ContentType"] = content_type
            headers["Content-Type"] = content_type

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
