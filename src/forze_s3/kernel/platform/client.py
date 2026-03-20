from forze_s3._compat import require_s3

require_s3()

# ....................... #

import io
from contextlib import asynccontextmanager
from contextvars import ContextVar
from datetime import datetime
from typing import Any, AsyncIterator, TypedDict, cast, final

import aioboto3
import attrs
from botocore.config import Config as AioConfig
from pydantic import SecretStr
from types_aiobotocore_s3.client import S3Client as AsyncS3Client
from types_aiobotocore_s3.type_defs import ObjectTypeDef

from forze.base.errors import CoreError, NotFoundError

from .errors import s3_handled

# ----------------------- #


@final
class S3Config(TypedDict, total=False):
    """S3 optional configuration (botocore config)."""

    region_name: str
    signature_version: str
    user_agent: str
    user_agent_extra: str
    connect_timeout: int | float  #! TODO: use timedelta
    read_timeout: int | float  #! TODO: use timedelta
    parameter_validation: bool
    max_pool_connections: int
    proxies: dict[str, str]
    proxies_config: dict[str, Any]
    s3: dict[str, Any]
    retries: dict[str, Any]
    client_cert: str | tuple[str, str]
    inject_host_prefix: bool
    use_dualstack_endpoint: bool
    use_fips_endpoint: bool
    ignore_configured_endpoint_urls: bool
    tcp_keepalive: bool
    request_min_compression_size_bytes: int


# ....................... #


@final
class S3Head(TypedDict, total=False):
    """Metadata returned by an S3 ``HeadObject`` call."""

    content_type: str
    """MIME type of the object."""

    metadata: dict[str, str]
    """User-defined metadata key-value pairs."""

    size: int
    """Content length in bytes."""

    last_modified: datetime
    """Timestamp of the last modification."""

    etag: str
    """Entity tag with surrounding quotes stripped."""


# ....................... #


@final
@attrs.define(frozen=True, slots=True, kw_only=True)
class _S3ConnectionOpts:
    """S3 connection options."""

    endpoint: str
    access_key_id: str
    secret_access_key: str | SecretStr
    config: AioConfig | None = None


# ....................... #


@final
@attrs.define(slots=True)
class S3Client:
    """Async S3 client with context-scoped connection reuse.

    Must be :meth:`initialize`d with endpoint credentials before use. The
    :meth:`client` context manager creates an ``aioboto3`` S3 client for the
    current context; nested entries reuse the same client via context variables.
    """

    __opts: _S3ConnectionOpts | None = attrs.field(default=None, init=False)
    __session: aioboto3.Session | None = attrs.field(default=None, init=False)

    __ctx_client: ContextVar[AsyncS3Client | None] = attrs.field(
        factory=lambda: ContextVar("s3_client", default=None),
        init=False,
    )
    __ctx_depth: ContextVar[int] = attrs.field(
        factory=lambda: ContextVar("s3_depth", default=0),
        init=False,
    )

    # ....................... #
    # Lifecycle

    async def initialize(
        self,
        endpoint: str,
        access_key_id: str,
        secret_access_key: str | SecretStr,
        config: S3Config | None = None,
    ) -> None:
        """Configure the client with S3 credentials and create a session.

        No-ops if the session is already initialized. When no ``retries``
        key is present in *config*, a default adaptive retry strategy with
        up to 3 attempts is applied automatically.

        :param endpoint: S3-compatible endpoint URL.
        :param access_key_id: AWS access key identifier.
        :param secret_access_key: AWS secret access key (plain or :class:`SecretStr`).
        :param config: Optional botocore configuration overrides.
        """

        if self.__session is not None:
            return

        if config is None:
            config = S3Config(retries={"max_attempts": 3, "mode": "adaptive"})
        elif "retries" not in config:
            config = {**config, "retries": {"max_attempts": 3, "mode": "adaptive"}}

        aio_config = AioConfig(**config)  # type: ignore

        self.__opts = _S3ConnectionOpts(
            endpoint=endpoint,
            access_key_id=access_key_id,
            secret_access_key=secret_access_key,
            config=aio_config,
        )
        self.__session = aioboto3.Session()

    # ....................... #

    def close(self) -> None:
        """Release the session and connection options."""

        self.__session = None
        self.__opts = None

    # ....................... #

    def __require_session(self) -> aioboto3.Session:
        if self.__session is None:
            raise CoreError("S3 session is not initialized")

        return self.__session

    # ....................... #

    def __current_client(self) -> AsyncS3Client | None:
        return self.__ctx_client.get()

    # ....................... #

    def __require_client(self) -> AsyncS3Client:
        c = self.__current_client()

        if c is None:
            raise CoreError("S3 client is not initialized")

        return c

    # ....................... #

    @asynccontextmanager
    async def client(self) -> AsyncIterator[AsyncS3Client]:
        """Yield a context-scoped S3 client.

        On first entry a new ``aioboto3`` client is created; nested calls reuse
        the existing client and increment a depth counter so the underlying
        connection is only closed when the outermost context exits.
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

        session = self.__require_session()
        opts = self.__opts

        if opts is None:
            raise CoreError("S3 client options are not initialized")

        sec_key = opts.secret_access_key

        if isinstance(sec_key, SecretStr):
            sec_key = sec_key.get_secret_value()

        cm = session.client(  # type: ignore
            "s3",
            endpoint_url=opts.endpoint,
            aws_access_key_id=opts.access_key_id,
            aws_secret_access_key=sec_key,
            config=opts.config,  # type: ignore
        )
        cm = cast(AsyncS3Client, cm)

        async with cm as c:
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

    @s3_handled("s3.bucket_exists")  # type: ignore[untyped-decorator]
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

    @s3_handled("s3.create_bucket")  # type: ignore[untyped-decorator]
    async def create_bucket(self, bucket: str) -> None:
        """Create a bucket, silently succeeding if it already exists.

        :param bucket: Bucket name to create.
        """

        c = self.__require_client()

        try:
            await c.create_bucket(Bucket=bucket)

        except c.exceptions.ClientError as e:  # type: ignore[attr-defined]
            code = (e.response or {}).get("Error", {}).get("Code")

            if code in {"409", "BucketAlreadyExists", "BucketAlreadyOwnedByYou"}:
                return

            raise

    # ....................... #

    @s3_handled("s3.ensure_bucket")  # type: ignore[untyped-decorator]
    async def ensure_bucket(self, bucket: str) -> None:
        """Assert that the bucket exists.

        :param bucket: Bucket name to verify.
        :raises NotFoundError: If the bucket does not exist.
        """

        if not await self.bucket_exists(bucket):
            raise NotFoundError("Bucket does not exist")

    # ....................... #

    @s3_handled("s3.object_exists")  # type: ignore[untyped-decorator]
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

    @s3_handled("s3.upload_bytes")  # type: ignore[untyped-decorator]
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
            extra["Tagging"] = "&".join(f"{k}={v}" for k, v in tags.items())

        fileobj = io.BytesIO(data)

        if extra:
            await c.upload_fileobj(fileobj, Bucket=bucket, Key=key, ExtraArgs=extra)

        else:
            await c.upload_fileobj(fileobj, Bucket=bucket, Key=key)

    # ....................... #

    @s3_handled("s3.download_bytes")  # type: ignore[untyped-decorator]
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

    @s3_handled("s3.delete_object")  # type: ignore[untyped-decorator]
    async def delete_object(self, bucket: str, key: str) -> None:
        """Delete an object from the bucket.

        :param bucket: Bucket name.
        :param key: Object key to delete.
        """

        c = self.__require_client()

        await c.delete_object(Bucket=bucket, Key=key)

    # ....................... #

    @s3_handled("s3.list_objects")  # type: ignore[untyped-decorator]
    async def list_objects(
        self,
        bucket: str,
        prefix: str | None = None,
        *,
        limit: int | None = None,
        offset: int | None = None,
    ) -> tuple[list[ObjectTypeDef], int]:
        """List objects in a bucket with optional pagination.

        Streams all pages from the ``list_objects_v2`` paginator and applies
        the requested offset/limit window in memory.

        :param bucket: Bucket name.
        :param prefix: Key prefix filter.
        :param limit: Maximum number of objects to return.
        :param offset: Number of objects to skip before collecting results.
        :returns: A tuple of ``(items, total_count)`` where *total_count*
            reflects the full (unpaginated) result set.
        :raises CoreError: If *limit* is non-positive or *offset* is negative.
        """

        c = self.__require_client()

        paginator = c.get_paginator("list_objects_v2")
        _prefix = prefix or ""

        if limit is not None and limit <= 0:
            raise CoreError("limit must be > 0")  # Validation ?

        if offset is not None and offset < 0:
            raise CoreError("offset must be >= 0")  # Validation ?

        # Defaults
        _limit = limit if limit is not None else 10_000_000  # effectively "no limit"
        _offset = offset or 0

        items: list[ObjectTypeDef] = []
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
                    items.append(obj)
                    if len(items) >= _limit:
                        collected_enough = True

            if collected_enough and limit is not None:
                break

        return items, total_count

    # ....................... #

    @s3_handled("s3.head_object")  # type: ignore[untyped-decorator]
    async def head_object(self, bucket: str, key: str) -> S3Head:
        """Retrieve object metadata without downloading the body.

        :param bucket: Bucket name.
        :param key: Object key.
        :returns: An :class:`S3Head` with content type, metadata, size, last
            modified timestamp, and ETag.
        """

        c = self.__require_client()
        head = await c.head_object(Bucket=bucket, Key=key)

        return {
            "content_type": head.get("ContentType", "application/octet-stream"),
            "metadata": head.get("Metadata", {}),
            "size": head.get("ContentLength", 0),
            "last_modified": head.get("LastModified"),
            "etag": head.get("ETag", "").strip('"'),
        }
