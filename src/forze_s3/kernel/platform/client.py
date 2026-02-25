from forze_s3._compat import require_s3

require_s3()

# ....................... #

import io
from contextlib import asynccontextmanager
from contextvars import ContextVar
from datetime import datetime
from typing import Any, AsyncIterator, Optional, TypedDict, cast, final

import aioboto3
import attrs
from botocore.config import Config as AioConfig
from pydantic import SecretStr
from types_aiobotocore_s3.client import S3Client as AsyncS3Client
from types_aiobotocore_s3.type_defs import ObjectTypeDef

from forze.base.errors import CoreError

from .errors import s3_handled

# ----------------------- #
#! TODO: abstract config class with attrs or typed dict


@final
class S3Head(TypedDict, total=False):
    content_type: str
    metadata: dict[str, str]
    size: int
    last_modified: datetime
    etag: str


# ....................... #


@final
@attrs.define(frozen=True, slots=True, kw_only=True)
class S3Config:
    """S3 configuration."""

    endpoint: str
    access_key_id: str
    secret_access_key: str | SecretStr
    config: Optional[AioConfig] = None


# ....................... #


@final
@attrs.define(slots=True)
class S3Client:
    __opts: Optional[S3Config] = attrs.field(default=None, init=False)
    __session: Optional[aioboto3.Session] = attrs.field(default=None, init=False)

    __ctx_client: ContextVar[Optional[AsyncS3Client]] = attrs.field(
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
        config: Optional[AioConfig] = None,
    ) -> None:
        if self.__session is not None:
            return

        self.__opts = S3Config(
            endpoint=endpoint,
            access_key_id=access_key_id,
            secret_access_key=secret_access_key,
            config=config,
        )
        self.__session = aioboto3.Session()

    # ....................... #

    def close(self) -> None:
        self.__session = None
        self.__opts = None

    # ....................... #

    def __require_session(self) -> aioboto3.Session:
        if self.__session is None:
            raise CoreError("S3 session is not initialized")

        return self.__session

    # ....................... #

    def __current_client(self) -> Optional[AsyncS3Client]:
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
        try:
            async with self.client() as c:
                await c.list_buckets()

            return "ok", True

        except Exception as e:
            return str(e), False

    # ....................... #

    @s3_handled("s3.bucket_exists")
    async def bucket_exists(self, bucket: str) -> bool:
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

    @s3_handled("s3.create_bucket")
    async def create_bucket(self, bucket: str) -> None:
        c = self.__require_client()

        try:
            await c.create_bucket(Bucket=bucket)

        except c.exceptions.ClientError as e:  # type: ignore[attr-defined]
            code = (e.response or {}).get("Error", {}).get("Code")

            if code in {"409", "BucketAlreadyExists", "BucketAlreadyOwnedByYou"}:
                return

            raise

    # ....................... #

    @s3_handled("s3.ensure_bucket")
    async def ensure_bucket(self, bucket: str) -> None:
        if await self.bucket_exists(bucket):
            return

        await self.create_bucket(bucket)

    # ....................... #

    @s3_handled("s3.object_exists")
    async def object_exists(self, bucket: str, key: str) -> bool:
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

    @s3_handled("s3.upload_bytes")
    async def upload_bytes(
        self,
        bucket: str,
        key: str,
        data: bytes,
        *,
        content_type: Optional[str] = None,
        metadata: Optional[dict[str, str]] = None,
        tags: Optional[dict[str, str]] = None,
    ) -> None:
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

    @s3_handled("s3.download_bytes")
    async def download_bytes(self, bucket: str, key: str) -> bytes:
        c = self.__require_client()

        resp = await c.get_object(Bucket=bucket, Key=key)
        body = resp["Body"]

        return await body.read()

    # ....................... #

    @s3_handled("s3.delete_object")
    async def delete_object(self, bucket: str, key: str) -> None:
        c = self.__require_client()

        await c.delete_object(Bucket=bucket, Key=key)

    # ....................... #

    @s3_handled("s3.list_objects")
    async def list_objects(
        self,
        bucket: str,
        prefix: Optional[str] = None,
        *,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> tuple[list[ObjectTypeDef], int]:
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

        async for page in iterator:
            contents = page.get("Contents") or []
            if not contents:
                continue

            for obj in contents:
                idx = total_count  # current object's index in the full sequence
                total_count += 1

                if start <= idx < end:
                    items.append(obj)

        return items, total_count

    # ....................... #

    @s3_handled("s3.head_object")
    async def head_object(self, bucket: str, key: str) -> S3Head:
        c = self.__require_client()
        head = await c.head_object(Bucket=bucket, Key=key)

        return {
            "content_type": head.get("ContentType", "application/octet-stream"),
            "metadata": head.get("Metadata", {}),
            "size": head.get("ContentLength", 0),
            "last_modified": head.get("LastModified"),
            "etag": head.get("ETag", "").strip('"'),
        }
