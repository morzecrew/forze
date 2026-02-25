from forze_redis._compat import require_redis

require_redis()

# ....................... #

from contextlib import asynccontextmanager
from contextvars import ContextVar
from typing import AsyncIterator, Mapping, Optional, Sequence, final

import attrs
from redis.asyncio.client import Pipeline, Redis
from redis.asyncio.connection import ConnectionPool

from forze.base.errors import InfrastructureError
from forze.base.primitives import JsonDict

from .errors import redis_handled
from .types import StreamMessage
from .utils import parse_stream_messages

# ----------------------- #
#! TODO: declare timeouts etc for redis config


@final
@attrs.define(frozen=True, slots=True, kw_only=True)
class RedisConfig:
    """Redis configuration."""

    max_size: int = 20


# ....................... #


@final
@attrs.define(slots=True)
class RedisClient:
    __pool: Optional[ConnectionPool] = attrs.field(default=None, init=False)
    __client: Optional[Redis] = attrs.field(default=None, init=False)

    __ctx_pipe: ContextVar[Optional[Pipeline]] = attrs.field(
        factory=lambda: ContextVar("redis_pipe", default=None),
        init=False,
    )
    __ctx_depth: ContextVar[int] = attrs.field(
        factory=lambda: ContextVar("redis_pipe_depth", default=0),
        init=False,
    )

    # ....................... #
    # Lifecycle

    async def initialize(
        self, dsn: str, *, config: RedisConfig = RedisConfig()
    ) -> None:
        if self.__client is not None:
            return

        self.__pool = (
            ConnectionPool.from_url(  # pyright: ignore[reportUnknownMemberType]
                dsn,
                max_connections=config.max_size,
                decode_responses=False,
                encoding="utf-8",
            )
        )
        self.__client = Redis(connection_pool=self.__pool)

        await self.__client.ping()  # pyright: ignore[reportUnknownMemberType, reportGeneralTypeIssues]

    # ....................... #

    async def close(self) -> None:
        if self.__client is not None:
            await self.__client.aclose()
            self.__client = None

        if self.__pool is not None:
            await self.__pool.disconnect(inuse_connections=True)
            self.__pool = None

    # ....................... #

    def __require_client(self) -> Redis:
        if self.__client is None:
            raise InfrastructureError("Redis client is not initialized")

        return self.__client

    # ....................... #

    async def health(self) -> tuple[str, bool]:
        try:
            ok = (  # pyright: ignore[reportUnknownVariableType]
                await self.__require_client().ping()  # pyright: ignore[reportUnknownMemberType, reportGeneralTypeIssues]
            )
            return "ok", bool(ok)  # pyright: ignore[reportUnknownArgumentType]

        except Exception as e:
            return str(e), False

    # ....................... #
    # Context helpers

    def __current_pipe(self) -> Optional[Pipeline]:
        return self.__ctx_pipe.get()

    # ....................... #

    def __executor(self) -> Redis | Pipeline:
        return self.__current_pipe() or self.__require_client()

    # ....................... #
    # Pipeline API

    @redis_handled("redis.pipeline")
    @asynccontextmanager
    async def pipeline(self, *, transaction: bool = True) -> AsyncIterator[Pipeline]:
        depth = self.__ctx_depth.get()
        parent = self.__current_pipe()

        if depth > 0 and parent is not None:
            self.__ctx_depth.set(depth + 1)

            try:
                yield parent

            finally:
                self.__ctx_depth.set(depth)

            return

        pipe = self.__require_client().pipeline(transaction=transaction)
        token_pipe = self.__ctx_pipe.set(pipe)
        token_depth = self.__ctx_depth.set(1)

        try:
            yield pipe
            await pipe.execute()

        finally:
            self.__ctx_pipe.reset(token_pipe)
            self.__ctx_depth.reset(token_depth)
            await pipe.reset()

    # ....................... #
    # Canonical methods

    @redis_handled("redis.get")
    async def get(self, key: str) -> Optional[bytes | str]:
        return await self.__executor().get(key)

    # ....................... #

    @redis_handled("redis.mget")
    async def mget(self, keys: Sequence[str]) -> list[Optional[bytes | str]]:
        return await self.__executor().mget(*keys)

    # ....................... #

    @redis_handled("redis.set")
    async def set(
        self,
        key: str,
        value: bytes | str,
        *,
        ex: Optional[int] = None,
        px: Optional[int] = None,
        nx: bool = False,
        xx: bool = False,
    ) -> bool:
        res = await self.__executor().set(key, value, ex=ex, px=px, nx=nx, xx=xx)

        return bool(res)

    # ....................... #

    @redis_handled("redis.mset")
    async def mset(
        self,
        mapping: Mapping[str, bytes | str],
        *,
        ex: Optional[int] = None,
        px: Optional[int] = None,
        nx: bool = False,
        xx: bool = False,
    ) -> bool:
        if not mapping:
            return True

        async with self.pipeline(transaction=True) as pipe:
            for key, value in mapping.items():
                await pipe.set(key, value, ex=ex, px=px, nx=nx, xx=xx)

        return True

    # ....................... #

    @redis_handled("redis.delete")
    async def delete(self, *keys: str) -> int:
        if not keys:
            return 0

        res = await self.__executor().delete(*keys)

        return int(res)

    # ....................... #

    @redis_handled("redis.unlink")
    async def unlink(self, *keys: str) -> int:
        if not keys:
            return 0

        res = await self.__executor().unlink(*keys)

        return int(res)

    # ....................... #

    @redis_handled("redis.expire")
    async def expire(self, key: str, seconds: int) -> bool:
        res = await self.__executor().expire(key, seconds)

        return bool(res)

    # ....................... #
    # Counter methods

    @redis_handled("redis.incr")
    async def incr(self, key: str, by: int = 1) -> int:
        res = await self.__executor().incrby(key, by)

        return int(res)

    # ....................... #

    @redis_handled("redis.decr")
    async def decr(self, key: str, by: int = 1) -> int:
        res = await self.__executor().decrby(key, by)

        return int(res)

    # ....................... #

    @redis_handled("redis.reset")
    async def reset(self, key: str, value: int) -> int:
        res = await self.__executor().getset(key, value)

        return int(res or 0)

    # ....................... #
    # Stream methods

    #! TODO: refactor and fix types, the same for stream gateway (make is simpler)

    @redis_handled("redis.xadd")
    async def xadd(
        self,
        stream: str,
        data: JsonDict,
        *,
        id: str = "*",
        maxlen: Optional[int] = None,
        approx: bool = True,
        nomkstream: bool = False,
        minid: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> str:
        res = await self.__executor().xadd(
            stream,
            data,  # type: ignore[reportUnknownArgumentType]
            id=id,
            maxlen=maxlen,
            approximate=approx,
            nomkstream=nomkstream,
            minid=minid,
            limit=limit,
        )

        if isinstance(res, bytes):
            return res.decode("utf-8")

        return str(res)  # type: ignore[reportUnknownReturnType]

    # ....................... #

    @redis_handled("redis.xread")
    async def xread(
        self,
        streams: dict[str, str],
        *,
        count: Optional[int] = None,
        block_ms: Optional[int] = None,
    ) -> list[tuple[str, list[StreamMessage]]]:
        res = await self.__executor().xread(
            streams=streams,  # type: ignore[reportUnknownMemberType]
            count=count,
            block=block_ms,
        )

        return parse_stream_messages(res)  #! too much as for low-level client

    # ....................... #

    @redis_handled("redis.xdel")
    async def xdel(self, stream: str, ids: Sequence[str]) -> int:
        if not ids:
            return 0

        res = await self.__executor().xdel(stream, *ids)

        return int(res)

    # ....................... #

    @redis_handled("redis.xtrim_maxlen")
    async def xtrim_maxlen(
        self,
        stream: str,
        maxlen: int,
        *,
        approx: bool = True,
        limit: Optional[int] = None,
    ) -> int:
        res = await self.__executor().xtrim(
            stream,
            maxlen=maxlen,
            approximate=approx,
            limit=limit,
        )

        return int(res)

    # ....................... #

    @redis_handled("redis.xtrim_minid")
    async def xtrim_minid(
        self,
        stream: str,
        minid: str,
        *,
        approx: bool = True,
        limit: Optional[int] = None,
    ) -> int:
        res = await self.__executor().xtrim(
            stream,
            minid=minid,
            approximate=approx,
            limit=limit,
        )

        return int(res)

    # ....................... #

    @redis_handled("redis.xgroup_create")
    async def xgroup_create(
        self,
        stream: str,
        group: str,
        *,
        id: str = "0-0",
        mkstream: bool = True,
    ) -> bool:
        res = await self.__executor().xgroup_create(
            stream,
            group,
            id=id,
            mkstream=mkstream,
        )

        return bool(res)

    # ....................... #

    @redis_handled("redis.xgroup_read")
    async def xgroup_read(
        self,
        group: str,
        consumer: str,
        streams: dict[str, str],
        *,
        count: Optional[int] = None,
        block_ms: Optional[int] = None,
        noack: bool = False,
    ) -> list[tuple[str, list[StreamMessage]]]:
        res = await self.__executor().xreadgroup(
            group,
            consumer,
            streams,  # type: ignore[reportUnknownArgumentType]
            count=count,
            block=block_ms,
            noack=noack,
        )

        return parse_stream_messages(res)  #! too much as for low-level client

    # ....................... #

    @redis_handled("redis.xack")
    async def xack(self, stream: str, group: str, ids: Sequence[str]) -> int:
        if not ids:
            return 0

        res = await self.__executor().xack(stream, group, *ids)

        return int(res)
