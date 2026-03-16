from forze_redis._compat import require_redis

require_redis()

# ....................... #

from contextlib import asynccontextmanager
from contextvars import ContextVar
from datetime import timedelta
from typing import AsyncIterator, Mapping, Optional, Sequence, final

import attrs
from redis.asyncio.client import Pipeline, Redis
from redis.asyncio.connection import ConnectionPool

from forze.base.errors import InfrastructureError
from forze.base.logging_v2 import getLogger
from forze.base.primitives import JsonDict

from .errors import redis_handled
from .types import RedisPubSubMessage, RedisStreamResponse
from .utils import parse_pubsub_message, parse_stream_entries

# ----------------------- #

logger = getLogger(__name__).bind(scope="redis.client")

# ....................... #


@final
@attrs.define(frozen=True, slots=True, kw_only=True)
class RedisConfig:
    """Redis configuration."""

    max_size: int = 20
    socket_timeout: Optional[float] = None
    connect_timeout: Optional[float] = None


# ....................... #


@final
@attrs.define(slots=True)
class RedisClient:
    """Async Redis client with connection pooling and context-bound pipelines.

    Must be initialised via :meth:`initialize` with a DSN before use.  Uses
    context variables to share a single pipeline per logical request, so nested
    :meth:`pipeline` blocks reuse the parent pipeline and increment a depth
    counter instead of creating a new one.
    """

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
        self,
        dsn: str,
        *,
        config: RedisConfig = RedisConfig(),
    ) -> None:
        if self.__client is not None:
            logger.trace("Client already initialized, skipping")
            return

        self.__pool = ConnectionPool.from_url(  # pyright: ignore[reportUnknownMemberType]
            dsn,
            max_connections=config.max_size,
            socket_timeout=config.socket_timeout,
            socket_connect_timeout=config.connect_timeout,
            decode_responses=False,
            encoding="utf-8",
        )
        self.__client = Redis(connection_pool=self.__pool)
        await self.__client.ping()  # type: ignore[misc]

        logger.trace("Client initialized successfully")

    # ....................... #

    async def close(self) -> None:
        if self.__client is not None:
            logger.trace("Client found, closing")
            await self.__client.aclose()
            self.__client = None

        if self.__pool is not None:
            logger.trace("Pool found, disconnecting")
            await self.__pool.disconnect(inuse_connections=True)
            self.__pool = None

        logger.trace("Client closed successfully")

    # ....................... #

    def __require_client(self) -> Redis:
        if self.__client is None:
            raise InfrastructureError("Redis client is not initialized")

        return self.__client

    # ....................... #

    async def health(self) -> tuple[str, bool]:
        try:
            ok = await self.__require_client().ping()  # type: ignore[misc]
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

    @redis_handled("redis.pipeline")  # type: ignore[untyped-decorator]
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
            await pipe.reset()  # type: ignore[no-untyped-call]

    # ....................... #
    # Canonical methods

    @redis_handled("redis.get")  # type: ignore[untyped-decorator]
    async def get(self, key: str) -> Optional[bytes | str]:
        return await self.__executor().get(key)

    # ....................... #

    @redis_handled("redis.mget")  # type: ignore[untyped-decorator]
    async def mget(self, keys: Sequence[str]) -> list[Optional[bytes | str]]:
        return await self.__executor().mget(*keys)

    # ....................... #

    @redis_handled("redis.set")  # type: ignore[untyped-decorator]
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

    @redis_handled("redis.mset")  # type: ignore[untyped-decorator]
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

    @redis_handled("redis.delete")  # type: ignore[untyped-decorator]
    async def delete(self, *keys: str) -> int:
        if not keys:
            return 0

        res = await self.__executor().delete(*keys)

        return int(res)

    # ....................... #

    @redis_handled("redis.unlink")  # type: ignore[untyped-decorator]
    async def unlink(self, *keys: str) -> int:
        if not keys:
            return 0

        res = await self.__executor().unlink(*keys)

        return int(res)

    # ....................... #

    @redis_handled("redis.expire")  # type: ignore[untyped-decorator]
    async def expire(self, key: str, seconds: int) -> bool:
        res = await self.__executor().expire(key, seconds)

        return bool(res)

    # ....................... #
    # Counter methods

    @redis_handled("redis.incr")  # type: ignore[untyped-decorator]
    async def incr(self, key: str, by: int = 1) -> int:
        res = await self.__executor().incrby(key, by)

        return int(res)

    # ....................... #

    @redis_handled("redis.decr")  # type: ignore[untyped-decorator]
    async def decr(self, key: str, by: int = 1) -> int:
        res = await self.__executor().decrby(key, by)

        return int(res)

    # ....................... #

    @redis_handled("redis.reset")  # type: ignore[untyped-decorator]
    async def reset(self, key: str, value: int) -> int:
        res = await self.__executor().getset(key, value)

        return int(res or 0)

    # ....................... #
    # PubSub methods

    @redis_handled("redis.publish")  # type: ignore[untyped-decorator]
    async def publish(self, channel: str, message: bytes | str) -> int:
        res = await self.__executor().publish(  # pyright: ignore[reportUnknownMemberType]
            channel, message
        )

        return int(res)

    # ....................... #

    @redis_handled("redis.subscribe")  # type: ignore[untyped-decorator]
    async def subscribe(
        self,
        channels: Sequence[str],
        *,
        timeout: Optional[timedelta] = None,
    ) -> AsyncIterator[RedisPubSubMessage]:
        if not channels:
            return

        pubsub = (
            self.__require_client().pubsub()  # pyright: ignore[reportUnknownMemberType]
        )
        await pubsub.subscribe(*channels)  # pyright: ignore[reportUnknownMemberType]

        try:
            while True:
                raw = await pubsub.get_message(  # pyright: ignore[reportUnknownVariableType]
                    ignore_subscribe_messages=True,
                    timeout=timeout.total_seconds() if timeout else None,
                )

                if raw is None:
                    continue

                parsed = parse_pubsub_message(
                    raw  # pyright: ignore[reportUnknownArgumentType]
                )

                if parsed is None:
                    continue

                yield parsed

        finally:
            await pubsub.unsubscribe(  # pyright: ignore[reportUnknownMemberType]
                *channels
            )
            await pubsub.aclose()  # type: ignore[no-untyped-call]

    # ....................... #
    # Stream methods

    @redis_handled("redis.xadd")  # type: ignore[untyped-decorator]
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
            data,  # type: ignore[arg-type]
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

    @redis_handled("redis.xread")  # type: ignore[untyped-decorator]
    async def xread(
        self,
        streams: dict[str, str],
        *,
        count: Optional[int] = None,
        block_ms: Optional[int] = None,
    ) -> RedisStreamResponse:
        res = await self.__executor().xread(
            streams=streams,  # type: ignore[arg-type]
            count=count,
            block=block_ms,
        )

        return parse_stream_entries(res)

    # ....................... #

    @redis_handled("redis.xdel")  # type: ignore[untyped-decorator]
    async def xdel(self, stream: str, ids: Sequence[str]) -> int:
        if not ids:
            return 0

        res = await self.__executor().xdel(stream, *ids)

        return int(res)

    # ....................... #

    @redis_handled("redis.xtrim_maxlen")  # type: ignore[untyped-decorator]
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

    @redis_handled("redis.xtrim_minid")  # type: ignore[untyped-decorator]
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

    @redis_handled("redis.xgroup_create")  # type: ignore[untyped-decorator]
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

    @redis_handled("redis.xgroup_read")  # type: ignore[untyped-decorator]
    async def xgroup_read(
        self,
        group: str,
        consumer: str,
        streams: dict[str, str],
        *,
        count: Optional[int] = None,
        block_ms: Optional[int] = None,
        noack: bool = False,
    ) -> RedisStreamResponse:
        res = await self.__executor().xreadgroup(
            group,
            consumer,
            streams,  # type: ignore[arg-type]
            count=count,
            block=block_ms,
            noack=noack,
        )

        return parse_stream_entries(res)

    # ....................... #

    @redis_handled("redis.xack")  # type: ignore[untyped-decorator]
    async def xack(self, stream: str, group: str, ids: Sequence[str]) -> int:
        if not ids:
            return 0

        res = await self.__executor().xack(stream, group, *ids)

        return int(res)
