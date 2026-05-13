from forze_redis._compat import require_redis

require_redis()

# ....................... #

import asyncio
from collections.abc import Awaitable, Callable
from contextlib import asynccontextmanager
from contextvars import ContextVar
from datetime import timedelta
from typing import Any, AsyncIterator, Mapping, Sequence, TypeVar, cast, final

import attrs
from redis.asyncio.client import Pipeline, Redis
from redis.asyncio.connection import ConnectionPool
from redis.commands.core import AsyncScript
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError

from forze.base.errors import CoreError, InfrastructureError
from forze.base.primitives import JsonDict
from forze_redis.kernel._logger import logger
from forze_redis.kernel.scripts import MSET_BULK_SET

from .errors import redis_handled
from .port import RedisClientPort
from .types import RedisPubSubMessage, RedisStreamResponse
from .utils import parse_pubsub_message, parse_stream_entries
from .value_objects import RedisConfig

# ----------------------- #

T = TypeVar("T")

_READ_RETRY_EXC: tuple[type[BaseException], ...] = (
    RedisConnectionError,
    RedisTimeoutError,
    TimeoutError,
    OSError,
)

_MGET_CHUNK_SIZE = 2000

_SCRIPT_AWAIT_MAX = 16

# ....................... #


def _bytes_or_none(value: bytes | str | None) -> bytes | None:
    if value is None:
        return None

    if isinstance(value, (bytes, bytearray)):
        return bytes(value)

    return str(value).encode("utf-8")


# ....................... #


@final
@attrs.define(slots=True)
class RedisClient(RedisClientPort):
    """Async Redis client with connection pooling and context-bound pipelines.

    Must be initialised via :meth:`initialize` with a DSN before use.  Uses
    context variables to share a single pipeline per logical request, so nested
    :meth:`pipeline` blocks reuse the parent pipeline and increment a depth
    counter instead of creating a new one.
    """

    __pool: ConnectionPool | None = attrs.field(default=None, init=False)
    __client: Redis | None = attrs.field(default=None, init=False)

    __ctx_pipe: ContextVar[Pipeline | None] = attrs.field(
        factory=lambda: ContextVar("redis_pipe", default=None),
        init=False,
    )
    __ctx_depth: ContextVar[int] = attrs.field(
        factory=lambda: ContextVar("redis_pipe_depth", default=0),
        init=False,
    )

    __script_registry: dict[str, AsyncScript] = attrs.field(factory=dict, init=False)

    __redis_config: RedisConfig = attrs.field(factory=RedisConfig, init=False)

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

        socket_timeout = (
            config.socket_timeout.total_seconds() if config.socket_timeout else None
        )
        connect_timeout = (
            config.connect_timeout.total_seconds() if config.connect_timeout else None
        )
        health_check_interval = (
            int(config.health_check_interval.total_seconds())
            if config.health_check_interval
            else None
        )

        self.__pool = (
            ConnectionPool.from_url(  # pyright: ignore[reportUnknownMemberType]
                dsn,
                max_connections=config.max_size,
                socket_timeout=socket_timeout,
                socket_connect_timeout=connect_timeout,
                decode_responses=False,
                encoding="utf-8",
                health_check_interval=health_check_interval,
                socket_keepalive=config.socket_keepalive,
                retry_on_timeout=config.retry_on_timeout,
                client_name=config.client_name,
            )
        )
        self.__client = Redis(connection_pool=self.__pool)
        await self.__client.ping()  # type: ignore[misc]

        self.__redis_config = config

        logger.trace("Client initialized successfully")

    # ....................... #

    def __in_pipeline(self) -> bool:
        return self.__current_pipe() is not None

    async def __maybe_read_retry(self, op: str, fn: Callable[[], Awaitable[T]]) -> T:
        if self.__in_pipeline():
            return await fn()

        cfg = self.__redis_config
        attempts = max(0, cfg.read_retry_attempts)
        base = max(0.0, cfg.read_retry_base_delay.total_seconds())
        last: BaseException | None = None

        for i in range(attempts + 1):
            try:
                return await fn()

            except _READ_RETRY_EXC as e:
                last = e

                if i >= attempts:
                    raise

                hook = cfg.on_read_retry

                if hook is not None:
                    hook(op, i + 1)

                await asyncio.sleep(base * (2**i))

        if last is None:
            raise CoreError("Last exception is None")

        raise last

    # ....................... #

    async def close(self) -> None:
        self.__script_registry.clear()

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

    def __current_pipe(self) -> Pipeline | None:
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

    @redis_handled("redis.exists")  # type: ignore[untyped-decorator]
    async def exists(self, key: str) -> bool:
        async def _call() -> bool:
            res = await self.__executor().exists(key)

            return res == 1

        return await self.__maybe_read_retry("exists", _call)

    # ....................... #

    @redis_handled("redis.pttl")  # type: ignore[untyped-decorator]
    async def pttl(self, key: str) -> int | None:
        """Milliseconds until expiry, or ``None`` if the key is missing or has no TTL."""

        raw = await self.pttl_raw_ms(key)

        return raw if raw >= 0 else None

    # ....................... #

    @redis_handled("redis.pttl")  # type: ignore[untyped-decorator]
    async def pttl_raw_ms(self, key: str) -> int:
        """Return the raw Redis ``PTTL`` value in milliseconds (``>= 0`` time left, ``-1`` persistent, ``-2`` missing)."""

        async def _call() -> int:
            raw_res = await self.__executor().pttl(key)

            return int(cast(int, raw_res))

        return await self.__maybe_read_retry("pttl_raw_ms", _call)

    # ....................... #

    @redis_handled("redis.run_script")  # type: ignore[untyped-decorator]
    async def run_script(
        self,
        script: str,
        keys: Sequence[str],
        args: Sequence[Any],
    ) -> str:
        pipe = self.__current_pipe()
        raw_res: Any

        if pipe is None:
            reg = self.__script_registry.get(script)

            if reg is None:
                reg = self.__require_client().register_script(script)
                self.__script_registry[script] = reg

            raw_res = reg(  # pyright: ignore[reportUnknownVariableType]
                keys=list(keys), args=list(args)
            )

        else:
            numkeys = len(keys)
            keys_and_args = [*keys, *args]
            raw_res = pipe.eval(script, numkeys, *keys_and_args)

        if isinstance(raw_res, Awaitable):
            raw_res = await raw_res  # pyright: ignore[reportUnknownVariableType]

        if raw_res is True:
            return "1"

        if raw_res is False:
            return "0"

        if type(raw_res) is int:  # pyright: ignore[reportUnknownArgumentType]
            return str(raw_res)

        if type(raw_res) is bytes:  # pyright: ignore[reportUnknownArgumentType]
            return raw_res.decode("utf-8")

        if type(raw_res) is bytearray:  # pyright: ignore[reportUnknownArgumentType]
            return bytes(raw_res).decode("utf-8")

        return str(raw_res)  # pyright: ignore[reportUnknownArgumentType]

    # ....................... #

    @redis_handled("redis.get")  # type: ignore[untyped-decorator]
    async def get(self, key: str) -> bytes | None:
        async def _call() -> bytes | None:
            return _bytes_or_none(await self.__executor().get(key))

        return await self.__maybe_read_retry("get", _call)

    # ....................... #

    @redis_handled("redis.mget")  # type: ignore[untyped-decorator]
    async def mget(self, keys: Sequence[str]) -> list[bytes | None]:
        if not keys:
            return []

        async def _one_batch(batch: Sequence[str]) -> list[bytes | None]:
            async def _call() -> list[bytes | None]:
                raw = await self.__executor().mget(*batch)

                return [_bytes_or_none(x) for x in raw]

            return await self.__maybe_read_retry("mget", _call)

        if len(keys) <= _MGET_CHUNK_SIZE:
            return await _one_batch(keys)

        out: list[bytes | None] = []

        for i in range(0, len(keys), _MGET_CHUNK_SIZE):
            out.extend(await _one_batch(keys[i : i + _MGET_CHUNK_SIZE]))

        return out

    # ....................... #

    @redis_handled("redis.set")  # type: ignore[untyped-decorator]
    async def set(
        self,
        key: str,
        value: bytes | str,
        *,
        ex: int | None = None,
        px: int | None = None,
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
        ex: int | None = None,
        px: int | None = None,
        nx: bool = False,
        xx: bool = False,
    ) -> bool:
        if not mapping:
            return True

        # MSET does not support EX/PX/NX/XX natively. Use one Lua script so NX/XX
        # semantics are all-or-nothing and optional expiries apply atomically.
        if ex is None and px is None and not nx and not xx:
            await self.__executor().mset(mapping)
            return True

        if nx and xx:
            raise CoreError("Redis mset does not allow nx and xx together")

        keys_list = list(mapping.keys())
        argv: list[Any] = [
            str(-1 if ex is None else ex),
            str(-1 if px is None else px),
            "1" if nx else "0",
            "1" if xx else "0",
            *[mapping[k] for k in keys_list],
        ]

        raw = await self.run_script(MSET_BULK_SET, keys_list, argv)

        try:
            code = int(str(raw).strip())
        except ValueError:
            return False

        return code == 1

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
        res = (
            await self.__executor().publish(  # pyright: ignore[reportUnknownMemberType]
                channel, message
            )
        )

        return int(res)

    # ....................... #

    @redis_handled("redis.subscribe")  # type: ignore[untyped-decorator]
    async def subscribe(
        self,
        channels: Sequence[str],
        *,
        timeout: timedelta | None = None,
    ) -> AsyncIterator[RedisPubSubMessage]:
        if not channels:
            return

        cfg = self.__redis_config

        if not cfg.pubsub_auto_reconnect:
            pubsub = (
                self.__require_client().pubsub()  # pyright: ignore[reportUnknownMemberType]
            )
            await pubsub.subscribe(  # pyright: ignore[reportUnknownMemberType]
                *channels
            )

            try:
                while True:
                    raw = await pubsub.get_message(  # pyright: ignore[reportUnknownVariableType]
                        ignore_subscribe_messages=True,
                        timeout=timeout.total_seconds() if timeout else None,
                    )

                    if raw is None:
                        if timeout is None:
                            await asyncio.sleep(0)
                        continue

                    parsed = parse_pubsub_message(
                        raw  # pyright: ignore[reportUnknownArgumentType]
                    )

                    if parsed is None:
                        if timeout is None:
                            await asyncio.sleep(0)
                        continue

                    yield parsed

            finally:
                await pubsub.unsubscribe(  # pyright: ignore[reportUnknownMemberType]
                    *channels
                )
                await pubsub.aclose()  # type: ignore[no-untyped-call]

        else:
            max_delay = max(0.05, cfg.pubsub_reconnect_max_delay.total_seconds())
            backoff = 0.05

            while True:
                pubsub = (
                    self.__require_client().pubsub()  # pyright: ignore[reportUnknownMemberType]
                )
                await pubsub.subscribe(  # pyright: ignore[reportUnknownMemberType]
                    *channels
                )

                reconnect = False

                try:
                    while True:
                        try:
                            raw = await pubsub.get_message(  # pyright: ignore[reportUnknownVariableType]
                                ignore_subscribe_messages=True,
                                timeout=timeout.total_seconds() if timeout else None,
                            )

                        except _READ_RETRY_EXC:
                            hook = cfg.on_pubsub_reconnect

                            if hook is not None:
                                hook()

                            await asyncio.sleep(min(max_delay, backoff))
                            backoff = min(max_delay, backoff * 2)
                            reconnect = True
                            break

                        if raw is None:
                            if timeout is None:
                                await asyncio.sleep(0)
                            continue

                        parsed = parse_pubsub_message(
                            raw  # pyright: ignore[reportUnknownArgumentType]
                        )

                        if parsed is None:
                            if timeout is None:
                                await asyncio.sleep(0)
                            continue

                        backoff = 0.05
                        yield parsed

                finally:
                    await pubsub.unsubscribe(  # pyright: ignore[reportUnknownMemberType]
                        *channels
                    )
                    await pubsub.aclose()  # type: ignore[no-untyped-call]

                if not reconnect:
                    return

    # ....................... #
    # Stream methods

    @redis_handled("redis.xadd")  # type: ignore[untyped-decorator]
    async def xadd(
        self,
        stream: str,
        data: JsonDict,
        *,
        id: str = "*",
        maxlen: int | None = None,
        approx: bool = True,
        nomkstream: bool = False,
        minid: str | None = None,
        limit: int | None = None,
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
        count: int | None = None,
        block_ms: int | None = None,
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
        limit: int | None = None,
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
        limit: int | None = None,
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
        count: int | None = None,
        block_ms: int | None = None,
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
