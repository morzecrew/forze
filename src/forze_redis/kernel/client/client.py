from pydantic import SecretStr

from forze_redis._compat import redis_supports_client_side_caching, require_redis

require_redis()

# ....................... #

import asyncio
from collections.abc import AsyncGenerator, Awaitable, Callable, Mapping, Sequence
from contextlib import asynccontextmanager
from contextvars import ContextVar
from datetime import timedelta
from typing import (
    Any,
    TypeVar,
    final,
)

import attrs
from redis.asyncio.client import Pipeline, Redis
from redis.asyncio.connection import ConnectionPool
from redis.commands.core import AsyncScript
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError

from forze.application.execution.resilience.read_retry import retry_read
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict
from forze_redis.kernel._logger import logger
from forze_redis.kernel.scripts import MSET_BULK_SET

from .errors import exc_interceptor
from .invalidation import InvalidationHub
from .port import RedisClientPort
from .types import (
    RedisAutoClaimResponse,
    RedisPendingEntry,
    RedisPubSubMessage,
    RedisStreamResponse,
)
from .utils import (
    parse_pubsub_message,
    parse_stream_entries,
    parse_xautoclaim_response,
    parse_xpending_entries,
)
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

    __init_lock: asyncio.Lock = attrs.field(factory=asyncio.Lock, init=False)

    __invalidation_hub: "InvalidationHub | None" = attrs.field(default=None, init=False)

    # ....................... #
    # Lifecycle

    async def initialize(
        self,
        dsn: str | SecretStr,
        *,
        config: RedisConfig = RedisConfig(),
    ) -> None:
        async with self.__init_lock:
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

            if isinstance(dsn, SecretStr):
                dsn = dsn.get_secret_value()

            pool = ConnectionPool.from_url(  # pyright: ignore[reportUnknownMemberType]
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
            client = Redis(connection_pool=pool)

            # Ping before assigning so a failed ping doesn't leave the guard
            # satisfied with an unverified client.
            try:
                await client.ping()  # type: ignore[misc]

            except BaseException:
                await pool.disconnect()
                raise

            self.__pool = pool
            self.__client = client
            self.__redis_config = config

            logger.trace("Client initialized successfully")

    # ....................... #

    def __in_pipeline(self) -> bool:
        return self.__current_pipe() is not None

    def __require_no_pipeline(self, method: str) -> None:
        """Reject value-returning calls inside a bound pipeline scope.

        Pipeline commands only produce results at ``execute()``; until then the
        underlying call returns the pipeline object itself, so any coerced
        return value would be garbage. Fail loud instead of corrupting silently.
        """

        if self.__in_pipeline():
            raise exc.precondition(
                f"{method} is not available inside a pipeline scope; "
                "results only materialize at execute()",
                code="redis_read_in_pipeline",
            )

    async def __maybe_read_retry(self, op: str, fn: Callable[[], Awaitable[T]]) -> T:
        cfg = self.__redis_config
        hook = cfg.on_read_retry

        if hook is not None:

            def on_retry(attempt: int) -> None:
                hook(op, attempt)

        else:
            on_retry = None  # type: ignore[assignment]

        return await retry_read(
            fn,
            attempts=cfg.read_retry_attempts,
            base_delay=cfg.read_retry_base_delay.total_seconds(),
            retry_on=_READ_RETRY_EXC,
            on_retry=on_retry,
        )

    # ....................... #

    async def close(self) -> None:
        async with self.__init_lock:
            self.__script_registry.clear()

            if self.__invalidation_hub is not None:
                await self.__invalidation_hub.stop()
                self.__invalidation_hub = None

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
            raise exc.internal("Redis client is not initialized")

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

    @exc_interceptor.asynccontextmanager("redis.pipeline")  # type: ignore[untyped-decorator]
    @asynccontextmanager
    async def pipeline(self, *, transaction: bool = True) -> AsyncGenerator[Pipeline]:
        """Bind a context-local pipeline that batches **writes** until the block exits.

        Pipelines are for write batching only: fire-and-forget methods called
        inside the scope (``set``, ``mset``, ``delete``, ``unlink``, ``expire``,
        ``publish``, ``xdel``, ``xtrim_maxlen``, ``xtrim_minid``) queue onto the
        pipeline and return neutral placeholder values (``True`` / ``0``); the
        real results only materialize when the pipeline executes on scope exit.

        Value-returning methods (``get``, ``mget``, ``exists``, ``pttl``,
        ``pttl_raw_ms``, ``run_script``, ``incr``, ``decr``, ``reset``,
        ``scan``, ``xadd``, ``xread``, ``xgroup_read``, ``xgroup_create``,
        ``xack``, ``xautoclaim``, ``xpending``)
        raise a ``precondition`` error with code ``redis_read_in_pipeline``
        when called inside the scope — their results cannot be observed before
        ``execute()``, so returning anything would be silent corruption.

        Nested ``pipeline`` blocks reuse the parent pipeline and only the
        outermost block executes it.
        """

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

    @exc_interceptor.coroutine("redis.exists")  # type: ignore[untyped-decorator]
    async def exists(self, key: str) -> bool:
        self.__require_no_pipeline("exists")

        async def _call() -> bool:
            res = await self.__require_client().exists(key)

            return res == 1

        return await self.__maybe_read_retry("exists", _call)

    # ....................... #

    @exc_interceptor.coroutine("redis.pttl")  # type: ignore[untyped-decorator]
    async def pttl(self, key: str) -> int | None:
        """Milliseconds until expiry, or ``None`` if the key is missing or has no TTL."""

        self.__require_no_pipeline("pttl")

        raw = await self.pttl_raw_ms(key)

        return raw if raw >= 0 else None

    # ....................... #

    @exc_interceptor.coroutine("redis.pttl")  # type: ignore[untyped-decorator]
    async def pttl_raw_ms(self, key: str) -> int:
        """Return the raw Redis ``PTTL`` value in milliseconds (``>= 0`` time left, ``-1`` persistent, ``-2`` missing)."""

        self.__require_no_pipeline("pttl_raw_ms")

        async def _call() -> int:
            raw_res = await self.__require_client().pttl(key)

            return raw_res

        return await self.__maybe_read_retry("pttl_raw_ms", _call)

    # ....................... #

    @exc_interceptor.coroutine("redis.run_script")  # type: ignore[untyped-decorator]
    async def run_script(
        self,
        script: str,
        keys: Sequence[str],
        args: Sequence[Any],
    ) -> str:
        self.__require_no_pipeline("run_script")

        raw_res: Any

        reg = self.__script_registry.get(script)

        if reg is None:
            reg = self.__require_client().register_script(script)
            self.__script_registry[script] = reg

        raw_res = reg(  # pyright: ignore[reportUnknownVariableType]
            keys=list(keys), args=list(args)
        )

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

    @exc_interceptor.coroutine("redis.get")  # type: ignore[untyped-decorator]
    async def get(self, key: str) -> bytes | None:
        self.__require_no_pipeline("get")

        async def _call() -> bytes | None:
            return _bytes_or_none(await self.__require_client().get(key))

        return await self.__maybe_read_retry("get", _call)

    # ....................... #

    @exc_interceptor.coroutine("redis.mget")  # type: ignore[untyped-decorator]
    async def mget(self, keys: Sequence[str]) -> list[bytes | None]:
        if not keys:
            return []

        self.__require_no_pipeline("mget")

        async def _one_batch(batch: Sequence[str]) -> list[bytes | None]:
            async def _call() -> list[bytes | None]:
                raw = await self.__require_client().mget(*batch)

                return [_bytes_or_none(x) for x in raw]

            return await self.__maybe_read_retry("mget", _call)

        if len(keys) <= _MGET_CHUNK_SIZE:
            return await _one_batch(keys)

        out: list[bytes | None] = []

        for i in range(0, len(keys), _MGET_CHUNK_SIZE):
            out.extend(await _one_batch(keys[i : i + _MGET_CHUNK_SIZE]))

        return out

    # ....................... #

    @exc_interceptor.coroutine("redis.scan")  # type: ignore[untyped-decorator]
    async def scan(
        self,
        cursor: int = 0,
        *,
        match: str | None = None,
        count: int | None = None,
    ) -> tuple[int, list[str]]:
        self.__require_no_pipeline("scan")

        async def _call() -> tuple[int, list[str]]:
            next_cursor, raw = await self.__require_client().scan(  # pyright: ignore[reportUnknownMemberType]
                cursor=cursor,
                match=match,
                count=count,
            )

            return int(next_cursor), [
                key.decode() if isinstance(key, bytes) else str(key) for key in raw
            ]

        return await self.__maybe_read_retry("scan", _call)

    # ....................... #

    @exc_interceptor.coroutine("redis.set")  # type: ignore[untyped-decorator]
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

        # Inside a pipeline the command is only queued; the outcome (incl.
        # NX/XX applicability) materializes at execute(). Report "queued".
        return True if self.__in_pipeline() else bool(res)

    # ....................... #

    @exc_interceptor.coroutine("redis.mset")  # type: ignore[untyped-decorator]
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
            raise exc.precondition("Redis mset does not allow nx and xx together")

        keys_list = list(mapping.keys())
        argv: list[Any] = [
            str(-1 if ex is None else ex),
            str(-1 if px is None else px),
            "1" if nx else "0",
            "1" if xx else "0",
            *[mapping[k] for k in keys_list],
        ]

        pipe = self.__current_pipe()

        if pipe is not None:
            # Queue the atomic bulk-set script directly on the pipeline
            # (run_script raises inside pipeline scopes because its result
            # cannot be observed). The NX/XX outcome materializes at execute().
            await pipe.eval(MSET_BULK_SET, len(keys_list), *keys_list, *argv)

            return True

        raw = await self.run_script(MSET_BULK_SET, keys_list, argv)

        try:
            code = int(str(raw).strip())
        except ValueError:
            return False

        return code == 1

    # ....................... #

    @exc_interceptor.coroutine("redis.delete")  # type: ignore[untyped-decorator]
    async def delete(self, *keys: str) -> int:
        if not keys:
            return 0

        res = await self.__executor().delete(*keys)

        # Queued onto the pipeline; the deleted count materializes at execute().
        return 0 if self.__in_pipeline() else int(res)

    # ....................... #

    @exc_interceptor.coroutine("redis.unlink")  # type: ignore[untyped-decorator]
    async def unlink(self, *keys: str) -> int:
        if not keys:
            return 0

        res = await self.__executor().unlink(*keys)

        # Queued onto the pipeline; the unlinked count materializes at execute().
        return 0 if self.__in_pipeline() else int(res)

    # ....................... #

    @exc_interceptor.coroutine("redis.expire")  # type: ignore[untyped-decorator]
    async def expire(self, key: str, seconds: int, *, gt: bool = False) -> bool:
        # ``gt=True`` maps to Redis 7+ ``EXPIRE ... GT``: extend-only — the TTL
        # is set only when larger than the current remaining one (the sliding
        # cache window must never shorten an age-stretched entry).
        res = await self.__executor().expire(key, seconds, gt=gt)

        # Queued onto the pipeline; whether the key existed materializes at execute().
        return True if self.__in_pipeline() else bool(res)

    # ....................... #
    # Counter methods

    @exc_interceptor.coroutine("redis.incr")  # type: ignore[untyped-decorator]
    async def incr(self, key: str, by: int = 1) -> int:
        self.__require_no_pipeline("incr")

        res = await self.__executor().incrby(key, by)

        return int(res)

    # ....................... #

    @exc_interceptor.coroutine("redis.decr")  # type: ignore[untyped-decorator]
    async def decr(self, key: str, by: int = 1) -> int:
        self.__require_no_pipeline("decr")

        res = await self.__executor().decrby(key, by)

        return int(res)

    # ....................... #

    @exc_interceptor.coroutine("redis.reset")  # type: ignore[untyped-decorator]
    async def reset(self, key: str, value: int) -> int:
        self.__require_no_pipeline("reset")

        res = await self.__executor().getset(key, value)

        return int(res or 0)

    # ....................... #
    # PubSub methods

    @exc_interceptor.coroutine("redis.publish")  # type: ignore[untyped-decorator]
    async def publish(self, channel: str, message: bytes | str) -> int:
        res = await self.__executor().publish(  # pyright: ignore[reportUnknownMemberType]
            channel, message
        )

        # Queued onto the pipeline; the receiver count materializes at execute().
        return 0 if self.__in_pipeline() else int(res)

    # ....................... #

    async def track_invalidations(
        self,
        *,
        prefixes: Sequence[str],
        on_keys: Callable[[Sequence[str]], None],
        on_reset: Callable[[], None],
    ) -> Callable[[], Awaitable[None]] | None:
        """Subscribe to server-side key invalidations (``CLIENT TRACKING`` BCAST).

        *on_keys* receives raw server keys touched by any client's writes (and
        expirations/evictions) under the registered *prefixes*; *on_reset*
        fires whenever events may have been missed (stream (re)connect or
        failure) — subscribers must flush on it. Returns an unsubscribe
        callable. The hub (one pinned RESP3 connection consuming the server's
        ``invalidate`` push frames) starts lazily on first subscription, takes
        one pool slot while active, and stops with the client.
        """

        self.__require_client()

        if not redis_supports_client_side_caching():
            raise exc.configuration(
                "Client-side caching invalidation (subscribe_invalidations) requires "
                "redis-py 8+; the installed redis-py lacks the RESP3 push API. Upgrade "
                "redis-py, or run without client-side caching (the rest of forze_redis "
                "supports redis-py 7.3+).",
                code="redis.client_side_caching_unsupported",
            )

        pool = self.__pool

        if pool is None:  # pragma: no cover — require_client guards this
            raise exc.internal("Redis pool is not initialized")

        if self.__invalidation_hub is None:

            async def _acquire() -> Any:
                return await pool.get_connection()  # type: ignore[no-untyped-call]

            async def _release(conn: Any) -> None:
                try:
                    await conn.disconnect()

                finally:
                    await pool.release(conn)

            self.__invalidation_hub = InvalidationHub(
                acquire_connection=_acquire,
                release_connection=_release,
            )

        return await self.__invalidation_hub.subscribe(
            prefixes=prefixes,
            on_keys=on_keys,
            on_reset=on_reset,
        )

    # ....................... #

    @exc_interceptor.asyncgenerator("redis.subscribe")  # type: ignore[untyped-decorator]
    async def subscribe(
        self,
        channels: Sequence[str],
        *,
        timeout: timedelta | None = None,
    ) -> AsyncGenerator[RedisPubSubMessage]:
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

    @exc_interceptor.coroutine("redis.xadd")  # type: ignore[untyped-decorator]
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
        self.__require_no_pipeline("xadd")

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

        return res.decode("utf-8") if isinstance(res, bytes) else str(res)  # type: ignore[reportUnknownReturnType]

    # ....................... #

    @exc_interceptor.coroutine("redis.xread")  # type: ignore[untyped-decorator]
    async def xread(
        self,
        streams: dict[str, str],
        *,
        count: int | None = None,
        block_ms: int | None = None,
    ) -> RedisStreamResponse:
        self.__require_no_pipeline("xread")

        res = await self.__executor().xread(
            streams=streams,  # type: ignore[arg-type]
            count=count,
            block=block_ms,
        )

        return parse_stream_entries(res)

    # ....................... #

    @exc_interceptor.coroutine("redis.xdel")  # type: ignore[untyped-decorator]
    async def xdel(self, stream: str, ids: Sequence[str]) -> int:
        if not ids:
            return 0

        res = await self.__executor().xdel(stream, *ids)

        # Queued onto the pipeline; the deleted count materializes at execute().
        return 0 if self.__in_pipeline() else int(res)

    # ....................... #

    @exc_interceptor.coroutine("redis.xtrim_maxlen")  # type: ignore[untyped-decorator]
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

        # Queued onto the pipeline; the trimmed count materializes at execute().
        return 0 if self.__in_pipeline() else int(res)

    # ....................... #

    @exc_interceptor.coroutine("redis.xtrim_minid")  # type: ignore[untyped-decorator]
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

        # Queued onto the pipeline; the trimmed count materializes at execute().
        return 0 if self.__in_pipeline() else int(res)

    # ....................... #

    @exc_interceptor.coroutine("redis.xgroup_create")  # type: ignore[untyped-decorator]
    async def xgroup_create(
        self,
        stream: str,
        group: str,
        *,
        id: str = "0-0",
        mkstream: bool = True,
    ) -> bool:
        self.__require_no_pipeline("xgroup_create")

        res = await self.__executor().xgroup_create(
            stream,
            group,
            id=id,
            mkstream=mkstream,
        )

        return bool(res)

    # ....................... #

    @exc_interceptor.coroutine("redis.xgroup_read")  # type: ignore[untyped-decorator]
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
        self.__require_no_pipeline("xgroup_read")

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

    @exc_interceptor.coroutine("redis.xack")  # type: ignore[untyped-decorator]
    async def xack(self, stream: str, group: str, ids: Sequence[str]) -> int:
        if not ids:
            return 0

        self.__require_no_pipeline("xack")

        res = await self.__executor().xack(stream, group, *ids)

        return int(res)

    # ....................... #

    @exc_interceptor.coroutine("redis.xautoclaim")  # type: ignore[untyped-decorator]
    async def xautoclaim(
        self,
        stream: str,
        group: str,
        consumer: str,
        *,
        min_idle_ms: int,
        start_id: str = "0-0",
        count: int | None = None,
    ) -> RedisAutoClaimResponse:
        """One ``XAUTOCLAIM`` page: claim entries idle for at least *min_idle_ms*.

        Scans the group's pending-entries list from *start_id* and transfers
        ownership of matching entries to *consumer*. Returns the next scan
        cursor (``"0-0"`` once the list is exhausted), the claimed entries,
        and the ids dropped because they no longer exist in the stream —
        callers loop on the cursor for a full sweep.
        """

        self.__require_no_pipeline("xautoclaim")

        res = await self.__executor().xautoclaim(
            stream,
            group,
            consumer,
            min_idle_time=min_idle_ms,
            start_id=start_id,
            count=count,
        )

        return parse_xautoclaim_response(res)

    # ....................... #

    @exc_interceptor.coroutine("redis.xpending")  # type: ignore[untyped-decorator]
    async def xpending(
        self,
        stream: str,
        group: str,
        *,
        count: int,
        start_id: str = "-",
        end_id: str = "+",
    ) -> list[RedisPendingEntry]:
        """Extended ``XPENDING``: up to *count* pending rows in ``[start_id, end_id]``.

        Each row reports the entry id, owning consumer, idle milliseconds, and
        delivery counter, oldest first. Inspection only — never alters the
        pending-entries list. Page with an exclusive cursor (``"(<last_id>"``
        as *start_id*) to walk lists larger than *count*.
        """

        self.__require_no_pipeline("xpending")

        res = await self.__executor().xpending_range(
            stream,
            group,
            min=start_id,
            max=end_id,
            count=count,
        )

        return parse_xpending_entries(res)
