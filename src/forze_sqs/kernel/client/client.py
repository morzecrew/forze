from __future__ import annotations

from forze_sqs._compat import require_sqs

require_sqs()

# ....................... #

import asyncio
import base64
import math
import re
from contextlib import AsyncExitStack, asynccontextmanager
from contextvars import ContextVar
from datetime import datetime, timedelta, timezone
from re import Pattern
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
from uuid import uuid4

import aioboto3
import attrs
from pydantic import SecretStr

if TYPE_CHECKING:
    # Type-only: ``types-aiobotocore-sqs`` is a stub package with no runtime value, so
    # keep it off the import path (saves ~40 ms of cold-start import).
    from types_aiobotocore_sqs.client import SQSClient as AsyncSQSClient

from forze.application.contracts.envelope import HEADER_EVENT_ID
from forze.application.contracts.queue import SQS_MAX_DELAY, resolve_delivery_delay
from forze.base.exceptions import exc

from .._logger import logger
from .errors import exc_interceptor
from .port import SQSClientPort
from .types import SQSQueueMessage
from .value_objects import SQSConfig, SQSConnectionOpts

# ----------------------- #
# Transport constants live next to the client that owns them (same convention
# as the rabbitmq/redis siblings) — no separate constants module.

_TYPE_ATTR = "forze_type"
_KEY_ATTR = "forze_key"
_ENQUEUED_AT_ATTR = "forze_enqueued_at"
_ENCODING_ATTR = "forze_encoding"
_ENCODING_B64 = "b64"

_RESERVED_ATTRS = frozenset({_TYPE_ATTR, _KEY_ATTR, _ENQUEUED_AT_ATTR, _ENCODING_ATTR})
"""Message-attribute names owned by the transport: caller header values are
overwritten and the keys are excluded from the caller-visible ``headers``
mapping on read."""

_RECEIVE_COUNT_ATTR = "ApproximateReceiveCount"
"""SQS system attribute reporting deliveries including the current one."""

_SQS_SEND_MESSAGE_BATCH_MAX: Final[int] = 10
"""AWS limit for ``send_message_batch`` entries per request."""

_MAX_ENQUEUE_BATCH_CONCURRENCY: Final[int] = 32
"""Upper bound for parallel ``send_message_batch`` calls in :meth:`SQSClient.enqueue_many`."""

_DEFAULT_HTTP_POOL_SIZE: Final[int] = 10
"""Botocore default when ``max_pool_connections`` is omitted."""

_CONSUME_LONG_POLL_WAIT: Final[timedelta] = timedelta(seconds=20)
"""Per-receive long-poll wait used by :meth:`SQSClient.consume` (SQS maximum)."""

_CONSUME_ERROR_BACKOFF_INITIAL: Final[float] = 0.5
"""Initial backoff (seconds) after a failed receive inside :meth:`SQSClient.consume`."""

_CONSUME_ERROR_BACKOFF_MAX: Final[float] = 5.0
"""Backoff ceiling (seconds) for repeated receive failures in :meth:`SQSClient.consume`."""

_RE_UNSUPPORTED_CHARS: Pattern[str] = re.compile(r"[^A-Za-z0-9_-]")
_RE_MULTI_UNDERSCORE: Pattern[str] = re.compile(r"_+")

# ....................... #


@final
@attrs.define(slots=True)
class SQSClient(SQSClientPort):
    __opts: SQSConnectionOpts | None = attrs.field(default=None, init=False)
    __session: aioboto3.Session | None = attrs.field(default=None, init=False)

    __ctx_client: ContextVar[AsyncSQSClient | None] = attrs.field(
        factory=lambda: ContextVar("sqs_client", default=None),
        init=False,
    )
    __ctx_depth: ContextVar[int] = attrs.field(
        factory=lambda: ContextVar("sqs_depth", default=0),
        init=False,
    )

    __persistent_client: AsyncSQSClient | None = attrs.field(default=None, init=False)
    """Long-lived SQS client opened by :meth:`initialize`, shared by all scopes.

    aiobotocore clients are coroutine-safe for concurrent calls: each client
    owns one ``aiohttp.ClientSession`` with a pooled ``TCPConnector``
    (bounded by ``max_pool_connections``) and serializes credential refresh
    behind an ``asyncio.Lock``, so a single instance can serve concurrent
    operations from multiple tasks.
    """

    __exit_stack: AsyncExitStack | None = attrs.field(default=None, init=False)
    """Owns the persistent client's async context; closed by :meth:`close`."""

    __queue_url_cache: dict[str, str] = attrs.field(factory=dict, init=False)

    __pending: dict[str, tuple[str, str]] = attrs.field(factory=dict, init=False)
    """In-flight deliveries: message id -> (queue, receipt handle).

    Mirrors the RabbitMQ client's pending map so callers ack/nack with the
    message ``id`` exposed on :class:`SQSQueueMessage` while the client
    resolves the per-delivery ``ReceiptHandle`` internally. A redelivered
    message (same ``MessageId``) overwrites its entry — only the latest
    receipt handle is valid.
    """

    __pending_lock: asyncio.Lock = attrs.field(factory=asyncio.Lock, init=False)
    __enqueue_batch_concurrency: int = attrs.field(
        default=_DEFAULT_HTTP_POOL_SIZE,
        init=False,
    )
    """Max concurrent ``send_message_batch`` calls for :meth:`enqueue_many`."""

    __init_lock: asyncio.Lock = attrs.field(factory=asyncio.Lock, init=False)

    # ....................... #
    # Lifecycle

    async def initialize(
        self,
        endpoint: str,
        access_key_id: str | None = None,
        secret_access_key: str | SecretStr | None = None,
        *,
        region_name: str | None = None,
        config: SQSConfig | None = None,
    ) -> None:
        """Initialize the SQS session and open a long-lived client.

        Creates the underlying ``aiobotocore`` client **once**; subsequent
        :meth:`client` scopes reuse it (depth tracking only), so per-operation
        scopes are cheap. The client is released by :meth:`close`.

        Credentials are optional: when *access_key_id* and
        *secret_access_key* are both ``None`` (the default), they are **not**
        passed to the client and botocore's default credential chain resolves
        them instead (environment variables, shared config/credentials files,
        container/instance roles — ECS task roles, EC2 instance profiles,
        EKS IRSA). Passing explicit static credentials keeps the previous
        behavior. Providing only one of the two raises a configuration error.

        The region is optional too: when *region_name* is ``None`` (the
        default), it is **not** passed to the client and botocore's chain
        resolves it (``AWS_REGION``/``AWS_DEFAULT_REGION``, shared profile,
        IMDS). With no region resolvable anywhere, botocore's
        ``NoRegionError`` surfaces through the normal error mapping.

        :param endpoint: SQS-compatible endpoint URL.
        :param access_key_id: AWS access key id, or ``None`` to defer to the
            default credential chain.
        :param secret_access_key: AWS secret access key (plain or
            :class:`SecretStr`), or ``None`` to defer to the chain.
        :param region_name: AWS region name, or ``None`` to defer to the
            chain-resolved region.
        :param config: Optional botocore configuration overrides.
        """
        async with self.__init_lock:
            if self.__session is not None:
                return

            pool_cap = _DEFAULT_HTTP_POOL_SIZE

            if config is not None:
                aio_config = config.to_aio_config()
                if config.max_pool_connections is not None:
                    pool_cap = max(1, int(config.max_pool_connections))
            else:
                aio_config = None

            self.__enqueue_batch_concurrency = max(
                1,
                min(pool_cap, _MAX_ENQUEUE_BATCH_CONCURRENCY),
            )

            self.__opts = SQSConnectionOpts(
                endpoint=endpoint,
                region_name=region_name,
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
        """Close the long-lived client and drop session, caches, and pending.

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
                self.__queue_url_cache.clear()

                async with self.__pending_lock:
                    self.__pending.clear()

    # ....................... #

    def __require_session(self) -> aioboto3.Session:
        if self.__session is None:
            raise exc.internal("SQS session is not initialized")

        return self.__session

    # ....................... #

    def __current_client(self) -> AsyncSQSClient | None:
        return self.__ctx_client.get()

    # ....................... #

    def __require_client(self) -> AsyncSQSClient:
        c = self.__current_client()

        if c is None:
            raise exc.internal("SQS client is not initialized")

        return c

    # ....................... #

    @staticmethod
    def __is_queue_url(queue: str) -> bool:
        return queue.startswith("https://") or queue.startswith("http://")

    # ....................... #

    @staticmethod
    def __sanitize_queue_name(queue: str) -> str:
        """Normalize queue name for SQS-compatible backends.

        Replaces unsupported characters with ``_`` and preserves ``.fifo`` suffix.
        """
        is_fifo = queue.endswith(".fifo")
        base = queue[:-5] if is_fifo else queue
        base = _RE_UNSUPPORTED_CHARS.sub("_", base)
        base = _RE_MULTI_UNDERSCORE.sub("_", base).strip("_")

        if not base:
            base = "queue"

        if is_fifo:
            max_base = 75  # 80 - len(".fifo")
            base = base[:max_base]
            return f"{base}.fifo"

        return base[:80]

    # ....................... #

    @staticmethod
    def __is_fifo_target(queue: str, queue_url: str) -> bool:
        if queue.endswith(".fifo"):
            return True

        return queue_url.rstrip("/").rsplit("/", 1)[-1].endswith(".fifo")

    # ....................... #

    def __create_client_cm(self) -> AsyncContextManager[AsyncSQSClient]:
        """Build the ``aiobotocore`` client async context manager from opts.

        When credentials are absent in the options, the ``aws_*`` kwargs are
        omitted so botocore's default credential chain resolves them. When the
        region is absent, the ``region_name`` kwarg is omitted so botocore's
        chain resolves it (env, profile, IMDS).
        """

        session = self.__require_session()
        opts = self.__opts

        if opts is None:
            raise exc.internal("SQS client options are not initialized")

        kwargs: dict[str, Any] = {
            "endpoint_url": opts.endpoint,
            "config": opts.config,
        }

        if opts.region_name is not None:
            kwargs["region_name"] = opts.region_name

        if opts.access_key_id is not None and opts.secret_access_key is not None:
            kwargs["aws_access_key_id"] = opts.access_key_id
            kwargs["aws_secret_access_key"] = opts.secret_access_key.get_secret_value()

        cm = session.client("sqs", **kwargs)  # type: ignore

        return cast("AsyncContextManager[AsyncSQSClient]", cm)

    # ....................... #

    @asynccontextmanager
    async def client(self) -> AsyncGenerator[AsyncSQSClient]:
        """Yield a context-bound SQS client with nested-scope reuse.

        When :meth:`initialize` has opened the long-lived client, scopes are
        cheap: they bind the shared client to the current context (depth
        tracking only). Lazy per-scope client construction remains as a
        fallback for instances whose options were configured without the
        persistent client (un-lifecycled usage).
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

    @exc_interceptor.coroutine("sqs.health")  # type: ignore[untyped-decorator]
    async def health(self) -> tuple[str, bool]:
        """Check SQS client health by listing queues.

        Self-contained: opens its own client scope when no ambient
        :meth:`client` context is active, and never raises — failures are
        reported as ``("<error message>", False)``.
        """

        try:
            if self.__current_client() is None:
                async with self.client() as c:
                    await c.list_queues(MaxResults=1)

            else:
                await self.__require_client().list_queues(MaxResults=1)

            return "ok", True

        except Exception as e:
            return str(e), False

    # ....................... #

    @exc_interceptor.coroutine("sqs.create_queue")  # type: ignore[untyped-decorator]
    async def create_queue(
        self,
        queue: str,
        *,
        attributes: dict[str, str] | None = None,
    ) -> str:
        """Create a queue and return its URL."""

        if self.__is_queue_url(queue):
            return queue

        queue_name = self.__sanitize_queue_name(queue)
        payload: dict[str, Any] = {"QueueName": queue_name}

        if attributes:
            payload["Attributes"] = attributes

        c = self.__require_client()

        resp = await c.create_queue(**payload)
        queue_url = resp.get("QueueUrl")

        self.__queue_url_cache[queue] = queue_url
        self.__queue_url_cache[queue_name] = queue_url

        return queue_url

    # ....................... #

    @exc_interceptor.coroutine("sqs.get_queue_url")  # type: ignore[untyped-decorator]
    async def queue_url(self, queue: str) -> str:
        """Resolve queue name (or URL) to queue URL."""

        return await self.__resolve_queue_url(queue)

    # ....................... #

    async def __resolve_queue_url(self, queue: str) -> str:
        if self.__is_queue_url(queue):
            return queue

        queue_name = self.__sanitize_queue_name(queue)

        if queue in self.__queue_url_cache:
            return self.__queue_url_cache[queue]

        if queue_name in self.__queue_url_cache:
            return self.__queue_url_cache[queue_name]

        c = self.__require_client()

        resp = await c.get_queue_url(QueueName=queue_name)
        queue_url = resp.get("QueueUrl")

        self.__queue_url_cache[queue] = queue_url
        self.__queue_url_cache[queue_name] = queue_url

        return queue_url

    # ....................... #

    @staticmethod
    def __build_message_attributes(
        *,
        type: str | None,
        key: str | None,
        enqueued_at: datetime | None,
        headers: Mapping[str, str] | None = None,
    ) -> dict[str, dict[str, str]]:
        # Caller headers pass through verbatim as String attributes; the
        # reserved transport attributes are written after them so they always
        # win on collision. Note AWS caps message attributes at 10 per
        # message — headers count against that limit.
        attrs: dict[str, dict[str, str]] = {}

        if headers:
            for header_key, header_value in headers.items():
                attrs[header_key] = {
                    "StringValue": header_value,
                    "DataType": "String",
                }

        attrs[_ENCODING_ATTR] = {"StringValue": _ENCODING_B64, "DataType": "String"}

        if type is not None:
            attrs[_TYPE_ATTR] = {"StringValue": type, "DataType": "String"}

        if key is not None:
            attrs[_KEY_ATTR] = {"StringValue": key, "DataType": "String"}

        if enqueued_at is not None:
            attrs[_ENQUEUED_AT_ATTR] = {
                "StringValue": enqueued_at.isoformat(),
                "DataType": "String",
            }

        return attrs

    # ....................... #

    @staticmethod
    def __extract_attr(
        attrs: dict[str, dict[str, str]] | None,
        key: str,
    ) -> str | None:
        if not attrs:
            return None

        raw = attrs.get(key)
        if not isinstance(raw, dict):
            return None

        value = raw.get("StringValue")
        return value if isinstance(value, str) else None

    # ....................... #

    @staticmethod
    def __extract_headers(
        attrs: dict[str, dict[str, str]] | None,
    ) -> dict[str, str] | None:
        """Return caller-visible string headers from message attributes.

        Reserved transport attributes are excluded; only ``String`` values
        survive — the port contract is string-to-string.
        """

        if not attrs:
            return None

        out: dict[str, str] = {}

        for attr_key, raw in attrs.items():
            if attr_key in _RESERVED_ATTRS or not isinstance(
                raw, dict
            ):  # pyright: ignore[reportUnnecessaryIsInstance]
                continue

            value = raw.get("StringValue")

            if isinstance(value, str):
                out[attr_key] = value

        return out or None

    # ....................... #

    @staticmethod
    def __extract_delivery_count(system_attrs: dict[str, str] | None) -> int | None:
        """Parse ``ApproximateReceiveCount`` (deliveries including this one)."""

        if not system_attrs:
            return None

        raw = system_attrs.get(_RECEIVE_COUNT_ATTR)

        if isinstance(raw, str) and raw.isdigit():
            return int(raw)

        return None

    # ....................... #

    @staticmethod
    def __encode_body(body: bytes) -> str:
        return base64.b64encode(body).decode("ascii")

    # ....................... #

    @staticmethod
    def __decode_body(
        body: str,
        attrs: dict[str, dict[str, str]] | None,
    ) -> bytes:
        encoding = SQSClient.__extract_attr(attrs, _ENCODING_ATTR)

        if encoding == _ENCODING_B64:
            try:
                return base64.b64decode(body, validate=True)

            except Exception as e:
                raise exc.internal("SQS message payload is not valid base64.") from e

        return body.encode("utf-8")

    # ....................... #

    @staticmethod
    def __extract_enqueued_at(
        attrs: dict[str, dict[str, str]] | None,
        system_attrs: dict[str, str] | None,
    ) -> datetime | None:
        from_message_attr = SQSClient.__extract_attr(attrs, _ENQUEUED_AT_ATTR)

        if from_message_attr:
            try:
                return datetime.fromisoformat(from_message_attr)
            except ValueError:
                pass

        if system_attrs:
            sent = system_attrs.get("SentTimestamp")
            if sent and sent.isdigit():
                return datetime.fromtimestamp(int(sent) / 1000.0, tz=timezone.utc)

        return None

    # ....................... #

    @staticmethod
    def __chunked_pending(
        pending: Sequence[tuple[str, str]],
        size: int = 10,
    ) -> list[list[tuple[str, str]]]:
        return [list(pending[i : i + size]) for i in range(0, len(pending), size)]

    # ....................... #

    @staticmethod
    def _resolve_sqs_delay_seconds(
        *,
        delay: timedelta | None,
        not_before: datetime | None,
    ) -> int | None:
        resolved = resolve_delivery_delay(delay=delay, not_before=not_before)

        if resolved is None:
            return None

        if resolved > SQS_MAX_DELAY:
            raise exc.precondition(
                "SQS enqueue delay exceeds 900 seconds; use an external scheduler, "
                "database outbox, or Temporal schedules for longer deferrals"
            )

        seconds = int(resolved.total_seconds())

        if seconds <= 0:
            return None

        return seconds

    # ....................... #

    @exc_interceptor.coroutine("sqs.enqueue")  # type: ignore[untyped-decorator]
    async def enqueue(
        self,
        queue: str,
        body: bytes,
        *,
        type: str | None = None,
        key: str | None = None,
        enqueued_at: datetime | None = None,
        message_id: str | None = None,
        delay: timedelta | None = None,
        not_before: datetime | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> str:
        """Send a single message and return the broker-assigned ``MessageId``.

        FIFO targets (``.fifo``): ``MessageGroupId`` is *key* (or ``"forze"``)
        — the per-aggregate ordering lane — and ``MessageDeduplicationId``
        follows the priority documented on :meth:`enqueue_many`: explicit
        *message_id*, else the ``forze_event_id`` header, else a fresh random
        id. The caller ``key`` is deliberately **never** the dedup id:
        distinct events may share a key (ordering), and key-based dedup would
        silently drop them within the FIFO five-minute window.
        """
        return (
            await self.enqueue_many(
                queue,
                [body],
                type=type,
                key=key,
                enqueued_at=enqueued_at,
                message_ids=[message_id] if message_id is not None else None,
                delay=delay,
                not_before=not_before,
                headers=headers,
            )
        )[0]

    # ....................... #

    @exc_interceptor.coroutine("sqs.enqueue_many")  # type: ignore[untyped-decorator]
    async def enqueue_many(
        self,
        queue: str,
        bodies: Sequence[bytes],
        *,
        type: str | None = None,
        key: str | None = None,
        enqueued_at: datetime | None = None,
        message_ids: Sequence[str] | None = None,
        delay: timedelta | None = None,
        not_before: datetime | None = None,
        headers: Mapping[str, str] | None = None,
        message_headers: Sequence[Mapping[str, str]] | None = None,
    ) -> list[str]:
        """Send a batch of messages and return broker-assigned ``MessageId``s.

        The returned identifiers correlate with the ``id`` of received
        messages (stable across redeliveries). Caller-provided *message_ids*
        are used as FIFO deduplication ids, not as the returned identifiers.

        FIFO targets (``.fifo``) build the FIFO entry fields as:

        - ``MessageGroupId`` = *key* (or ``"forze"`` when unset) — the
          ordering lane: the outbox relay passes the staged ``ordering_key``
          here, so same-key events deliver in order on the happy path.
        - ``MessageDeduplicationId`` priority: explicit *message_ids* entry →
          the per-message ``forze_event_id`` header → a fresh random id per
          message. When *message_headers* is given each entry derives its dedup
          id from its own ``forze_event_id``, so a multi-message batch dedups
          per-message. Otherwise the batch-wide ``headers`` event id is used
          only for a single-message send — a shared event id must not collapse
          a multi-body batch. The event id header is the stable per-event
          identity, so a relay republishing the same event within the
          five-minute window dedupes, while **different** events sharing an
          ordering ``key`` never dedupe each other. The caller ``key`` is
          deliberately never used as the dedup id — that would drop distinct
          same-key events (event loss).

        Caller *headers* ride SQS message attributes (``String`` type)
        verbatim; the reserved transport attributes (``forze_type``,
        ``forze_key``, ``forze_enqueued_at``, ``forze_encoding``) always win
        on collision. *message_headers*, when given, supplies one header
        mapping per body (its length must equal *bodies*); entry ``i`` carries
        ``{**(headers or {}), **message_headers[i]}`` so a single batched
        publish can give each message distinct attributes (and distinct dedup
        ids). ``None`` keeps every entry on the shared *headers*.

        Splits into chunks of up to :data:`_SQS_SEND_MESSAGE_BATCH_MAX` (AWS limit).
        Multiple chunks are sent with bounded concurrency derived from
        ``max_pool_connections`` in :meth:`initialize` so large publishes do not
        serialize on the network while staying within the HTTP connection pool.
        """

        if not bodies:
            return []

        if message_ids is not None and len(message_ids) != len(bodies):
            raise exc.precondition("SQS message_ids size must match batch body size")

        if message_headers is not None and len(message_headers) != len(bodies):
            raise exc.precondition(
                "SQS message_headers size must match batch body size"
            )

        # Per-message effective headers (shared headers overridden by the
        # per-message entry) when message_headers is given; otherwise every
        # message rides the shared batch-wide headers.
        per_message_headers: list[Mapping[str, str] | None]

        if message_headers is not None:
            per_message_headers = [
                {**(headers or {}), **mh} for mh in message_headers
            ]
        else:
            per_message_headers = [headers] * len(bodies)

        if message_ids is not None:
            resolved_ids = list(message_ids)

        elif message_headers is not None:
            # Each entry dedups on its own event id; absent one, a fresh random
            # id keeps distinct messages from colliding.
            resolved_ids = [
                mh.get(HEADER_EVENT_ID) or uuid4().hex for mh in message_headers
            ]

        else:
            header_event_id = headers.get(HEADER_EVENT_ID) if headers else None

            if header_event_id and len(bodies) == 1:
                resolved_ids = [header_event_id]
            else:
                resolved_ids = [uuid4().hex for _ in range(len(bodies))]

        queue_url = await self.__resolve_queue_url(queue)
        # Batch-wide attributes reused for every entry when message_headers is
        # absent (the common path); per-entry attributes built below otherwise.
        shared_msg_attrs = self.__build_message_attributes(
            type=type,
            key=key,
            enqueued_at=enqueued_at,
            headers=headers,
        )
        is_fifo = self.__is_fifo_target(queue, queue_url)
        c = self.__require_client()
        delay_seconds = self._resolve_sqs_delay_seconds(
            delay=delay, not_before=not_before
        )

        def _entries_for_chunk(
            chunk: list[bytes],
            chunk_ids: list[str],
            chunk_headers: list[Mapping[str, str] | None],
        ) -> list[dict[str, Any]]:
            entries: list[dict[str, Any]] = []

            for i, (body, chunk_message_id, entry_headers) in enumerate(
                zip(chunk, chunk_ids, chunk_headers, strict=True)
            ):
                if message_headers is not None:
                    entry_attrs = self.__build_message_attributes(
                        type=type,
                        key=key,
                        enqueued_at=enqueued_at,
                        headers=entry_headers,
                    )
                else:
                    entry_attrs = shared_msg_attrs

                entry: dict[str, Any] = {
                    "Id": f"m{i}",
                    "MessageBody": self.__encode_body(body),
                    "MessageAttributes": entry_attrs,
                }

                if delay_seconds is not None:
                    entry["DelaySeconds"] = delay_seconds

                if is_fifo:
                    entry["MessageGroupId"] = key or "forze"
                    entry["MessageDeduplicationId"] = chunk_message_id

                entries.append(entry)

            return entries

        async def _send_chunk(
            chunk: list[bytes],
            chunk_ids: list[str],
            chunk_headers: list[Mapping[str, str] | None],
        ) -> list[str]:
            resp = await c.send_message_batch(
                QueueUrl=queue_url,
                Entries=_entries_for_chunk(chunk, chunk_ids, chunk_headers),  # type: ignore[arg-type]
            )
            failed = resp.get("Failed") or []

            if failed:
                failed_ids = ", ".join(f.get("Id", "unknown") for f in failed)

                raise exc.internal(
                    f"SQS send_message_batch has failed entries: {failed_ids}"
                )

            # Map entry ids ("m{i}") back to broker-assigned MessageIds so the
            # returned identifiers correlate with received ``message.id``.
            by_entry: dict[str, str] = {}

            for success in resp.get("Successful") or []:
                entry_id = success.get("Id")
                broker_id = success.get("MessageId")

                if isinstance(
                    entry_id, str
                ) and isinstance(  # pyright: ignore[reportUnnecessaryIsInstance]
                    broker_id, str
                ):
                    by_entry[entry_id] = broker_id

            return [
                by_entry.get(f"m{i}", fallback_id)
                for i, fallback_id in enumerate(chunk_ids)
            ]

        chunks: list[
            tuple[list[bytes], list[str], list[Mapping[str, str] | None]]
        ] = []

        for offset in range(0, len(bodies), _SQS_SEND_MESSAGE_BATCH_MAX):
            chunk = list(bodies[offset : offset + _SQS_SEND_MESSAGE_BATCH_MAX])
            chunk_ids = resolved_ids[offset : offset + _SQS_SEND_MESSAGE_BATCH_MAX]
            chunk_headers = per_message_headers[
                offset : offset + _SQS_SEND_MESSAGE_BATCH_MAX
            ]
            chunks.append((chunk, chunk_ids, chunk_headers))

        if len(chunks) == 1:
            return await _send_chunk(chunks[0][0], chunks[0][1], chunks[0][2])

        sem = asyncio.Semaphore(self.__enqueue_batch_concurrency)
        chunk_results: list[list[str]] = [[] for _ in chunks]

        async def _bounded(
            index: int,
            chunk: list[bytes],
            chunk_ids: list[str],
            chunk_headers: list[Mapping[str, str] | None],
        ) -> None:
            async with sem:
                chunk_results[index] = await _send_chunk(
                    chunk, chunk_ids, chunk_headers
                )

        async with asyncio.TaskGroup() as tg:
            for index, (ch, ids, hdrs) in enumerate(chunks):
                tg.create_task(_bounded(index, ch, ids, hdrs))

        return [broker_id for chunk_ids in chunk_results for broker_id in chunk_ids]

    # ....................... #

    @exc_interceptor.coroutine("sqs.receive")  # type: ignore[untyped-decorator]
    async def receive(
        self,
        queue: str,
        *,
        limit: int | None = None,
        timeout: timedelta | None = None,
    ) -> list[SQSQueueMessage]:
        """Receive up to ``limit`` messages from the queue."""
        max_messages = 1 if limit is None else min(limit, 10)

        if max_messages <= 0:
            return []

        wait_time = 0
        if timeout is not None:
            wait_time = int(max(0, min(timeout.total_seconds(), 20)))

        queue_url = await self.__resolve_queue_url(queue)
        c = self.__require_client()

        resp = await c.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=max_messages,
            WaitTimeSeconds=wait_time,
            MessageAttributeNames=["All"],
            AttributeNames=["SentTimestamp", "ApproximateReceiveCount"],
        )
        raw_messages = resp.get("Messages") or []
        out: list[SQSQueueMessage] = []

        for raw in raw_messages:
            receipt = raw.get("ReceiptHandle")
            if not isinstance(receipt, str):
                continue

            broker_id = raw.get("MessageId")
            # ``id`` is the broker MessageId: stable across redeliveries and
            # correlatable with enqueue. Fall back to the receipt handle for
            # SQS-compatible backends that omit MessageId.
            message_id = (
                broker_id if isinstance(broker_id, str) and broker_id else receipt
            )

            body = raw.get("Body", "")

            attrs = raw.get("MessageAttributes")
            attrs = attrs if isinstance(attrs, dict) else None

            system_attrs = raw.get("Attributes")
            system_attrs = system_attrs if isinstance(system_attrs, dict) else None

            out.append(
                SQSQueueMessage(
                    queue=queue,
                    id=message_id,
                    receipt_handle=receipt,
                    body=self.__decode_body(body, attrs),  # type: ignore[arg-type]
                    type=self.__extract_attr(attrs, _TYPE_ATTR),  # type: ignore[arg-type]
                    enqueued_at=self.__extract_enqueued_at(attrs, system_attrs),  # type: ignore[arg-type]
                    key=self.__extract_attr(attrs, _KEY_ATTR),  # type: ignore[arg-type]
                    headers=self.__extract_headers(attrs),  # type: ignore[arg-type]
                    delivery_count=self.__extract_delivery_count(system_attrs),  # type: ignore[arg-type]
                )
            )

        if out:
            async with self.__pending_lock:
                for message in out:
                    # A redelivery (same MessageId) supersedes the previous
                    # receipt handle; the latest one is the only valid one.
                    self.__pending[message.id] = (queue, message.receipt_handle)

        return out

    # ....................... #

    @exc_interceptor.asyncgenerator("sqs.consume")  # type: ignore[untyped-decorator]
    async def consume(
        self,
        queue: str,
        *,
        timeout: timedelta | None = None,
    ) -> AsyncGenerator[SQSQueueMessage]:
        """Yield queue messages continuously using SQS long polling.

        ``timeout`` is an **idle** timeout: ``None`` (or a non-positive
        value) consumes forever, long-polling with
        :data:`_CONSUME_LONG_POLL_WAIT` per receive so empty queues do not
        busy-loop; a finite value stops the generator cleanly (no error)
        once no message has arrived for that duration. Each message resets
        the idle window. Failed receives retry with exponential backoff.
        """

        idle_seconds = (
            timeout.total_seconds()
            if timeout is not None and timeout.total_seconds() > 0
            else None
        )
        long_poll_seconds = _CONSUME_LONG_POLL_WAIT.total_seconds()
        backoff = _CONSUME_ERROR_BACKOFF_INITIAL
        loop = asyncio.get_running_loop()
        idle_deadline = loop.time() + idle_seconds if idle_seconds is not None else None

        while True:
            if idle_deadline is not None:
                remaining = idle_deadline - loop.time()

                if remaining <= 0:
                    return

                # Ceil to a whole second (SQS wait granularity) so a small
                # remainder still long-polls instead of short-poll spinning;
                # the idle stop may overshoot by less than a second.
                wait_seconds = min(long_poll_seconds, float(math.ceil(remaining)))
            else:
                wait_seconds = long_poll_seconds

            try:
                # The long-poll await is the natural cancellation point.
                messages = await self.receive(
                    queue,
                    limit=1,
                    timeout=timedelta(seconds=wait_seconds),
                )

            except Exception:
                # Back off so a persistently failing receive does not
                # hot-loop; the idle deadline (when set) still terminates.
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, _CONSUME_ERROR_BACKOFF_MAX)

                continue

            backoff = _CONSUME_ERROR_BACKOFF_INITIAL

            if not messages:
                continue

            yield messages[0]

            # Reset the idle window when the caller resumes, so message
            # processing time does not count against the idle timeout
            # (mirrors the per-wait idle semantics of the RabbitMQ backend).
            if idle_seconds is not None:
                idle_deadline = loop.time() + idle_seconds

    # ....................... #

    async def __pending_by_ids(
        self,
        queue: str,
        ids: Sequence[str],
    ) -> list[tuple[str, str]]:
        """Resolve message ids to ``(id, receipt handle)`` pairs for *queue*."""

        async with self.__pending_lock:
            out: list[tuple[str, str]] = []

            for message_id in ids:
                entry = self.__pending.get(message_id)

                if entry is None:
                    continue

                pending_queue, receipt = entry

                if pending_queue != queue:
                    continue

                out.append((message_id, receipt))

            return out

    # ....................... #

    async def __drop_pending_many(self, message_ids: Sequence[str]) -> None:
        async with self.__pending_lock:
            for mid in message_ids:
                self.__pending.pop(mid, None)

    # ....................... #

    @exc_interceptor.coroutine("sqs.ack")  # type: ignore[untyped-decorator]
    async def ack(self, queue: str, ids: Sequence[str]) -> int:
        """Acknowledge messages by deleting them from the queue.

        *ids* are message ids as exposed on :class:`SQSQueueMessage`; ids
        without a pending delivery on this client are skipped.
        """
        if not ids:
            return 0

        pending = await self.__pending_by_ids(queue, ids)

        if not pending:
            return 0

        queue_url = await self.__resolve_queue_url(queue)
        acked_ids: list[str] = []

        c = self.__require_client()

        for chunk in self.__chunked_pending(pending):
            entries = [
                {"Id": f"m{i}", "ReceiptHandle": receipt}
                for i, (_, receipt) in enumerate(chunk)
            ]
            resp = await c.delete_message_batch(
                QueueUrl=queue_url,
                Entries=entries,  # type: ignore[arg-type]
            )
            failed = resp.get("Failed") or []

            if failed:
                failed_ids = ", ".join(f.get("Id", "unknown") for f in failed)

                raise exc.internal(
                    f"SQS delete_message_batch has failed entries: {failed_ids}"
                )

            acked_ids.extend(message_id for message_id, _ in chunk)

        await self.__drop_pending_many(acked_ids)

        return len(acked_ids)

    # ....................... #

    @exc_interceptor.coroutine("sqs.nack")  # type: ignore[untyped-decorator]
    async def nack(
        self,
        queue: str,
        ids: Sequence[str],
        *,
        requeue: bool = True,
    ) -> int:
        """Negative-acknowledge messages.

        When ``requeue`` is ``True``, the visibility timeout is reset to ``0``
        (best effort — a failed reset is logged and the message still
        redelivers once its original visibility timeout lapses). When
        ``False``, the message is **not** deleted: it stays invisible until
        its visibility timeout lapses, so the queue's redrive policy counts
        the receive and eventually dead-letters it (SQS's native DLQ
        mechanism).

        *ids* are message ids as exposed on :class:`SQSQueueMessage`; ids
        without a pending delivery on this client are skipped.
        """
        if not ids:
            return 0

        pending = await self.__pending_by_ids(queue, ids)

        if not pending:
            return 0

        nacked_ids = [message_id for message_id, _ in pending]

        if requeue:
            queue_url = await self.__resolve_queue_url(queue)
            c = self.__require_client()

            for chunk in self.__chunked_pending(pending):
                entries = [
                    {
                        "Id": f"m{i}",
                        "ReceiptHandle": receipt,
                        "VisibilityTimeout": 0,
                    }
                    for i, (_, receipt) in enumerate(chunk)
                ]

                try:
                    resp = await c.change_message_visibility_batch(
                        QueueUrl=queue_url,
                        Entries=entries,  # type: ignore[arg-type]
                    )
                    failed = resp.get("Failed") or []

                except Exception as e:
                    logger.warning(
                        "SQS nack visibility reset failed for queue %s: %s; "
                        "messages redeliver after their visibility timeout",
                        queue,
                        e,
                    )
                    continue

                if failed:
                    failed_ids = ", ".join(f.get("Id", "unknown") for f in failed)

                    logger.warning(
                        "SQS nack visibility reset has failed entries for "
                        "queue %s: %s; messages redeliver after their "
                        "visibility timeout",
                        queue,
                        failed_ids,
                    )

        await self.__drop_pending_many(nacked_ids)

        return len(nacked_ids)
