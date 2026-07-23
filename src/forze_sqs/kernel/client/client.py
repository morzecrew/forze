from __future__ import annotations

from forze_sqs._compat import require_sqs

require_sqs()

# ....................... #

import asyncio
import base64
import json
import math
import re
from collections.abc import AsyncGenerator, Mapping, Sequence
from contextlib import AbstractAsyncContextManager, AsyncExitStack, asynccontextmanager, suppress
from contextvars import ContextVar
from datetime import UTC, datetime, timedelta
from re import Pattern
from typing import (
    TYPE_CHECKING,
    Any,
    Final,
    cast,
    final,
)

import aioboto3
import attrs
from pydantic import SecretStr

if TYPE_CHECKING:
    # Type-only: ``types-aiobotocore-sqs`` is a stub package with no runtime value, so
    # keep it off the import path (saves ~40 ms of cold-start import).
    from types_aiobotocore_sqs.client import SQSClient as AsyncSQSClient

from forze.application.contracts.envelope import HEADER_EVENT_ID
from forze.application.contracts.queue import SQS_MAX_DELAY, resolve_delivery_delay
from forze.base.exceptions import CoreException, exc
from forze.base.primitives import clamp, uuid4

from .._logger import logger
from .constants import SQS_DEFAULT_MAX_BATCH_PAYLOAD_BYTES
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

_POISON_SOURCE_QUEUE_ATTR = "forze_poison_source_queue"
"""Provenance attribute on a retained poison copy: the logical source queue name."""

_POISON_SOURCE_ID_ATTR = "forze_poison_message_id"
"""Provenance attribute on a retained poison copy: the original broker ``MessageId``
(the copy is assigned a new one by the poison queue)."""

_SQS_MAX_MESSAGE_ATTRIBUTES: Final[int] = 10
"""AWS cap on message attributes per message."""

_SQS_SEND_MESSAGE_BATCH_MAX: Final[int] = 10
"""AWS limit for ``send_message_batch`` entries per request."""


_SQS_RESERVED_ATTR_BYTES: Final[int] = 1024
"""Generous per-entry headroom for the reserved transport attributes (``forze_type`` /
``forze_key`` / ``forze_enqueued_at`` / ``forze_encoding``, ~185 B in practice) that the
client adds on top of the caller headers, so size accounting over-counts rather than under."""

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
@attrs.define(slots=True, kw_only=True, frozen=True)
class _RetainedRaw:
    """The raw wire form of an in-flight delivery, kept for a byte-identical copy.

    Two paths re-send a delivery and therefore need the **raw** body/attributes — the
    same bytes the decode path retains, not the decoded model: a FIFO poison park (which
    must delete the message to unblock its group, preserving it on the retention queue;
    held only while a retention target is configured) and a standard-queue uncounted
    requeue (``nack(requeue=True, count=False)``, which replaces the message with a fresh
    copy so the broker's receive count resets; always held).
    """

    body: str
    """Raw SQS message body, exactly as received (still encoded)."""

    attributes: dict[str, Any] | None = None
    """Raw ``MessageAttributes`` as received, or ``None`` when the message carried none."""

    receive_count: int | None = None
    """Broker ``ApproximateReceiveCount`` at this delivery, or ``None`` if unreported.

    Read by the FIFO uncounted requeue to copy-back only when one more counted receive
    would make the message eligible for the queue's redrive DLQ."""

    group_id: str | None = None
    """FIFO ``MessageGroupId``, or ``None`` on a standard queue.

    A FIFO copy must re-enter under its original group or it loses group affinity
    entirely; without it the copy-back path is refused and the counted reset is used."""


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

    __poison_queue_url: str | None = attrs.field(default=None, init=False)
    """Retention queue for undecodable FIFO messages (``SQSConfig.poison_queue_url``)."""

    __pending: dict[str, tuple[str, str, _RetainedRaw | None]] = attrs.field(
        factory=dict, init=False
    )
    """In-flight deliveries: message id -> (queue, receipt handle, retained raw).

    Mirrors the RabbitMQ client's pending map so callers ack/nack with the
    message ``id`` exposed on :class:`SQSQueueMessage` while the client
    resolves the per-delivery ``ReceiptHandle`` internally. A redelivered
    message (same ``MessageId``) overwrites its entry — only the latest
    receipt handle is valid.

    The third slot is the raw wire form (body, attributes, receive count, group id) kept
    for every path that may re-send the delivery: a FIFO poison park and the uncounted
    requeue (see :class:`_RetainedRaw`).
    """

    __redrive_max_cache: dict[str, int | None] = attrs.field(factory=dict, init=False)
    """queue URL -> the redrive policy's ``maxReceiveCount``, ``None`` when unset.

    Populated lazily by the FIFO uncounted requeue (one ``GetQueueAttributes`` per queue
    per client lifetime); a fetch failure is not cached, so a transient error cannot
    permanently disable the near-threshold copy-back."""

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

            self.__enqueue_batch_concurrency = clamp(pool_cap, 1, _MAX_ENQUEUE_BATCH_CONCURRENCY)
            self.__poison_queue_url = config.poison_queue_url if config is not None else None

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
            logger.trace("SQS client connected", endpoint=endpoint)

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
                self.__poison_queue_url = None
                self.__queue_url_cache.clear()

                async with self.__pending_lock:
                    self.__pending.clear()

                logger.trace("SQS client closed")

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
        return queue.startswith(("https://", "http://"))

    # ....................... #

    @staticmethod
    def __sanitize_queue_name(queue: str) -> str:
        """Normalize queue name for SQS-compatible backends.

        Replaces unsupported characters with ``_`` and preserves the ``.fifo`` suffix. Fails
        closed on an over-length name rather than truncating: SQS caps a queue name at 80 chars
        (75 + ``.fifo`` for FIFO), and silently truncating would let two distinct logical names
        collapse onto one physical queue (cross-talk).
        """
        is_fifo = queue.endswith(".fifo")
        base = queue[:-5] if is_fifo else queue
        base = _RE_UNSUPPORTED_CHARS.sub("_", base)
        base = _RE_MULTI_UNDERSCORE.sub("_", base).strip("_") or "queue"

        max_base = 75 if is_fifo else 80
        if len(base) > max_base:
            raise exc.precondition(
                f"SQS queue name {queue!r} exceeds the {max_base}-char limit after "
                f"sanitization ({base!r}); shorten it — truncating would alias distinct queues.",
                code="sqs.queue_name_too_long",
            )

        return f"{base}.fifo" if is_fifo else base

    # ....................... #

    @staticmethod
    def __is_fifo_target(queue: str, queue_url: str) -> bool:
        if queue.endswith(".fifo"):
            return True

        return queue_url.rstrip("/").rsplit("/", 1)[-1].endswith(".fifo")

    # ....................... #

    def __create_client_cm(self) -> AbstractAsyncContextManager[AsyncSQSClient]:
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

        return cast("AbstractAsyncContextManager[AsyncSQSClient]", cm)

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

        # Serve an already-resolved URL before re-validating the name: once we've talked to a
        # queue, it must stay ack/nack-able even if its logical name trips the length guard —
        # re-sanitizing here would raise and strand pending deliveries in a redelivery loop. Only
        # the first resolution of a never-seen name pays the fail-closed check below.
        if queue in self.__queue_url_cache:
            return self.__queue_url_cache[queue]

        queue_name = self.__sanitize_queue_name(queue)

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
        """Build the SQS ``MessageAttributes`` map from headers and transport metadata.

        Caller *headers* pass through verbatim as ``String`` attributes; the reserved
        transport attributes (the base64 encoding marker, plus type/key/enqueued-at when
        given) are written afterwards so they win on a name collision. AWS caps a message
        at 10 attributes total, and headers count against that limit.

        Args:
            type (str | None): Message type for the reserved type attribute; omitted
                when ``None``.
            key (str | None): Message/ordering key for the reserved key attribute;
                omitted when ``None``.
            enqueued_at (datetime | None): Enqueue time serialized ISO-8601 into the
                reserved timestamp attribute; omitted when ``None``.
            headers (Mapping[str, str] | None): Caller headers carried as ``String``
                attributes, overridden by any reserved attribute of the same name.

        Returns:
            dict[str, dict[str, str]]: Attribute name to its
            ``{"StringValue", "DataType"}`` entry, ready for
            ``SendMessage``/``SendMessageBatch``.
        """

        attrs_: dict[str, dict[str, str]] = {}

        if headers:
            for header_key, header_value in headers.items():
                attrs_[header_key] = {
                    "StringValue": header_value,
                    "DataType": "String",
                }

        attrs_[_ENCODING_ATTR] = {"StringValue": _ENCODING_B64, "DataType": "String"}

        if type is not None:
            attrs_[_TYPE_ATTR] = {"StringValue": type, "DataType": "String"}

        if key is not None:
            attrs_[_KEY_ATTR] = {"StringValue": key, "DataType": "String"}

        if enqueued_at is not None:
            attrs_[_ENQUEUED_AT_ATTR] = {
                "StringValue": enqueued_at.isoformat(),
                "DataType": "String",
            }

        return attrs_

    # ....................... #

    @staticmethod
    def __extract_attr(
        attrs_: dict[str, dict[str, str]] | None,
        key: str,
    ) -> str | None:
        if not attrs_:
            return None

        raw = attrs_.get(key)

        if not isinstance(raw, dict):
            return None

        value = raw.get("StringValue")
        return value if isinstance(value, str) else None

    # ....................... #

    @staticmethod
    def __extract_headers(
        attrs_: dict[str, dict[str, str]] | None,
    ) -> dict[str, str] | None:
        """Return caller-visible string headers from message attributes.

        Reserved transport attributes are excluded; only ``String`` values
        survive — the port contract is string-to-string.
        """

        if not attrs_:
            return None

        out: dict[str, str] = {}

        for attr_key, raw in attrs_.items():
            if attr_key in _RESERVED_ATTRS or not isinstance(raw, dict):  # pyright: ignore[reportUnnecessaryIsInstance]
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

        return int(raw) if isinstance(raw, str) and raw.isdigit() else None

    # ....................... #

    @staticmethod
    def __encode_body(body: bytes) -> str:
        return base64.b64encode(body).decode("ascii")

    # ....................... #

    @staticmethod
    def __decode_body(
        body: str,
        attrs_: dict[str, dict[str, str]] | None,
    ) -> bytes:
        encoding = SQSClient.__extract_attr(attrs_, _ENCODING_ATTR)

        if encoding == _ENCODING_B64:
            try:
                return base64.b64decode(body, validate=True)

            except Exception as e:
                raise exc.internal("SQS message payload is not valid base64.") from e

        return body.encode("utf-8")

    # ....................... #

    @staticmethod
    def __extract_enqueued_at(
        attrs_: dict[str, dict[str, str]] | None,
        system_attrs: dict[str, str] | None,
    ) -> datetime | None:
        if from_message_attr := SQSClient.__extract_attr(attrs_, _ENQUEUED_AT_ATTR):
            with suppress(ValueError):
                return datetime.fromisoformat(from_message_attr)

        if system_attrs:
            sent = system_attrs.get("SentTimestamp")

            if sent and sent.isdigit():
                return datetime.fromtimestamp(int(sent) / 1000.0, tz=UTC)

        return None

    # ....................... #

    @staticmethod
    def __chunked_pending(
        pending: Sequence[tuple[str, str, _RetainedRaw | None]],
        size: int = 10,
    ) -> list[list[tuple[str, str, _RetainedRaw | None]]]:
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

        return None if seconds <= 0 else seconds

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
        max_batch_payload_bytes: int = SQS_DEFAULT_MAX_BATCH_PAYLOAD_BYTES,
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

        A FIFO target rejects a per-message *delay* / *not_before* — SQS allows only a
        queue-level delay on a FIFO queue — so it raises ``sqs.fifo_per_message_delay`` rather
        than letting the broker reject the whole batch; use a queue-level delay or a standard
        queue.

        Splits into chunks bounded by **both** the 10-entry count
        (:data:`_SQS_SEND_MESSAGE_BATCH_MAX`) and *max_batch_payload_bytes* (the queue's total
        request-payload limit; default :data:`SQS_DEFAULT_MAX_BATCH_PAYLOAD_BYTES` = 256 KiB,
        raise it per route for a queue configured up to AWS's 1 MiB ceiling). A single message
        exceeding the byte limit raises rather than letting the broker reject the request.
        Multiple chunks are sent with bounded concurrency derived from
        ``max_pool_connections`` in :meth:`initialize` so large publishes do not
        serialize on the network while staying within the HTTP connection pool.
        """

        if not bodies:
            return []

        if message_ids is not None and len(message_ids) != len(bodies):
            raise exc.precondition("SQS message_ids size must match batch body size")

        if message_headers is not None and len(message_headers) != len(bodies):
            raise exc.precondition("SQS message_headers size must match batch body size")

        # Per-message effective headers (shared headers overridden by the
        # per-message entry) when message_headers is given; otherwise every
        # message rides the shared batch-wide headers.
        per_message_headers: list[Mapping[str, str] | None]

        if message_headers is not None:
            per_message_headers = [{**(headers or {}), **mh} for mh in message_headers]
        else:
            per_message_headers = [headers] * len(bodies)

        if message_ids is not None:
            resolved_ids = list(message_ids)

        elif message_headers is not None:
            # Each entry dedups on its own event id; absent one, a fresh random
            # id keeps distinct messages from colliding.
            resolved_ids = [mh.get(HEADER_EVENT_ID) or uuid4().hex for mh in message_headers]

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
        delay_seconds = self._resolve_sqs_delay_seconds(delay=delay, not_before=not_before)

        if is_fifo and delay_seconds is not None:
            # SQS rejects per-message DelaySeconds on a FIFO queue (only a queue-level delay is
            # allowed there), so the whole batch send would fail — fail closed with a clear reason.
            raise exc.precondition(
                f"SQS FIFO queue {queue!r} does not support per-message delay; configure a "
                "queue-level delay or send to a standard queue.",
                code="sqs.fifo_per_message_delay",
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

                raise exc.internal(f"SQS send_message_batch has failed entries: {failed_ids}")

            # Map entry ids ("m{i}") back to broker-assigned MessageIds so the
            # returned identifiers correlate with received ``message.id``.
            by_entry: dict[str, str] = {}

            for success in resp.get("Successful") or []:
                entry_id = success.get("Id")
                broker_id = success.get("MessageId")

                if isinstance(entry_id, str) and isinstance(  # pyright: ignore[reportUnnecessaryIsInstance]
                    broker_id, str
                ):
                    by_entry[entry_id] = broker_id

            return [by_entry.get(f"m{i}", fallback_id) for i, fallback_id in enumerate(chunk_ids)]

        # Pack into batches bounded by BOTH limits: at most 10 entries (count) and at most
        # 256 KiB total (size). The size accounting is O(1) per message — the base64 body
        # length is arithmetic (``ceil(n/3)*4``, no encoding), plus the caller-header bytes
        # and a reserve for the transport attributes — so it is always on, never disabled.
        def _entry_size(index: int) -> int:
            entry_headers = per_message_headers[index]
            header_bytes = (
                sum(
                    len(name.encode("utf-8")) + 6 + len(value.encode("utf-8"))
                    for name, value in entry_headers.items()
                )
                if entry_headers
                else 0
            )
            body_b64_bytes = ((len(bodies[index]) + 2) // 3) * 4
            return body_b64_bytes + header_bytes + _SQS_RESERVED_ATTR_BYTES

        chunks: list[tuple[list[bytes], list[str], list[Mapping[str, str] | None]]] = []
        chunk_indices: list[int] = []
        chunk_bytes = 0

        for index in range(len(bodies)):
            size = _entry_size(index)

            if size > max_batch_payload_bytes:
                raise exc.precondition(
                    f"SQS message {index} is ~{size} bytes (body + attributes), exceeding "
                    f"the {max_batch_payload_bytes}-byte per-message/batch limit "
                    "(raise SQSQueueConfig.max_batch_payload_bytes for a higher-limit queue)."
                )

            if chunk_indices and (
                len(chunk_indices) >= _SQS_SEND_MESSAGE_BATCH_MAX
                or chunk_bytes + size > max_batch_payload_bytes
            ):
                chunks.append(
                    (
                        [bodies[i] for i in chunk_indices],
                        [resolved_ids[i] for i in chunk_indices],
                        [per_message_headers[i] for i in chunk_indices],
                    )
                )
                chunk_indices = []
                chunk_bytes = 0

            chunk_indices.append(index)
            chunk_bytes += size

        if chunk_indices:
            chunks.append(
                (
                    [bodies[i] for i in chunk_indices],
                    [resolved_ids[i] for i in chunk_indices],
                    [per_message_headers[i] for i in chunk_indices],
                )
            )

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
                chunk_results[index] = await _send_chunk(chunk, chunk_ids, chunk_headers)

        async with asyncio.TaskGroup() as tg:
            for index, (ch, ids, hdrs) in enumerate(chunks):
                tg.create_task(_bounded(index, ch, ids, hdrs))

        return [broker_id for chunk_ids in chunk_results for broker_id in chunk_ids]

    # ....................... #

    async def __send_poison_copy(
        self,
        *,
        source_queue: str,
        source_queue_url: str,
        message_id: str,
        body: str,
        attributes: dict[str, Any] | None,
    ) -> None:
        """Send a raw copy of an undecodable message to the configured poison queue.

        The still-encoded ``Body`` goes out verbatim (byte-identical) together with the
        original message attributes, plus provenance attributes carrying the source queue
        and original ``MessageId`` (the copy gets a new broker-assigned one). The original
        attributes win the per-message attribute cap — they are the routing/decoding
        context replay needs, and an SQS original carries at most the cap itself, so they
        always all fit; a provenance attribute that no longer fits is dropped with a
        warning naming it (the same facts are in the poison log line, and on FIFO the
        original id doubles as the group/dedup id).

        A ``.fifo`` poison queue needs FIFO entry fields: the original message id serves
        as both ``MessageGroupId`` (each poison is its own group, so one poison can never
        block another — poison ordering is meaningless) and ``MessageDeduplicationId``
        (a redelivered original never lands twice within the dedup window). A standard
        poison queue needs neither and is the recommended shape.

        Raises on failure — the caller deletes the original either way (unblocking the
        message group is the primary duty) and upgrades its log to ERROR.
        """

        poison_queue_url = self.__poison_queue_url

        if poison_queue_url is None:
            raise exc.internal("SQS poison queue URL is not configured")

        if poison_queue_url.rstrip("/") == source_queue_url.rstrip("/"):
            # A self-targeted copy would land back on the queue it poisoned and loop
            # (copy → undecodable → copy …). Raising here follows the caller's existing
            # failure path: the original is still deleted, the loss logged as an error.
            raise exc.internal(
                "SQS poison queue must differ from the source queue "
                f"(both resolve to {poison_queue_url!r})"
            )

        message_attributes: dict[str, Any] = dict(attributes) if attributes else {}

        # The original id first: it is the correlation key back to the source log
        # line (and the FIFO group/dedup id below), so under slot pressure it
        # outlives the source-queue attribute.
        provenance: tuple[tuple[str, dict[str, str]], ...] = (
            (_POISON_SOURCE_ID_ATTR, {"StringValue": message_id, "DataType": "String"}),
            (
                _POISON_SOURCE_QUEUE_ATTR,
                {"StringValue": source_queue, "DataType": "String"},
            ),
        )

        dropped: list[str] = []

        for name, value in provenance:
            if name in message_attributes:
                continue

            if len(message_attributes) >= _SQS_MAX_MESSAGE_ATTRIBUTES:
                dropped.append(name)
                continue

            message_attributes[name] = value

        if dropped:
            logger.warning(
                "SQS poison copy has no free attribute slots for provenance",
                queue=source_queue,
                message_id=message_id,
                dropped=dropped,
            )

        payload: dict[str, Any] = {
            "QueueUrl": poison_queue_url,
            "MessageBody": body,
            "MessageAttributes": message_attributes,
        }

        if poison_queue_url.rstrip("/").endswith(".fifo"):
            payload["MessageGroupId"] = message_id
            payload["MessageDeduplicationId"] = message_id

        await self.__require_client().send_message(**payload)

    # ....................... #

    async def __retain_and_delete_fifo_poison(
        self,
        *,
        queue: str,
        queue_url: str,
        message_id: str,
        receipt: str,
        raw: _RetainedRaw | None,
        reason: str,
    ) -> None:
        """Retain a raw copy of a FIFO poison message, then delete it to unblock its group.

        A FIFO message that is discarded but never deleted stays at the head of its message
        group and blocks every later message in that group until the visibility timeout,
        then redelivers at the head again — a permanent deadlock when no redrive policy
        trims it. Deleting is the only thing that frees the group.

        **The delete is conditional on the message being safe.** It happens when the copy
        reached the retention queue, or when no retention queue is configured and
        destruction was therefore accepted at wiring time. If retention *was* configured
        and the copy could not be sent, the original is left in place: deleting would put
        the message on neither queue, turning a transient outage of the retention queue
        into permanent data loss. The group stays blocked in that case, which is
        recoverable — the message reappears after its visibility timeout and the next
        attempt retries the copy — where destruction is not.

        The delete itself is best-effort: a failure only leaves the group blocked until
        redrive/visibility, no worse than before. Callers put the (bounded, non-secret)
        *why* in *reason* — never the raw body, which can carry production data into
        central error logs.

        :param raw: Wire form to copy, or ``None`` when nothing was retained for this
            delivery.
        :param reason: Short phrase describing why the message is poison, for the logs.
        """

        if self.__poison_queue_url is None:
            # No retention target: destruction was accepted at configuration time, and the
            # log names the knob that would have prevented it.
            logger.warning(
                "SQS FIFO message %s on queue %s %s; deleting to unblock its message group "
                "— the message is destroyed (set SQSConfig(poison_queue_url=...) to retain "
                "a raw copy)",
                message_id,
                queue,
                reason,
            )

        elif raw is None:
            logger.error(
                "SQS FIFO message %s on queue %s %s, but no raw copy was retained for it, "
                "so none can be sent to %s; leaving it in place — its message group stays "
                "blocked until the message is retried or the queue's redrive policy trims it",
                message_id,
                queue,
                reason,
                self.__poison_queue_url,
            )

            return

        else:
            try:
                await self.__send_poison_copy(
                    source_queue=queue,
                    source_queue_url=queue_url,
                    message_id=message_id,
                    body=raw.body,
                    attributes=raw.attributes,
                )

            except Exception as send_err:
                # Retention was configured and did not happen. Deleting now would put the
                # message on neither queue, turning a throttled or unavailable poison queue
                # into permanent data loss — so the group stays blocked instead. This is
                # self-healing: the message becomes visible again, and the next attempt
                # retries the copy once the retention queue recovers.
                logger.error(
                    "SQS FIFO message %s on queue %s %s and its raw copy could not be sent "
                    "to %s (%s); leaving it in place rather than destroying it — its message "
                    "group stays blocked until the retention queue recovers",
                    message_id,
                    queue,
                    reason,
                    self.__poison_queue_url,
                    send_err,
                )

                return

            logger.warning(
                "SQS FIFO message %s on queue %s %s; raw copy retained on %s; deleting "
                "the original to unblock its message group",
                message_id,
                queue,
                reason,
                self.__poison_queue_url,
            )

        try:
            await self.__require_client().delete_message(QueueUrl=queue_url, ReceiptHandle=receipt)

        except Exception as del_err:
            logger.warning(
                "SQS FIFO poison delete failed for message %s on queue %s: %s",
                message_id,
                queue,
                del_err,
            )

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
            wait_time = clamp(int(timeout.total_seconds()), 0, 20)

        queue_url = await self.__resolve_queue_url(queue)
        c = self.__require_client()
        is_fifo = self.__is_fifo_target(queue, queue_url)

        resp = await c.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=max_messages,
            WaitTimeSeconds=wait_time,
            MessageAttributeNames=["All"],
            AttributeNames=["SentTimestamp", "ApproximateReceiveCount", "MessageGroupId"],
        )
        raw_messages = resp.get("Messages") or []
        out: list[SQSQueueMessage] = []

        # Wire form kept per delivery, for every path that may re-send it: a FIFO poison
        # park (copy to the retention queue before deleting off the group head), and an
        # uncounted requeue (``count=False``) — every delivery on a standard queue, and a
        # FIFO delivery whose receive count nears the redrive threshold. Held only while
        # the delivery is in flight, so the cost is bounded by the batch size.
        retained: dict[str, _RetainedRaw] = {}

        for raw in raw_messages:
            receipt = raw.get("ReceiptHandle")
            if not isinstance(receipt, str):
                continue

            broker_id = raw.get("MessageId")
            # ``id`` is the broker MessageId: stable across redeliveries and
            # correlatable with enqueue. Fall back to the receipt handle for
            # SQS-compatible backends that omit MessageId.
            message_id = broker_id if isinstance(broker_id, str) and broker_id else receipt

            body = raw.get("Body", "")

            attrs_ = raw.get("MessageAttributes")
            attrs_ = attrs_ if isinstance(attrs_, dict) else None

            system_attrs = raw.get("Attributes")
            system_attrs = system_attrs if isinstance(system_attrs, dict) else None

            try:
                message = SQSQueueMessage(
                    queue=queue,
                    id=message_id,
                    receipt_handle=receipt,
                    body=self.__decode_body(body, attrs_),  # type: ignore[arg-type]
                    type=self.__extract_attr(attrs_, _TYPE_ATTR),  # type: ignore[arg-type]
                    enqueued_at=self.__extract_enqueued_at(attrs_, system_attrs),  # type: ignore[arg-type]
                    key=self.__extract_attr(attrs_, _KEY_ATTR),  # type: ignore[arg-type]
                    headers=self.__extract_headers(attrs_),  # type: ignore[arg-type]
                    delivery_count=self.__extract_delivery_count(system_attrs),  # type: ignore[arg-type]
                )
            except CoreException as e:
                # A malformed message (e.g. a non-base64 body) must not poison the rest of the
                # batch. Standard queue: skip it — left in-flight, so SQS redelivery and the
                # queue's redrive policy (maxReceiveCount → DLQ) handle the poison, while the good
                # messages in this batch are still returned and registered for ack/nack.
                if not is_fifo:
                    logger.warning(
                        "SQS message %s on queue %s could not be decoded, skipping: %s",
                        message_id,
                        queue,
                        e,
                    )
                    continue

                # FIFO queue: a skipped-but-undeleted message stays at the head of its message
                # group and blocks every later message in that group. The raw body is NOT
                # logged (a malformed/attacker-supplied payload can carry production data and
                # this lands in central error logs) — only its size and the bounded decode
                # error ride along in the reason.
                await self.__retain_and_delete_fifo_poison(
                    queue=queue,
                    queue_url=queue_url,
                    message_id=message_id,
                    receipt=receipt,
                    raw=_RetainedRaw(body=body, attributes=attrs_),  # type: ignore[arg-type]
                    reason=f"could not be decoded (body {len(body)} bytes): {e}",
                )
                continue

            group_id = (system_attrs or {}).get("MessageGroupId")
            retained[message_id] = _RetainedRaw(
                body=body,  # type: ignore[arg-type]
                attributes=attrs_,  # type: ignore[arg-type]
                receive_count=message.delivery_count,
                group_id=group_id if isinstance(group_id, str) else None,
            )

            out.append(message)

        if out:
            async with self.__pending_lock:
                for message in out:
                    # A redelivery (same MessageId) supersedes the previous
                    # receipt handle; the latest one is the only valid one.
                    self.__pending[message.id] = (
                        queue,
                        message.receipt_handle,
                        retained.get(message.id),
                    )

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
            timeout.total_seconds() if timeout is not None and timeout.total_seconds() > 0 else None
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

            except Exception as e:
                # Log so a persistently failing receive is observable rather than a silent retry
                # loop, then back off; the idle deadline (when set) still terminates.
                logger.warning(
                    "SQS consume receive failed on queue %s; backing off %.1fs: %s",
                    queue,
                    backoff,
                    e,
                )
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
    ) -> list[tuple[str, str, _RetainedRaw | None]]:
        """Resolve message ids to ``(id, receipt handle, retained raw)`` for *queue*.

        The retained raw rides along under the same lock acquisition so a FIFO poison park
        cannot race an ack that drops the entry between resolving the receipt and reading
        the copy it is about to send.
        """

        async with self.__pending_lock:
            out: list[tuple[str, str, _RetainedRaw | None]] = []

            for message_id in ids:
                entry = self.__pending.get(message_id)

                if entry is None:
                    continue

                pending_queue, receipt, retained = entry

                if pending_queue != queue:
                    continue

                out.append((message_id, receipt, retained))

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
                {"Id": f"m{i}", "ReceiptHandle": receipt} for i, (_, receipt, _) in enumerate(chunk)
            ]

            resp = await c.delete_message_batch(
                QueueUrl=queue_url,
                Entries=entries,  # type: ignore[arg-type]
            )

            if failed := resp.get("Failed") or []:
                failed_ids = ", ".join(f.get("Id", "unknown") for f in failed)

                raise exc.internal(f"SQS delete_message_batch has failed entries: {failed_ids}")

            acked_ids.extend(message_id for message_id, _, _ in chunk)

        await self.__drop_pending_many(acked_ids)

        return len(acked_ids)

    # ....................... #

    async def __park_poison(
        self,
        queue: str,
        pending: Sequence[tuple[str, str, _RetainedRaw | None]],
    ) -> None:
        """Terminal disposition for ``nack(requeue=False)``.

        On a **standard** queue this is deliberately a no-op: leaving the message invisible
        is what lets SQS's redrive policy count the receive and dead-letter it natively.

        On a **FIFO** queue that would block the message group forever, so each message is
        retained (when a target is configured) and deleted instead — see
        :meth:`__retain_and_delete_fifo_poison`.
        """

        queue_url = await self.__resolve_queue_url(queue)

        if not self.__is_fifo_target(queue, queue_url):
            return

        for message_id, receipt, retained in pending:
            await self.__retain_and_delete_fifo_poison(
                queue=queue,
                queue_url=queue_url,
                message_id=message_id,
                receipt=receipt,
                raw=retained,
                reason="was nacked as poison by the consumer",
            )

    # ....................... #

    async def __reset_visibility(
        self,
        queue: str,
        queue_url: str,
        pending: Sequence[tuple[str, str, _RetainedRaw | None]],
    ) -> None:
        """Reset visibility to ``0`` for *pending* deliveries (the counted requeue).

        Best effort — a failed reset is logged and the message still redelivers once its
        original visibility timeout lapses.
        """

        c = self.__require_client()

        for chunk in self.__chunked_pending(pending):
            entries = [
                {
                    "Id": f"m{i}",
                    "ReceiptHandle": receipt,
                    "VisibilityTimeout": 0,
                }
                for i, (_, receipt, _) in enumerate(chunk)
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

    # ....................... #

    async def __max_receive_count(self, queue_url: str) -> int | None:
        """The queue's redrive ``maxReceiveCount``, or ``None`` without a redrive policy.

        Cached per queue URL on success; a fetch failure returns ``None`` for this call
        without caching, so the near-threshold copy-back degrades to the plain reset for
        one cycle instead of being disabled for the client's lifetime.
        """

        if queue_url in self.__redrive_max_cache:
            return self.__redrive_max_cache[queue_url]

        try:
            resp = await self.__require_client().get_queue_attributes(
                QueueUrl=queue_url,
                AttributeNames=["RedrivePolicy"],
            )
            policy = (resp.get("Attributes") or {}).get("RedrivePolicy")
            max_rc = int(json.loads(policy)["maxReceiveCount"]) if policy else None

        except Exception as e:
            logger.warning(
                "SQS could not read the redrive policy for %s (%s); treating it as "
                "absent for this nack",
                queue_url,
                e,
            )
            return None

        self.__redrive_max_cache[queue_url] = max_rc
        return max_rc

    # ....................... #

    @staticmethod
    def __nears_redrive(retained: _RetainedRaw | None, max_rc: int | None) -> bool:
        """Whether one more counted receive could make this delivery DLQ-eligible."""

        if max_rc is None or retained is None or retained.receive_count is None:
            return False

        return retained.receive_count >= max(1, max_rc - 1)

    # ....................... #

    async def __requeue_uncounted(
        self,
        *,
        queue: str,
        queue_url: str,
        pending: Sequence[tuple[str, str, _RetainedRaw | None]],
        fifo: bool = False,
    ) -> None:
        """Requeue deliveries as fresh copies so the broker receive count resets.

        The receive tally is broker-managed and grows on every receive no matter why the
        message went back, so enough uncounted requeues (a prolonged KMS key outage, many
        rolling deploys) would cross the redrive policy's ``maxReceiveCount`` and
        dead-letter messages that were never poison. Sending a byte-identical copy — new
        broker ``MessageId``, receive count zero — and then deleting the original is the
        only way SQS can honor "this redelivery is not the message's fault".

        Copy first, delete only after the copy is on the queue. A failed copy falls back
        to a plain visibility reset: the counted requeue, degraded but never lossy. A
        failed delete after a successful copy leaves a duplicate, which the consumer-side
        inbox dedup absorbs (its identity headers ride in the copied attributes).

        A FIFO copy re-enters at the back of its message group — the caller only sends a
        FIFO delivery here when its receive count nears the redrive threshold, where the
        broker was about to break group order anyway by dead-lettering the head. The copy
        keeps the original ``MessageGroupId`` (group affinity survives) and carries a
        deterministic ``MessageDeduplicationId`` derived from the original id and receive
        count, so a retried copy after a failed delete dedups instead of duplicating.
        """

        c = self.__require_client()

        for message_id, receipt, retained in pending:
            if retained is None or (fifo and retained.group_id is None):
                # No raw copy (or a FIFO delivery whose group is unknown, which a copy
                # would detach) — the counted fallback keeps the message safe.
                await self.__reset_visibility(queue, queue_url, [(message_id, receipt, retained)])
                continue

            payload: dict[str, Any] = {
                "QueueUrl": queue_url,
                "MessageBody": retained.body,
            }

            if retained.attributes:
                payload["MessageAttributes"] = retained.attributes

            if fifo:
                payload["MessageGroupId"] = retained.group_id
                payload["MessageDeduplicationId"] = f"{message_id}-r{retained.receive_count or 0}"

            try:
                await c.send_message(**payload)

            except Exception as send_err:
                logger.warning(
                    "SQS uncounted requeue could not send a copy of message %s on "
                    "queue %s (%s); falling back to a counted visibility reset",
                    message_id,
                    queue,
                    send_err,
                )
                await self.__reset_visibility(queue, queue_url, [(message_id, receipt, retained)])
                continue

            try:
                await c.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt)

            except Exception as del_err:
                logger.warning(
                    "SQS uncounted requeue copied message %s on queue %s but could not "
                    "delete the original (%s); it will redeliver alongside its copy and "
                    "the consumer-side inbox dedup absorbs the duplicate",
                    message_id,
                    queue,
                    del_err,
                )

    # ....................... #

    @exc_interceptor.coroutine("sqs.nack")  # type: ignore[untyped-decorator]
    async def nack(
        self,
        queue: str,
        ids: Sequence[str],
        *,
        requeue: bool = True,
        count: bool = True,
    ) -> int:
        """Negative-acknowledge messages.

        When ``requeue`` is ``True`` and ``count`` is ``True``, the visibility timeout is
        reset to ``0`` (best effort — a failed reset is logged and the message still
        redelivers once its original visibility timeout lapses). The redelivery raises
        the broker-managed receive count, as any SQS receive does.

        When ``requeue`` is ``True`` and ``count`` is ``False`` — a redelivery that is
        not the message's fault (a drain refusal, a decrypt blocked on a disabled KMS
        key) — a **standard** queue requeues a byte-identical *copy* and deletes the
        original, so the receive count genuinely resets and the redrive policy cannot
        dead-letter a message over an outage it did not cause (a failed copy falls back
        to the counted reset). A **FIFO** queue keeps the order-preserving visibility
        reset while it safely can, and switches to the copy — same ``MessageGroupId``,
        re-entering at the back of its group — only when one more counted receive could
        cross the redrive policy's ``maxReceiveCount``: at that point the broker was
        about to break group order anyway by dead-lettering the head, and the copy keeps
        the message on the queue instead. A FIFO queue with no redrive policy always
        resets (no DLQ exists to protect against, and blocking the group head preserves
        order outright).

        When ``requeue`` is ``False`` on a **standard** queue the message is *not*
        deleted: it stays invisible until its visibility timeout lapses, so the queue's
        redrive policy counts the receive and eventually dead-letters it (SQS's native
        DLQ mechanism).

        When ``False`` on a **FIFO** queue that same treatment would wedge the queue: an
        undeleted message sits at the head of its message group and blocks every later
        message in it, redelivering at the head forever when no redrive policy trims it —
        exactly the in-app ``max_deliveries`` shape, where the caller has already decided
        the message is poison and nothing else will ever remove it. So a FIFO poison nack
        takes the same path the undecodable-message branch of ``receive`` does: retain a raw
        copy on ``poison_queue_url`` when configured, then delete to free the group.

        *ids* are message ids as exposed on :class:`SQSQueueMessage`; ids
        without a pending delivery on this client are skipped.
        """
        if not ids:
            return 0

        pending = await self.__pending_by_ids(queue, ids)

        if not pending:
            return 0

        nacked_ids = [message_id for message_id, _, _ in pending]

        if not requeue:
            await self.__park_poison(queue, pending)

        else:
            queue_url = await self.__resolve_queue_url(queue)

            if count:
                await self.__reset_visibility(queue, queue_url, pending)

            elif not self.__is_fifo_target(queue, queue_url):
                await self.__requeue_uncounted(queue=queue, queue_url=queue_url, pending=pending)

            else:
                # FIFO: reset while order-preservation is free; copy-back only the
                # deliveries one counted receive away from the redrive DLQ.
                max_rc = await self.__max_receive_count(queue_url)
                near = [p for p in pending if self.__nears_redrive(p[2], max_rc)]
                rest = [p for p in pending if not self.__nears_redrive(p[2], max_rc)]

                if rest:
                    await self.__reset_visibility(queue, queue_url, rest)

                if near:
                    await self.__requeue_uncounted(
                        queue=queue, queue_url=queue_url, pending=near, fifo=True
                    )

        await self.__drop_pending_many(nacked_ids)

        return len(nacked_ids)
