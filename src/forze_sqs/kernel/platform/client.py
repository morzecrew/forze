from forze_sqs._compat import require_sqs

require_sqs()

# ....................... #

import base64
import re
from contextlib import asynccontextmanager
from re import Pattern
from contextvars import ContextVar
from datetime import datetime, timedelta, timezone
from typing import Any, AsyncIterator, Sequence, TypedDict, cast, final
from uuid import uuid4

import aioboto3
import attrs
from botocore.config import Config as AioConfig
from pydantic import SecretStr
from types_aiobotocore_sqs.client import SQSClient as AsyncSQSClient

from forze.base.errors import CoreError, InfrastructureError

from .errors import sqs_handled
from .types import SQSQueueMessage

# ----------------------- #

_TYPE_ATTR = "forze_type"
_KEY_ATTR = "forze_key"
_ENQUEUED_AT_ATTR = "forze_enqueued_at"
_ENCODING_ATTR = "forze_encoding"
_ENCODING_B64 = "b64"

_RE_UNSUPPORTED_CHARS: Pattern[str] = re.compile(r"[^A-Za-z0-9_-]")
_RE_MULTI_UNDERSCORE: Pattern[str] = re.compile(r"_+")

# ....................... #


@final
class SQSConfig(TypedDict, total=False):
    """SQS optional configuration (botocore config)."""

    region_name: str
    signature_version: str
    user_agent: str
    user_agent_extra: str
    connect_timeout: int | float  #! TODO: use timedelta
    read_timeout: int | float  #! TODO: use timedelta
    parameter_validation: bool
    max_pool_connections: int
    proxies: dict[str, str]
    client_cert: str | tuple[str, str]
    inject_host_prefix: bool
    use_dualstack_endpoint: bool
    use_fips_endpoint: bool
    tcp_keepalive: bool
    request_min_compression_size_bytes: int


# ....................... #


@final
@attrs.define(frozen=True, slots=True, kw_only=True)
class _SQSConnectionOpts:
    """SQS connection options."""

    endpoint: str
    region_name: str  #! Should NOT be required
    access_key_id: str
    secret_access_key: str | SecretStr
    config: AioConfig | None = None


# ....................... #


@final
@attrs.define(slots=True)
class SQSClient:
    __opts: _SQSConnectionOpts | None = attrs.field(default=None, init=False)
    __session: aioboto3.Session | None = attrs.field(default=None, init=False)

    __ctx_client: ContextVar[AsyncSQSClient | None] = attrs.field(
        factory=lambda: ContextVar("sqs_client", default=None),
        init=False,
    )
    __ctx_depth: ContextVar[int] = attrs.field(
        factory=lambda: ContextVar("sqs_depth", default=0),
        init=False,
    )

    __queue_url_cache: dict[str, str] = attrs.field(factory=dict, init=False)

    # ....................... #
    # Lifecycle

    async def initialize(
        self,
        endpoint: str,
        access_key_id: str,
        secret_access_key: str | SecretStr,
        *,
        region_name: str,
        config: SQSConfig | None = None,
    ) -> None:
        """Initialize the SQS session with endpoint and credentials."""
        if self.__session is not None:
            return

        aio_config = AioConfig(**config) if config else None
        self.__opts = _SQSConnectionOpts(
            endpoint=endpoint,
            region_name=region_name,
            access_key_id=access_key_id,
            secret_access_key=secret_access_key,
            config=aio_config,
        )
        self.__session = aioboto3.Session()

    # ....................... #

    def close(self) -> None:
        """Drop the current session and queue URL cache."""
        self.__session = None
        self.__opts = None
        self.__queue_url_cache.clear()

    # ....................... #

    def __require_session(self) -> aioboto3.Session:
        if self.__session is None:
            raise CoreError("SQS session is not initialized")

        return self.__session

    # ....................... #

    def __current_client(self) -> AsyncSQSClient | None:
        return self.__ctx_client.get()

    # ....................... #

    def __require_client(self) -> AsyncSQSClient:
        c = self.__current_client()

        if c is None:
            raise CoreError("SQS client is not initialized")

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

    @asynccontextmanager
    async def client(self) -> AsyncIterator[AsyncSQSClient]:
        """Yield a context-bound SQS client with nested-scope reuse."""
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
            raise CoreError("SQS client options are not initialized")

        sec_key = opts.secret_access_key

        if isinstance(sec_key, SecretStr):
            sec_key = sec_key.get_secret_value()

        cm = session.client(  # type: ignore
            "sqs",
            endpoint_url=opts.endpoint,
            region_name=opts.region_name,
            aws_access_key_id=opts.access_key_id,
            aws_secret_access_key=sec_key,
            config=opts.config,  # type: ignore[arg-type]
        )
        cm = cast(AsyncSQSClient, cm)

        async with cm as c:
            token_client = self.__ctx_client.set(c)
            token_depth = self.__ctx_depth.set(1)

            try:
                yield c

            finally:
                self.__ctx_client.reset(token_client)
                self.__ctx_depth.reset(token_depth)

    # ....................... #

    @sqs_handled("sqs.health")  # type: ignore[untyped-decorator]
    async def health(self) -> tuple[str, bool]:
        """Check SQS client health by listing queues."""

        c = self.__require_client()

        try:
            await c.list_queues(MaxResults=1)
            return "ok", True

        except Exception as e:
            return str(e), False

    # ....................... #

    @sqs_handled("sqs.create_queue")  # type: ignore[untyped-decorator]
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

    @sqs_handled("sqs.get_queue_url")  # type: ignore[untyped-decorator]
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
    ) -> dict[str, dict[str, str]]:
        attrs: dict[str, dict[str, str]] = {
            _ENCODING_ATTR: {"StringValue": _ENCODING_B64, "DataType": "String"}
        }

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
                raise InfrastructureError(
                    "SQS message payload is not valid base64."
                ) from e

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
    def __chunked_ids(ids: Sequence[str], size: int = 10) -> list[list[str]]:
        return [list(ids[i : i + size]) for i in range(0, len(ids), size)]

    # ....................... #

    @sqs_handled("sqs.enqueue")  # type: ignore[untyped-decorator]
    async def enqueue(
        self,
        queue: str,
        body: bytes,
        *,
        type: str | None = None,
        key: str | None = None,
        enqueued_at: datetime | None = None,
        message_id: str | None = None,
    ) -> str:
        """Send a single message and return its message identifier."""
        return (
            await self.enqueue_many(
                queue,
                [body],
                type=type,
                key=key,
                enqueued_at=enqueued_at,
                message_ids=[message_id] if message_id is not None else None,
            )
        )[0]

    # ....................... #

    @sqs_handled("sqs.enqueue_many")  # type: ignore[untyped-decorator]
    async def enqueue_many(
        self,
        queue: str,
        bodies: Sequence[bytes],
        *,
        type: str | None = None,
        key: str | None = None,
        enqueued_at: datetime | None = None,
        message_ids: Sequence[str] | None = None,
    ) -> list[str]:
        """Send a batch of messages and return resolved message identifiers."""
        if not bodies:
            return []

        if message_ids is not None and len(message_ids) != len(bodies):
            raise InfrastructureError("SQS message_ids size must match batch body size")

        resolved_ids = (
            list(message_ids)
            if message_ids is not None
            else [uuid4().hex for _ in range(len(bodies))]
        )
        queue_url = await self.__resolve_queue_url(queue)
        attrs = self.__build_message_attributes(
            type=type,
            key=key,
            enqueued_at=enqueued_at,
        )
        is_fifo = self.__is_fifo_target(queue, queue_url)
        c = self.__require_client()

        for offset in range(0, len(bodies), 10):
            chunk = bodies[offset : offset + 10]
            chunk_ids = resolved_ids[offset : offset + 10]
            entries: list[dict[str, Any]] = []

            for i, (body, chunk_message_id) in enumerate(
                zip(chunk, chunk_ids, strict=True)
            ):
                entry: dict[str, Any] = {
                    "Id": f"m{i}",
                    "MessageBody": self.__encode_body(body),
                    "MessageAttributes": attrs,
                }

                if is_fifo:
                    entry["MessageGroupId"] = key or "forze"
                    entry["MessageDeduplicationId"] = chunk_message_id

                entries.append(entry)

            resp = await c.send_message_batch(
                QueueUrl=queue_url,
                Entries=entries,  # type: ignore[arg-type]
            )
            failed = resp.get("Failed") or []

            if failed:
                failed_ids = ", ".join(f.get("Id", "unknown") for f in failed)
                raise InfrastructureError(
                    f"SQS send_message_batch has failed entries: {failed_ids}"
                )

        return resolved_ids

    # ....................... #

    @sqs_handled("sqs.receive")  # type: ignore[untyped-decorator]
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
            AttributeNames=["SentTimestamp"],
        )
        raw_messages = resp.get("Messages") or []
        out: list[SQSQueueMessage] = []

        for raw in raw_messages:
            receipt = raw.get("ReceiptHandle")
            if not isinstance(receipt, str):
                continue

            body = raw.get("Body", "")

            attrs = raw.get("MessageAttributes")
            attrs = attrs if isinstance(attrs, dict) else None

            system_attrs = raw.get("Attributes")
            system_attrs = system_attrs if isinstance(system_attrs, dict) else None

            out.append(
                SQSQueueMessage(
                    queue=queue,
                    id=receipt,
                    body=self.__decode_body(body, attrs),  # type: ignore[arg-type]
                    type=self.__extract_attr(attrs, _TYPE_ATTR),  # type: ignore[arg-type]
                    enqueued_at=self.__extract_enqueued_at(attrs, system_attrs),  # type: ignore[arg-type]
                    key=self.__extract_attr(attrs, _KEY_ATTR),  # type: ignore[arg-type]
                )
            )

        return out

    # ....................... #

    @sqs_handled("sqs.consume")  # type: ignore[untyped-decorator]
    async def consume(
        self,
        queue: str,
        *,
        timeout: timedelta | None = None,
    ) -> AsyncIterator[SQSQueueMessage]:
        """Yield queue messages continuously using long polling."""

        while True:
            messages = await self.receive(queue, limit=1, timeout=timeout)

            if not messages:
                continue

            yield messages[0]

    # ....................... #

    @sqs_handled("sqs.ack")  # type: ignore[untyped-decorator]
    async def ack(self, queue: str, ids: Sequence[str]) -> int:
        """Acknowledge messages by deleting them from the queue."""
        if not ids:
            return 0

        queue_url = await self.__resolve_queue_url(queue)
        acked = 0

        c = self.__require_client()

        for chunk in self.__chunked_ids(ids):
            entries = [
                {"Id": f"m{i}", "ReceiptHandle": receipt}
                for i, receipt in enumerate(chunk)
            ]
            resp = await c.delete_message_batch(
                QueueUrl=queue_url,
                Entries=entries,  # type: ignore[arg-type]
            )
            failed = resp.get("Failed") or []

            if failed:
                failed_ids = ", ".join(f.get("Id", "unknown") for f in failed)
                raise InfrastructureError(
                    f"SQS delete_message_batch has failed entries: {failed_ids}"
                )

            acked += len(resp.get("Successful") or [])

        return acked

    # ....................... #

    @sqs_handled("sqs.nack")  # type: ignore[untyped-decorator]
    async def nack(
        self,
        queue: str,
        ids: Sequence[str],
        *,
        requeue: bool = True,
    ) -> int:
        """Negative-acknowledge messages.

        When ``requeue`` is ``True``, visibility timeout is reset to ``0`` so
        messages can be consumed again. When ``False``, messages are deleted.
        """
        if not ids:
            return 0

        if not requeue:
            return await self.ack(queue, ids)

        queue_url = await self.__resolve_queue_url(queue)
        nacked = 0

        c = self.__require_client()

        for chunk in self.__chunked_ids(ids):
            entries = [
                {
                    "Id": f"m{i}",
                    "ReceiptHandle": receipt,
                    "VisibilityTimeout": 0,
                }
                for i, receipt in enumerate(chunk)
            ]
            resp = await c.change_message_visibility_batch(
                QueueUrl=queue_url,
                Entries=entries,  # type: ignore[arg-type]
            )
            failed = resp.get("Failed") or []

            if failed:
                failed_ids = ", ".join(f.get("Id", "unknown") for f in failed)
                raise InfrastructureError(
                    "SQS change_message_visibility_batch has failed entries: "
                    f"{failed_ids}"
                )

            nacked += len(resp.get("Successful") or [])

        return nacked
