from collections.abc import AsyncGenerator, Awaitable, Mapping, Sequence
from datetime import datetime, timedelta
from typing import (
    Protocol,
    runtime_checkable,
)

from .value_objects import QueueMessage

# ----------------------- #


@runtime_checkable
class QueueQueryPort[M](Protocol):
    """Contract for reading and acknowledging messages from a queue backend."""

    def receive(
        self,
        queue: str,
        *,
        limit: int | None = None,
        timeout: timedelta | None = None,
    ) -> Awaitable[list[QueueMessage[M]]]:
        """Fetch a batch of messages from *queue*.

        :param timeout: Upper bound on how long the call may wait for
            messages. The call always returns within a bounded window —
            with whatever messages arrived (possibly fewer than *limit*,
            possibly none). ``None`` means a backend-default bounded wait,
            never an unbounded block.
        """
        ...  # pragma: no cover

    # ....................... #

    def consume(
        self,
        queue: str,
        *,
        timeout: timedelta | None = None,
    ) -> AsyncGenerator[QueueMessage[M]]:
        """Yield messages continuously from *queue*.

        :param timeout: **Idle** timeout. ``None`` means consume forever,
            yielding messages as they arrive. A finite value means the
            generator stops cleanly (no error is raised) once no message
            has arrived for that duration; each received message resets
            the idle window.
        """
        ...  # pragma: no cover

    # ....................... #

    def ack(self, queue: str, ids: Sequence[str]) -> Awaitable[int]:
        """Acknowledge processed messages, returning the count acknowledged."""
        ...  # pragma: no cover

    # ....................... #

    def nack(
        self,
        queue: str,
        ids: Sequence[str],
        *,
        requeue: bool = True,
        count: bool = True,
    ) -> Awaitable[int]:
        """Negatively acknowledge messages, returning the count processed.

        :param requeue: ``True`` returns the messages to the queue for
            **immediate** redelivery (RabbitMQ broker-requeue; SQS visibility
            reset to ``0``). ``False`` does **not** return them immediately —
            the terminal disposition is broker-specific: RabbitMQ dead-letters
            via the queue's DLX (or drops without one); SQS leaves the message
            invisible until its visibility timeout lapses, so the queue's
            redrive policy counts the receive and eventually dead-letters it.
            Neither value is an immediate permanent delete.
        :param count: Whether this negative-ack counts as a delivery **attempt**
            for poison-parking. Default ``True``. Pass ``False`` for a requeue
            that is not the message's fault — e.g. the runtime is draining — so it
            is not driven toward ``max_deliveries``. Only backends that maintain
            their own delivery counter honor it (RabbitMQ under
            ``redelivery_counting``); backends whose count is the broker's own
            receive tally (SQS redrive) cannot suppress it and ignore the flag.

            **Additive:** an implementation predating this parameter stays usable.
            Callers inside the framework omit it entirely unless it is ``False``, and
            fall back to a plain nack if a port rejects the keyword — so such a port
            keeps working, it simply cannot suppress a delivery count.
        """
        ...  # pragma: no cover


# ....................... #


@runtime_checkable
class QueueCommandPort[M](Protocol):
    """Contract for publishing messages to a queue backend."""

    def enqueue(
        self,
        queue: str,
        payload: M,
        *,
        type: str | None = None,
        key: str | None = None,
        enqueued_at: datetime | None = None,
        delay: timedelta | None = None,
        not_before: datetime | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> Awaitable[str]:
        """Enqueue a single message and return its identifier.

        :param key: Opaque partition/correlation token. Portably it only
            means "these messages belong together": it always round-trips to
            :attr:`~forze.application.contracts.queue.QueueMessage.key` on
            received messages, while any broker-side semantics are
            per backend (this is the canonical table):

            - **RabbitMQ** — inert ``forze_key`` AMQP header; pure metadata,
              no routing or ordering effect (publishing routes by queue name
              on the default exchange).
            - **SQS, standard queue** — inert ``forze_key`` message attribute
              only.
            - **SQS, FIFO queue** (``.fifo``) — becomes ``MessageGroupId``,
              the per-key ordering lane. Deduplication never uses *key*:
              ``MessageDeduplicationId`` comes from an explicit message id or
              the ``forze_event_id`` header (distinct events may share a key,
              so key-based dedup would silently drop them).
            - **Mock** — stored verbatim on the message.

            The outbox relay sets ``key`` to the staged ``ordering_key`` when
            present, else ``str(event_id)``. The sibling stream/pubsub ports
            accept the same ``key`` but carry it as a field of the encoded
            message envelope (for streams, a partition hint on partitioned
            backends) rather than native broker metadata.
        :param enqueued_at: Logical enqueue timestamp stored on the message (metadata).
        :param delay: Relative delay before the message is visible to consumers.
        :param not_before: Absolute UTC instant before which the message is not visible.
        :param headers: String-to-string transport metadata, propagated
            best-effort via the backend's native metadata channel and surfaced
            on received messages as ``QueueMessage.headers``. Not part of the
            payload contract. Reserved transport keys (``forze_type``,
            ``forze_key``, ``forze_encoding``, ``forze_enqueued_at``) cannot
            be overridden by caller headers.
        """

        ...  # pragma: no cover

    # ....................... #

    def enqueue_many(
        self,
        queue: str,
        payloads: Sequence[M],
        *,
        type: str | None = None,
        key: str | None = None,
        enqueued_at: datetime | None = None,
        delay: timedelta | None = None,
        not_before: datetime | None = None,
        headers: Mapping[str, str] | None = None,
        message_headers: Sequence[Mapping[str, str]] | None = None,
    ) -> Awaitable[list[str]]:
        """Enqueue multiple messages and return their identifiers.

        The same *delay*, *not_before*, and *headers* apply to every message
        in the batch. *key* follows the per-backend semantics documented on
        :meth:`enqueue`.

        :param message_headers: Optional **per-message** headers, one mapping per
            payload (its length must equal *payloads*). Message ``i`` carries
            ``{**headers, **message_headers[i]}`` — the per-message entry overrides
            the shared *headers* — so a single batched publish can still give each
            message distinct metadata (e.g. a per-message ``forze_event_id`` for
            end-to-end encryption, which also keeps the SQS FIFO dedup id
            per-message). ``None`` keeps every message on the shared *headers*.
        """

        ...  # pragma: no cover
