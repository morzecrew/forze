"""Offset-log consumer runner: the commit-after-inbox loop for Kafka-class streams."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Iterable
from datetime import timedelta
from functools import partial
from typing import Any, final

import attrs

from forze.application.contracts.crypto import BytesCipherPort, KeyringDepKey
from forze.application.contracts.envelope import HEADER_EVENT_ID
from forze.application.contracts.inbox import InboxSpec
from forze.application.contracts.stream import (
    CommitStreamGroupQueryPort,
    StreamCommandDepKey,
    StreamCommandPort,
    StreamMessage,
    StreamPosition,
    StreamSpec,
    UndecodableStreamPayload,
)
from forze.application.execution.context import ExecutionContext
from forze.application.integrations.crypto import PAYLOAD_CIPHER_MISSING_CODE
from forze.application.integrations.outbox import decrypt_consumed_payload
from forze.base.exceptions import CoreException, exc, exception_egress_policy
from forze.base.primitives import StrKey

from .._logger import logger
from ..inbox import process_with_inbox

# ----------------------- #

_CIPHER_MISSING_CODE = PAYLOAD_CIPHER_MISSING_CODE
"""Decrypt config error (no keyring): a deployment fault, not a poison message."""

_POLL_INTERVAL = timedelta(milliseconds=50)
"""Sleep between empty reads when running forever (``timeout=None``)."""


def _default_dedup_id(message: StreamMessage[Any]) -> str:
    """Dedup id for a log message: the outbox event id, else the partition-offset.

    Same chain as the generic ``process_with_inbox`` fallback (header → id),
    with typed access. Never ``key``: ``key`` is the partition/ordering key, so
    many distinct events share it. The message ``id`` is the canonical
    ``stream:partition:offset`` — globally unique, stable, and monotonic — so it
    is the correct raw-produce dedup key.
    """

    return message.headers.get(HEADER_EVENT_ID) or message.id


# ....................... #


def _to_topic_tuple(topics: Iterable[str]) -> tuple[str, ...]:
    """Freeze the subscribed topics to a tuple (typed so callers may pass any iterable)."""

    return tuple(topics)


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class CommitStreamGroupConsumerRunResult:
    """Summary of a finished :meth:`CommitStreamGroupConsumer.run` pass.

    Returned when the loop drains (finite idle ``timeout``) or stops on a
    pause-and-alert poison message. With ``timeout=None`` the consumer runs
    forever and only returns on such a pause.
    """

    processed: int = 0
    """Messages processed by the handler (inbox-marked and committed)."""

    duplicates: int = 0
    """Redelivered already-processed messages, committed without re-running the handler."""

    dead_lettered: int = 0
    """Poison messages produced to the dead-letter stream and committed past."""

    failed: int = 0
    """Handler failures that exhausted ``max_attempts`` with no dead-letter route
    (the run paused; the partition stays uncommitted for redelivery)."""


# ....................... #


async def _decrypt_message[M](
    message: StreamMessage[M],
    *,
    stream_spec: StreamSpec[M],
    cipher: BytesCipherPort | None,
) -> StreamMessage[M]:
    """Decrypt an end-to-end-encrypted message in place; pass plaintext through.

    The offset-log counterpart of the queue consumer's decrypt: a whole-envelope
    ciphertext payload is decrypted (AAD rebuilt from the forwarded headers) and
    decoded via the stream codec so the handler always sees the typed model.
    """

    model = await decrypt_consumed_payload(
        cipher,
        message.payload,
        codec=stream_spec.codec,
        headers=message.headers,
    )

    if model is message.payload:  # plaintext — nothing decrypted
        return message

    return attrs.evolve(message, payload=model)


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class CommitStreamGroupConsumer[M]:
    """Consumer-side loop of the offset-log (commit sub-model) transactional path.

    The dual of :class:`~forze_kits.integrations.consumer.QueueConsumer` for a
    partitioned, offset-committed log: it reads a batch, processes each message
    through the inbox in a transaction, and **commits the offset only after the
    handler commits** — at-least-once transport + inbox dedup = exactly-once
    *effect*, identical to every other backend. Auto-commit is never used.

    The decision ladder, adapted to a log's lack of per-message nack:

    1. **Decode** — the adapter's read path never raises a malformed record out of
       the batch (a raise would let a later commit skip the whole fetched batch on
       an offset-log). Instead it substitutes an
       :class:`~forze.application.contracts.stream.UndecodableStreamPayload` marker,
       which the loop treats as decode poison: **pause and alert**, rewind to
       committed, and leave the offset uncommitted for redelivery.
    2. **Decrypt** — an end-to-end ciphertext envelope is decrypted to the typed
       model. Decrypt failures are classified by exception kind: a **retryable**
       kind (the keyring's KMS dependency unavailable or throttled while
       unwrapping a cold data key) commits the successes so far and **raises**
       so the supervised lifecycle restarts the run with backoff (the offset
       stays uncommitted for redelivery); a missing-keyring config error aborts
       the loop (deployment fault); any other non-retryable failure (tampering,
       unknown key, malformed envelope) **pauses and alerts** (an undecryptable
       payload has no typed model to re-produce, so it cannot be dead-lettered
       through the typed producer — an operator must inspect it).
    3. **Process** — :func:`process_with_inbox` runs the dedup mark + handler in
       one transaction on ``tx_route``, rebinding correlation/causation/trace
       from the envelope headers. Both processed and duplicate outcomes commit.
    4. **Retry** — a failing handler is retried up to ``max_attempts`` (each
       attempt optionally wrapped in the named ``retry_policy``).
    5. **Handler poison (log-shaped)** — a log cannot requeue one message without
       wedging the partition. After ``max_attempts``, if ``dlq_stream`` is set the
       (decoded) message is produced there and the offset is **committed past**
       (freeing the partition); otherwise the run **pauses and alerts** — the
       offset stays uncommitted for redelivery, never silently skipped.
    6. **Idle** — a finite ``timeout`` drains what is available and returns;
       ``None`` runs forever, polling.

    **Pause is consumer-wide, not per-topic.** A pause-and-alert (poison with no
    dead-letter route, or any decrypt/decode poison) stops the *whole* consumer —
    every subscribed topic — not just the wedged partition. The run returns and
    does not resume until an operator intervenes (clear the poison / add a DLQ
    route / reset the offset). Run one consumer per isolation boundary if a poison
    message in one topic must not stall the others, and alert on a run that
    returns with ``failed > 0``.
    """

    topics: tuple[str, ...] = attrs.field(converter=_to_topic_tuple)
    """Topics (streams) this consumer subscribes to."""

    group: str
    """Consumer group whose committed offsets gate delivery."""

    consumer: str
    """Member identity within the group (static-membership / client id)."""

    stream_spec: StreamSpec[M]
    """Spec resolving the offset-log consumer port + payload codec."""

    handler: Callable[[StreamMessage[M]], Awaitable[None]]
    """Per-message handler, run inside the dedup transaction."""

    inbox_spec: InboxSpec
    """Consumer-side dedup store spec."""

    tx_route: StrKey
    """Transaction route the dedup mark + handler commit on."""

    message_id: Callable[[StreamMessage[M]], str] | None = None
    """Dedup-id extractor override (default: ``forze_event_id`` header, else the
    canonical ``message.id`` partition-offset — **never** ``key``, which is a
    shared ordering key on a log; see :func:`_default_dedup_id`)."""

    bind_tenant_from_headers: bool = False
    """Forwarded to ``process_with_inbox``; **opt-in** because headers are untrusted."""

    max_attempts: int = 1
    """Attempts per message before poison handling (``>= 1``). Each attempt runs
    the process step (optionally under ``retry_policy``)."""

    retry_policy: StrKey | None = None
    """Optional **named** resilience policy wrapping each process attempt."""

    dlq_stream: str | None = None
    """Dead-letter stream name for **handler** poison. When set, a decoded message
    that exhausts ``max_attempts`` is produced here (via ``stream_spec``'s command
    port) and the offset is committed past it; when ``None``, it **pauses and
    alerts** instead. Decrypt/decode poison always pauses (nothing to re-produce)."""

    batch_limit: int | None = None
    """Max messages per ``read`` (``None`` = the whole uncommitted tail)."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.max_attempts < 1:
            raise exc.configuration("max_attempts must be >= 1")

        if not self.topics:
            raise exc.configuration("at least one topic is required")

    # ....................... #

    async def run(
        self,
        ctx: ExecutionContext,
        *,
        timeout: timedelta | None = None,
    ) -> CommitStreamGroupConsumerRunResult:
        """Consume the log and process each message exactly-once via the inbox.

        Returns early — even with ``timeout=None`` — if a message pauses the
        consumer (poison with no dead-letter route, or decrypt/decode poison):
        the pause is **consumer-wide**, halting every subscribed topic, so treat a
        result with ``failed > 0`` as an alert requiring operator intervention.

        :param timeout: ``None`` runs forever (polling on an empty log); a finite
            value drains what is currently available and returns a result.
        """

        executor = ctx.resilience() if self.retry_policy is not None else None

        port: CommitStreamGroupQueryPort[M] = ctx.stream.commit_query(self.stream_spec)

        cipher = (
            ctx.deps.provide(KeyringDepKey) if ctx.deps.exists(KeyringDepKey) else None
        )

        dlq_producer: StreamCommandPort[M] | None = None

        if self.dlq_stream is not None:
            dlq_producer = ctx.deps.resolve_configurable(
                ctx,
                StreamCommandDepKey,
                self.stream_spec,
                route=self.stream_spec.name,
            )

        totals = _RunTotals()
        topics = list(self.topics)

        while True:
            batch = await port.read(
                self.group,
                self.consumer,
                topics,
                limit=self.batch_limit,
                timeout=timeout,
            )

            if not batch:
                if timeout is None:
                    await asyncio.sleep(_POLL_INTERVAL.total_seconds())
                    continue

                return totals.finish()

            committed = await self._process_batch(
                ctx,
                batch,
                port=port,
                executor=executor,
                cipher=cipher,
                dlq_producer=dlq_producer,
                totals=totals,
            )

            if not committed:
                # A pause-and-alert message stopped the batch; successes so far
                # are already committed inside _process_batch. Stop the run.
                return totals.finish()

    # ....................... #

    async def reset_to_committed(self, ctx: ExecutionContext) -> None:
        """Rewind the in-memory read position to the group's committed offset.

        For a **loss-free restart**: a crash mid-batch may have left the backend's
        pooled reader positioned past uncommitted records (``read`` advances an
        in-memory cursor); rewinding to committed makes the restart re-fetch them
        instead of skipping. A no-op on backends whose read position *is* the
        committed cursor. Called by the supervised lifecycle step before it
        restarts a crashed run.
        """

        port: CommitStreamGroupQueryPort[M] = ctx.stream.commit_query(self.stream_spec)
        await port.seek_to_committed(self.group, list(self.topics))

    # ....................... #

    async def _process_batch(
        self,
        ctx: ExecutionContext,
        batch: list[StreamMessage[M]],
        *,
        port: CommitStreamGroupQueryPort[M],
        executor: Any,
        cipher: BytesCipherPort | None,
        dlq_producer: StreamCommandPort[M] | None,
        totals: _RunTotals,
    ) -> bool:
        """Process a batch; return ``False`` if a pause-and-alert message stopped it."""

        positions: list[StreamPosition] = []

        async def _flush() -> None:
            if positions:
                await port.commit(self.group, positions)

        async def _pause() -> bool:
            # Commit the successes so far, then rewind the in-memory read position to
            # committed so the uncommitted tail (this poison + everything after it) is
            # re-fetched on the next read / supervised restart — never skipped past.
            await _flush()
            await port.seek_to_committed(self.group, list(self.topics))
            totals.failed += 1
            return False

        for message in batch:
            # -- Decode: a poison marker from the read path pauses like decode. --- #
            if isinstance(message.payload, UndecodableStreamPayload):
                self._alert_pause(message, reason="decode")
                return await _pause()

            # -- Decrypt: end-to-end ciphertext → typed model before the ladder. -- #
            try:
                message = await _decrypt_message(
                    message, stream_spec=self.stream_spec, cipher=cipher
                )

            except asyncio.CancelledError:
                raise

            except CoreException as e:
                if e.code == _CIPHER_MISSING_CODE:
                    raise  # deployment fault — abort, don't dead-letter every message

                # Decrypt runs through the KMS-backed keyring, so a transient
                # dependency fault (KMS unavailable/throttled on a cold data key)
                # surfaces here too and is indistinguishable from tampering by
                # failure alone — classify by kind. A retryable kind is
                # crash-shaped, not poison: commit the successes so far and
                # re-raise so the supervised lifecycle restarts the run with
                # backoff (rewinding to committed), instead of pausing for an
                # operator; the message is redelivered once the blip clears.
                if exception_egress_policy(e.kind).retryable:
                    logger.warning(
                        "Commit-stream consumer could not decrypt %s on %s "
                        "(transient %s failure); raising for supervised restart",
                        message.id,
                        message.stream,
                        e.kind.value,
                        exc_info=True,
                    )
                    await _flush()
                    raise

                # Decrypt-poison: no typed model in hand, so it cannot be
                # re-produced through the typed DLQ port — pause and alert.
                self._alert_pause(message, reason="decrypt")
                return await _pause()

            except Exception:
                # Decode-poison: same as decrypt — nothing decodable to forward.
                self._alert_pause(message, reason="decode")
                return await _pause()

            # -- Process: dedup mark + handler in one transaction, up to N tries. -- #
            outcome = await self._process_one(ctx, message, executor=executor)

            if outcome is None:
                # Exhausted max_attempts — poison.
                if await self._dead_letter(message, dlq_producer, reason="handler"):
                    positions.append(StreamPosition.from_message(message))
                    totals.dead_lettered += 1
                    continue

                self._alert_pause(message, reason="handler")
                return await _pause()

            if outcome:
                totals.processed += 1
            else:
                totals.duplicates += 1

            positions.append(StreamPosition.from_message(message))

        await _flush()
        return True

    # ....................... #

    async def _process_one(
        self,
        ctx: ExecutionContext,
        message: StreamMessage[M],
        *,
        executor: Any,
    ) -> bool | None:
        """Process one message; return the processed/duplicate flag, or ``None`` if it exhausted retries."""

        process = partial(
            process_with_inbox,
            ctx,
            message,
            inbox_spec=self.inbox_spec,
            handler=self.handler,
            tx_route=self.tx_route,
            message_id=self.message_id if self.message_id is not None else _default_dedup_id,
            bind_tenant_from_headers=self.bind_tenant_from_headers,
        )

        for attempt in range(1, self.max_attempts + 1):
            try:
                if executor is not None and self.retry_policy is not None:
                    return await executor.run(
                        process, policy=self.retry_policy, route=message.stream
                    )

                return await process()

            except asyncio.CancelledError:
                raise

            except Exception:
                logger.warning(
                    "Commit-stream consumer handler failed for %s on %s (attempt %s/%s)",
                    message.id,
                    message.stream,
                    attempt,
                    self.max_attempts,
                    exc_info=True,
                )

        return None

    # ....................... #

    def _alert_pause(self, message: StreamMessage[M], *, reason: str) -> None:
        """Log the pause-and-alert: the offset is left uncommitted for redelivery."""

        logger.error(
            "Commit-stream consumer pausing group %s on %s at %s (%s poison); offset "
            "left uncommitted for redelivery — operator intervention required",
            self.group,
            message.stream,
            message.id,
            reason,
        )

    # ....................... #

    async def _dead_letter(
        self,
        message: StreamMessage[M],
        dlq_producer: StreamCommandPort[M] | None,
        *,
        reason: str,
    ) -> bool:
        """Produce *message* to the dead-letter stream; return whether it was dead-lettered."""

        if dlq_producer is None or self.dlq_stream is None:
            return False

        logger.warning(
            "Commit-stream consumer dead-lettering %s on %s to %s (%s poison)",
            message.id,
            message.stream,
            self.dlq_stream,
            reason,
        )
        await dlq_producer.append(
            self.dlq_stream,
            message.payload,
            type=message.type,
            key=message.key,
            headers=message.headers,
        )
        return True


# ....................... #


@final
@attrs.define(slots=True, kw_only=True)
class _RunTotals:
    """Mutable tally folded into the immutable run result at the end."""

    processed: int = 0
    duplicates: int = 0
    dead_lettered: int = 0
    failed: int = 0

    def finish(self) -> CommitStreamGroupConsumerRunResult:
        return CommitStreamGroupConsumerRunResult(
            processed=self.processed,
            duplicates=self.duplicates,
            dead_lettered=self.dead_lettered,
            failed=self.failed,
        )
