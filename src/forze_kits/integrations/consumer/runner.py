"""Queue-consumer runner: the consumer-side loop of the transactional outbox."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from contextlib import aclosing
from datetime import timedelta
from functools import partial
from typing import Any, final

import attrs

from forze.application._logger import logger
from forze.application.contracts.crypto import KeyringDepKey
from forze.application.contracts.inbox import InboxSpec
from forze.application.contracts.queue import (
    QueueMessage,
    QueueQueryDepKey,
    QueueQueryPort,
    QueueSpec,
)
from forze.application.execution.context import ExecutionContext
from forze.application.integrations.outbox import decrypt_consumed_payload
from forze.base.exceptions import CoreException, exc
from forze.base.primitives import StrKey

from ..inbox import process_with_inbox

# ----------------------- #

_CIPHER_MISSING_CODE = "core.outbox.payload_cipher_missing"
"""Decrypt config error (no keyring): a deployment fault, not a poison message."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ConsumerRunResult:
    """Summary of a finished :func:`run_consumer` pass.

    Returned when the consume generator ends — i.e. only for a **finite**
    idle *timeout* (one-shot drains, tests). With ``timeout=None`` the
    runner consumes forever and never returns normally.

    Ack/nack delivery failures are logged but not counted: the disposition
    decision was already taken, and broker redelivery covers a lost ack.
    """

    processed: int = 0
    """Messages processed by the handler (inbox-marked and acked)."""

    duplicates: int = 0
    """Redelivered already-processed messages, acked without re-running the handler."""

    parked: int = 0
    """Poison messages parked via ``nack(requeue=False)`` after exceeding *max_deliveries*."""

    failed: int = 0
    """Handler failures nacked back (``requeue=True``) for redelivery."""


# ....................... #


async def _dispose(
    port: QueueQueryPort[Any],
    queue: str,
    message_id: str,
    *,
    requeue: bool,
) -> None:
    """Best-effort ``nack``: a failed disposition is logged, never fatal.

    The message stays unacked on the broker, so its visibility timeout (or
    channel close) redelivers it — the loop must keep consuming.
    """

    try:
        await port.nack(queue, [message_id], requeue=requeue)

    except Exception:
        logger.exception(
            "Queue consumer failed to nack message %s on queue %s "
            "(requeue=%s); broker redelivery covers it, continuing",
            message_id,
            queue,
            requeue,
        )


# ....................... #


async def _acknowledge(
    port: QueueQueryPort[Any],
    queue: str,
    message_id: str,
) -> None:
    """Best-effort ``ack``: a failed ack is logged, never fatal.

    The broker will redeliver the unacked message; the inbox mark already
    committed, so the redelivery is recognized as a duplicate and acked again.
    """

    try:
        await port.ack(queue, [message_id])

    except Exception:
        logger.exception(
            "Queue consumer failed to ack message %s on queue %s; "
            "broker redelivery + inbox dedup cover it, continuing",
            message_id,
            queue,
        )


# ....................... #


async def _decrypt_message[M](
    message: QueueMessage[M],
    *,
    queue_spec: QueueSpec[M],
    cipher: object | None,
) -> QueueMessage[M]:
    """Decrypt an end-to-end-encrypted message in place; pass plaintext through.

    The consumer counterpart of the relay's at-rest decrypt: a message whose payload is
    a whole-envelope wrapper is decrypted (AAD rebuilt from the ``event_id``/``tenant``
    headers the relay forwarded) and decoded via the queue codec, so the handler always
    sees the typed model. Plaintext messages are returned unchanged.
    """

    model = await decrypt_consumed_payload(
        cipher,  # type: ignore[arg-type]
        message.payload,
        codec=queue_spec.codec,
        headers=message.headers,
    )

    if model is message.payload:  # plaintext — nothing decrypted
        return message

    return attrs.evolve(message, payload=model)


# ....................... #


async def run_consumer[M](
    ctx: ExecutionContext,
    *,
    queue: str,
    queue_spec: QueueSpec[M],
    handler: Callable[[QueueMessage[M]], Awaitable[None]],
    inbox_spec: InboxSpec,
    tx_route: StrKey,
    message_id: Callable[[QueueMessage[M]], str] | None = None,
    bind_tenant_from_headers: bool = False,
    max_deliveries: int | None = None,
    retry_policy: StrKey | None = None,
    timeout: timedelta | None = None,
) -> ConsumerRunResult:
    """Consume *queue* and process each message exactly-once via the inbox.

    The consumer-side counterpart of the outbox relay: resolves the queue
    query port for *queue_spec*, iterates its
    :meth:`~forze.application.contracts.queue.QueueQueryPort.consume`
    generator, and applies a fixed **decision ladder** per message:

    1. **Decode** — already handled *below* this loop: queue adapters decode
       payloads inside their ``consume`` generator, and the real adapters
       (RabbitMQ, SQS) reject undecodable (poison) messages there with
       ``nack(requeue=False)`` — dead-letter/redrive per broker — and keep
       yielding. A decode-poison message therefore never reaches this loop
       and is not counted in the result.
    2. **Park** — if *max_deliveries* is set and the message reports a
       :attr:`~forze.application.contracts.queue.QueueMessage.delivery_count`
       greater than it, the message is parked with ``nack(requeue=False)``
       **without running the handler** (handler-poison: it has already
       failed *max_deliveries* times). Terminal disposition is
       broker-specific: RabbitMQ DLX, SQS redrive policy, mock
       ``dead_letters``. **Caveat:** when the backend cannot report a
       delivery count (``delivery_count is None``) parking never triggers —
       rely on the broker's own redrive/DLX policy instead.
    3. **Process** — :func:`~forze_kits.integrations.inbox.process_with_inbox`
       runs the dedup mark and *handler* in one transaction on *tx_route*,
       rebinding correlation/causation from the envelope headers.
       ``True`` (processed) **and** ``False`` (duplicate) both ``ack``: a
       redelivered already-processed message must leave the queue, or it
       would redeliver forever.
    4. **Transient failure** — a raising handler is logged with its delivery
       context and the message is nacked back with ``requeue=True`` for
       immediate redelivery; the loop continues — one message's failure
       never kills the consumer. With *retry_policy* set, the whole process
       step (dedup mark + handler transaction) is first run under that named
       policy via ``ctx.resilience().run(...)``, so in-process retries are
       attempted before the message goes back to the broker. Each retry is
       a fresh transaction — the failed attempt's inbox mark rolled back
       with it.
    5. **Ack/nack failures** are logged and skipped (never fatal): broker
       redelivery plus inbox dedup make a lost acknowledgement safe.

    :class:`asyncio.CancelledError` propagates cleanly — cancellation is the
    shutdown path (see
    :func:`~forze_kits.integrations.consumer.queue_consumer_background_lifecycle_step`).
    A crash of the consume generator itself (e.g. broker connection loss)
    also propagates: one-shot callers see it, the background lifecycle step
    logs and restarts.

    :param queue: Logical queue (channel) to consume — what the relay
        published to.
    :param timeout: **Idle** timeout forwarded to ``consume``. ``None``
        consumes forever; a finite value returns a :class:`ConsumerRunResult`
        once no message arrived for that long (one-shot drains, tests).
    :param max_deliveries: Opt-in poison parking threshold (default
        ``None`` = parking disabled; the broker's own redrive/DLX remains the
        safety net). A message whose ``delivery_count`` (which **includes**
        the current delivery) *exceeds* this is parked — i.e. the handler
        gets at most *max_deliveries* attempts per message.
    :param retry_policy: Optional **named** resilience policy wrapping the
        process step before the nack-for-redelivery fallback.
    :param message_id: Dedup-id extractor override, forwarded to
        :func:`~forze_kits.integrations.inbox.process_with_inbox` (default
        priority: ``forze_event_id`` header, then ``message.key``, then
        ``message.id`` — the relay carries the integration ``event_id`` in
        the header and the staged ``ordering_key`` in ``key``).
    :param bind_tenant_from_headers: Forwarded to ``process_with_inbox``;
        **opt-in** because headers are untrusted input.
    """

    if max_deliveries is not None and max_deliveries < 1:
        raise exc.configuration("max_deliveries must be >= 1 when set")

    # Resolve eagerly so a missing policy executor fails fast, not mid-stream.
    executor = ctx.resilience() if retry_policy is not None else None

    port: QueueQueryPort[M] = ctx.deps.resolve_configurable(
        ctx,
        QueueQueryDepKey,
        queue_spec,
        route=queue_spec.name,
    )

    # End-to-end encrypted messages arrive as ciphertext envelopes; decrypt before the
    # decision ladder. ``None`` keyring is fine for plaintext queues — an encrypted
    # message then fails loud (deployment fault) and aborts the loop.
    cipher = ctx.deps.provide(KeyringDepKey) if ctx.deps.exists(KeyringDepKey) else None

    processed = 0
    duplicates = 0
    parked = 0
    failed = 0

    async with aclosing(port.consume(queue, timeout=timeout)) as messages:
        async for message in messages:
            # -- Decrypt: end-to-end ciphertext → typed model before the ladder. -- #
            try:
                message = await _decrypt_message(
                    message, queue_spec=queue_spec, cipher=cipher
                )

            except asyncio.CancelledError:
                raise

            except CoreException as e:
                if e.code == _CIPHER_MISSING_CODE:
                    raise  # deployment fault — abort, don't dead-letter every message

                logger.exception(
                    "Queue consumer could not decrypt message %s on queue %s; "
                    "nack(requeue=False) as poison",
                    message.id,
                    queue,
                )
                await _dispose(port, queue, message.id, requeue=False)
                parked += 1
                continue

            except Exception:
                logger.exception(
                    "Queue consumer could not decode encrypted message %s on queue %s; "
                    "nack(requeue=False) as poison",
                    message.id,
                    queue,
                )
                await _dispose(port, queue, message.id, requeue=False)
                parked += 1
                continue

            # -- Park: handler-poison detected via the delivery count. ----- #
            if (
                max_deliveries is not None
                and message.delivery_count is not None
                and message.delivery_count > max_deliveries
            ):
                logger.warning(
                    "Queue consumer parking message %s on queue %s: "
                    "delivery %s exceeds max_deliveries=%s; nack(requeue=False)",
                    message.id,
                    queue,
                    message.delivery_count,
                    max_deliveries,
                )
                await _dispose(port, queue, message.id, requeue=False)
                parked += 1
                continue

            # -- Process: dedup mark + handler in one transaction. --------- #
            process = partial(
                process_with_inbox,
                ctx,
                message,
                inbox_spec=inbox_spec,
                handler=handler,
                tx_route=tx_route,
                message_id=message_id,
                bind_tenant_from_headers=bind_tenant_from_headers,
            )

            try:
                if executor is not None and retry_policy is not None:
                    was_processed = await executor.run(
                        process,
                        policy=retry_policy,
                        route=queue,
                    )

                else:
                    was_processed = await process()

            except asyncio.CancelledError:
                raise

            except Exception:
                # -- Transient failure: requeue and keep consuming. -------- #
                logger.exception(
                    "Queue consumer handler failed for message %s on queue %s "
                    "(delivery %s); nack(requeue=True) for redelivery",
                    message.id,
                    queue,
                    message.delivery_count,
                )
                await _dispose(port, queue, message.id, requeue=True)
                failed += 1
                continue

            # -- Ack BOTH outcomes: a duplicate must leave the queue too. -- #
            if was_processed:
                processed += 1

            else:
                duplicates += 1

            await _acknowledge(port, queue, message.id)

    return ConsumerRunResult(
        processed=processed,
        duplicates=duplicates,
        parked=parked,
        failed=failed,
    )
