"""Queue-consumer runner: the consumer-side loop of the transactional outbox."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from contextlib import aclosing
from datetime import timedelta
from functools import partial
from typing import Any, final

import attrs

from forze.application.contracts.crypto import BytesCipherPort, KeyringDepKey
from forze_kits.integrations._logger import logger
from forze.application.contracts.inbox import InboxSpec
from forze.application.contracts.queue import (
    QueueMessage,
    QueueQueryDepKey,
    QueueQueryPort,
    QueueSpec,
)
from forze.application.execution.context import ExecutionContext
from forze.application.integrations.crypto import PAYLOAD_CIPHER_MISSING_CODE
from forze.application.integrations.outbox import decrypt_consumed_payload
from forze.base.exceptions import CoreException, exc, exception_egress_policy
from forze.base.primitives import StrKey

from ..inbox import process_with_inbox

# ----------------------- #

# Single source of truth (was "core.outbox.payload_cipher_missing" before the payload
# primitive moved to the shared crypto plane — unreleased, so no external break).
_CIPHER_MISSING_CODE = PAYLOAD_CIPHER_MISSING_CODE
"""Decrypt config error (no keyring): a deployment fault, not a poison message."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ConsumerRunResult:
    """Summary of a finished :meth:`QueueConsumer.run` pass.

    Returned when the consume generator ends — i.e. only for a **finite**
    idle ``timeout`` (one-shot drains, tests). With ``timeout=None`` the
    consumer runs forever and never returns normally.

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
    """Handler failures and transient decrypt failures nacked back
    (``requeue=True``) for redelivery."""


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
    cipher: BytesCipherPort | None,
) -> QueueMessage[M]:
    """Decrypt an end-to-end-encrypted message in place; pass plaintext through.

    The consumer counterpart of the relay's at-rest decrypt: a message whose payload is
    a whole-envelope wrapper is decrypted (AAD rebuilt from the ``event_id``/``tenant``
    headers the relay forwarded) and decoded via the queue codec, so the handler always
    sees the typed model. Plaintext messages are returned unchanged.
    """

    model = await decrypt_consumed_payload(
        cipher,
        message.payload,
        codec=queue_spec.codec,
        headers=message.headers,
    )

    if model is message.payload:  # plaintext — nothing decrypted
        return message

    return attrs.evolve(message, payload=model)


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class QueueConsumer[M]:
    """Configurable consumer-side loop of the transactional outbox.

    Holds the consume *configuration* (queue, codec spec, handler, inbox/tx routes,
    poison/retry policy); :meth:`run` takes the per-run context + idle timeout and drives
    the loop. Static dependencies (the queue query port, the decrypt keyring, the
    resilience executor) are resolved once at the top of :meth:`run`, never inside the
    per-message loop.

    Each message goes through a fixed **decision ladder**:

    1. **Decode** — already handled *below* this loop: queue adapters decode payloads
       inside their ``consume`` generator and reject undecodable (poison) messages there
       (dead-letter/redrive per broker), so a decode-poison message never reaches the loop.
    2. **Decrypt** — an end-to-end ciphertext envelope is decrypted to the typed model
       (see :func:`_decrypt_message`). Decrypt failures are classified by exception
       kind: a **retryable** kind (the keyring's KMS dependency unavailable or
       throttled while unwrapping a cold data key) is nacked back (``requeue=True``)
       for redelivery, a non-retryable kind (tampering, unknown key, malformed
       envelope) is parked as poison, and a missing-keyring config error aborts the
       loop (a deployment fault, not per-message).
    3. **Park** — when ``max_deliveries`` is set and the message's ``delivery_count``
       exceeds it, the message is parked with ``nack(requeue=False)`` **without** running
       the handler (handler-poison). When the backend can't report a count, parking never
       triggers — rely on the broker's redrive/DLX policy instead.
    4. **Process** — :func:`~forze_kits.integrations.inbox.process_with_inbox` runs the
       dedup mark + handler in one transaction on ``tx_route``, rebinding correlation /
       causation from the envelope headers. ``True`` (processed) and ``False`` (duplicate)
       both ``ack`` — a redelivered already-processed message must leave the queue.
    5. **Transient failure** — a raising handler is nacked back (``requeue=True``) for
       redelivery; the loop continues. With ``retry_policy`` set, the process step first
       runs under that named policy (in-process retries before the broker redelivery).
    6. **Ack/nack failures** are logged and skipped (never fatal): broker redelivery plus
       inbox dedup make a lost acknowledgement safe.
    """

    queue: str
    """Logical queue (channel) to consume — what the relay published to."""

    queue_spec: QueueSpec[M]
    """Spec resolving the queue query port + payload codec."""

    handler: Callable[[QueueMessage[M]], Awaitable[None]]
    """Per-message handler, run inside the dedup transaction."""

    inbox_spec: InboxSpec
    """Consumer-side dedup store spec."""

    tx_route: StrKey
    """Transaction route the dedup mark + handler commit on."""

    message_id: Callable[[QueueMessage[M]], str] | None = None
    """Dedup-id extractor override (default: ``forze_event_id`` header, then
    ``message.id`` — never ``message.key``, which is a grouping key)."""

    bind_tenant_from_headers: bool = False
    """Forwarded to ``process_with_inbox``; **opt-in** because headers are untrusted."""

    max_deliveries: int | None = None
    """Opt-in poison-parking threshold (default ``None`` = the broker's redrive/DLX is the
    only safety net). A message whose ``delivery_count`` *exceeds* this is parked."""

    retry_policy: StrKey | None = None
    """Optional **named** resilience policy wrapping the process step before the
    nack-for-redelivery fallback."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.max_deliveries is not None and self.max_deliveries < 1:
            raise exc.configuration("max_deliveries must be >= 1 when set")

    # ....................... #

    async def run(
        self,
        ctx: ExecutionContext,
        *,
        timeout: timedelta | None = None,
    ) -> ConsumerRunResult:
        """Consume the queue and process each message exactly-once via the inbox.

        :param timeout: **Idle** timeout forwarded to ``consume``. ``None`` consumes
            forever; a finite value returns a :class:`ConsumerRunResult` once no message
            arrived for that long (one-shot drains, tests).
        """

        queue = self.queue

        # Resolve static dependencies once, up front — never inside the loop. Eager so a
        # missing policy executor fails fast, not mid-stream.
        executor = ctx.resilience() if self.retry_policy is not None else None

        port: QueueQueryPort[M] = ctx.deps.resolve_configurable(
            ctx,
            QueueQueryDepKey,
            self.queue_spec,
            route=self.queue_spec.name,
        )

        # End-to-end encrypted messages arrive as ciphertext envelopes; decrypt before the
        # ladder. ``None`` keyring is fine for plaintext queues — an encrypted message then
        # fails loud (deployment fault) and aborts the loop.
        cipher = (
            ctx.deps.provide(KeyringDepKey) if ctx.deps.exists(KeyringDepKey) else None
        )

        processed = 0
        duplicates = 0
        parked = 0
        failed = 0

        # Warn once if a poison ceiling was requested but this backend cannot report a
        # delivery count — the parking branch below can then never fire, so the broker's
        # dead-letter / redrive policy is the only ceiling.
        warned_no_delivery_count = False

        async with aclosing(port.consume(queue, timeout=timeout)) as messages:
            async for message in messages:
                # -- Decrypt: end-to-end ciphertext → typed model before the ladder. -- #
                try:
                    message = await _decrypt_message(
                        message, queue_spec=self.queue_spec, cipher=cipher
                    )

                except asyncio.CancelledError:
                    raise

                except CoreException as e:
                    if e.code == _CIPHER_MISSING_CODE:
                        raise  # deployment fault — abort, don't dead-letter every message

                    # Decrypt runs through the KMS-backed keyring, so a transient
                    # dependency fault (KMS unavailable/throttled on a cold data
                    # key) surfaces here too and is indistinguishable from
                    # tampering by failure alone — classify by kind: retryable
                    # kinds go back for redelivery, only the rest are poison.
                    if exception_egress_policy(e.kind).retryable:
                        logger.warning(
                            "Queue consumer could not decrypt message %s on queue "
                            "%s (transient %s failure); nack(requeue=True) for "
                            "redelivery",
                            message.id,
                            queue,
                            e.kind.value,
                            exc_info=True,
                        )
                        await _dispose(port, queue, message.id, requeue=True)
                        failed += 1
                        continue

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

                # -- Warn once: poison ceiling requested but unsupported by backend. - #
                if (
                    self.max_deliveries is not None
                    and message.delivery_count is None
                    and not warned_no_delivery_count
                ):
                    warned_no_delivery_count = True
                    logger.warning(
                        "Queue consumer for %s: max_deliveries=%s is set but the "
                        "backend does not report a delivery count, so in-app poison "
                        "parking cannot trigger; rely on the broker's dead-letter / "
                        "redrive policy instead.",
                        queue,
                        self.max_deliveries,
                    )

                # -- Park: handler-poison detected via the delivery count. ----- #
                if (
                    self.max_deliveries is not None
                    and message.delivery_count is not None
                    and message.delivery_count > self.max_deliveries
                ):
                    logger.warning(
                        "Queue consumer parking message %s on queue %s: "
                        "delivery %s exceeds max_deliveries=%s; nack(requeue=False)",
                        message.id,
                        queue,
                        message.delivery_count,
                        self.max_deliveries,
                    )
                    await _dispose(port, queue, message.id, requeue=False)
                    parked += 1
                    continue

                # -- Process: dedup mark + handler in one transaction. --------- #
                process = partial(
                    process_with_inbox,
                    ctx,
                    message,
                    inbox_spec=self.inbox_spec,
                    handler=self.handler,
                    tx_route=self.tx_route,
                    message_id=self.message_id,
                    bind_tenant_from_headers=self.bind_tenant_from_headers,
                )

                try:
                    if executor is not None and self.retry_policy is not None:
                        was_processed = await executor.run(
                            process,
                            policy=self.retry_policy,
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
