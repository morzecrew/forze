"""Queue-consumer runner: the consumer-side loop of the transactional outbox."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import aclosing, suppress
from datetime import timedelta
from functools import partial
from typing import Any, final

import attrs

from forze.application.contracts.crypto import BytesCipherPort, KeyringDepKey
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
from forze.base.exceptions import (
    CoreException,
    ExceptionKind,
    exc,
    exception_egress_policy,
)
from forze.base.primitives import StrKey
from forze_kits.integrations._logger import logger

from ..inbox import process_with_inbox

# ----------------------- #

_CIPHER_MISSING_CODE = PAYLOAD_CIPHER_MISSING_CODE
"""Decrypt config error (no keyring): a deployment fault, not a poison message."""

_CONFIG_FAULT_PAUSE_STREAK = 25
"""Consecutive configuration-kind decrypt failures after which the loop starts pausing.

A key-specific fault (one tenant's CMK disabled on a multi-key queue) interleaves with
messages that still decrypt, resetting the streak — the queue keeps flowing while the
affected messages cycle through requeue until their key returns. When *nothing* decrypts
(the only key disabled), the streak crosses this ceiling within one redelivery burst and
every further failure pauses :data:`_CONFIG_FAULT_PAUSE_SECONDS` before consuming on.

Pause, never abort: a ``CONFIGURATION``-kind raise is a **terminal** crash to the
supervisor (``is_terminal_crash``), so aborting here would permanently stop the consumer
on what may be one tenant's key — blocking every other key's messages until an operator
restarts the process. The pause bounds the redelivery spin to one failure per interval
while the loop stays live, and the first successful decrypt lifts it. A false positive
(25 same-key messages in a row on a multi-key queue) costs a few seconds' added latency,
nothing more."""

_CONFIG_FAULT_PAUSE_SECONDS = 5.0
"""Length of each throttle pause once the configuration-fault streak is crossed.

Also paces broker receives during an outage: an SQS receive tally still climbs between
uncounted copy-backs, and the pause keeps that to at most one receive per message per
backlog pass. See the requeue branch in :meth:`QueueConsumer.run`."""

_DRAINING_CODE = "draining"
"""Drain-gate refusal code (``THROTTLED``/``code="draining"``): the runtime is quiescing,
not a handler defect — the message must requeue **without** counting toward the poison
ceiling. Kept in sync with ``DrainGate.admit`` in the execution plane."""


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
    """Handler failures and transient or key-fault decrypt failures nacked back
    (``requeue=True``) for redelivery."""


# ....................... #


async def _dispose(
    port: QueueQueryPort[Any],
    queue: str,
    message_id: str,
    *,
    requeue: bool,
    count: bool = True,
) -> None:
    """Best-effort ``nack``: a failed disposition is logged, never fatal.

    The message stays unacked on the broker, so its visibility timeout (or
    channel close) redelivers it — the loop must keep consuming.

    ``count=False`` requeues without the disposition counting as a delivery attempt
    (poison-parking), for a requeue that is not the message's fault — see the draining
    case in :meth:`QueueConsumer.run`.

    ``count`` is only passed when it is actually ``False``, so a third-party port written
    against the older ``nack(queue, ids, *, requeue)`` signature keeps working on every
    ordinary disposition. If such a port rejects the keyword on the draining path, the
    disposition is retried without it rather than dropped — the message still goes back,
    it just counts as a delivery attempt on that backend.
    """

    try:
        if count:
            await port.nack(queue, [message_id], requeue=requeue)

        else:
            try:
                await port.nack(queue, [message_id], requeue=requeue, count=False)

            except TypeError:
                logger.warning(
                    "Queue port %s does not accept nack(count=…); requeuing message %s on "
                    "queue %s without suppressing the delivery count",
                    type(port).__name__,
                    message_id,
                    queue,
                )
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


# ....................... #


async def _pause_or_stop(stop: asyncio.Event | None, delay: float) -> bool:
    """Sleep *delay* seconds, ending early when *stop* fires; ``True`` = stop requested.

    The throttle pause of the configuration-fault branch: it must not outlive a shutdown
    request, so with a *stop* event the sleep is a bounded wait on it. Cancelling an
    ``Event.wait`` on timeout is side-effect free.
    """

    if stop is None:
        await asyncio.sleep(delay)
        return False

    with suppress(TimeoutError):
        await asyncio.wait_for(stop.wait(), delay)

    return stop.is_set()


# ....................... #


async def _next_or_stop(
    messages: AsyncIterator[QueueMessage[Any]],
    stop: asyncio.Event | None,
) -> QueueMessage[Any] | None:
    """The next message, or ``None`` when a stop is requested (or the stream ends).

    The wait for a message and the wait for a stop **race**, and they have to: a consumer
    parked on an idle queue is blocked inside the broker's generator, where it cannot see a
    stop flag — so a shutdown would sit out the whole grace and then cancel it anyway.

    When the stop wins, the pending fetch is abandoned. Any message the broker was about to
    hand over stays unacked, so it is redelivered and the inbox dedups it — exactly the
    contract a crash already has, and cheaper than cancelling a handler mid-transaction.
    """

    if stop is None:
        return await anext(messages, None)

    pull = asyncio.ensure_future(anext(messages, None))
    halt = asyncio.ensure_future(stop.wait())

    try:
        await asyncio.wait({pull, halt}, return_when=asyncio.FIRST_COMPLETED)

        return pull.result() if pull.done() else None

    finally:
        # Both futures are settled here on *every* path — including this task being cancelled
        # inside the ``wait`` above, which is a live path now that an overrunning loop really is
        # cancelled. A ``pull`` left in flight is not merely a leak: it is an unfinished
        # ``__anext__`` on the broker's generator, and an async generator cannot be aclosed
        # while one of its ``__anext__`` calls is still running — so the consumer's own cleanup
        # would raise on the way out.
        for pending in (pull, halt):
            if pending.done():
                continue

            pending.cancel()

            with suppress(asyncio.CancelledError):
                await pending


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
       kind: a missing keyring aborts the loop outright (nothing on the queue can
       decrypt); a **configuration** kind (KMS key disabled / pending deletion / not
       found — operator-actionable and reversible) is requeued **without** counting
       toward the poison ceiling and the loop keeps consuming, so on a multi-key
       queue one key's outage never blocks or destroys the rest — an unbroken
       streak of such failures (:data:`_CONFIG_FAULT_PAUSE_STREAK`; nothing
       decrypting at all) paces the loop with a stop-responsive pause per failure
       rather than spinning or stopping, and the first successful decrypt lifts it;
       a **retryable** kind (the keyring's KMS dependency unavailable or throttled
       while unwrapping a cold data key) is nacked back (``requeue=True``) for
       redelivery; the remaining non-retryable kinds (tampering, unknown key,
       malformed envelope) are per-message poison and are parked.
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
       runs under that named policy (in-process retries before the broker redelivery). A
       **draining** refusal (``THROTTLED``/``code="draining"``: the runtime began quiescing
       mid-message, before the loop's stop signal) is not a defect — it requeues with
       ``count=False`` so it is never driven toward ``max_deliveries``, and stops the loop.
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
        stop: asyncio.Event | None = None,
    ) -> ConsumerRunResult:
        """Consume the queue and process each message exactly-once via the inbox.

        :param timeout: **Idle** timeout forwarded to ``consume``. ``None`` consumes
            forever; a finite value returns a :class:`ConsumerRunResult` once no message
            arrived for that long (one-shot drains, tests).
        :param stop: Set to end the run at the next message boundary — *after* the message in
            hand has been acked, never in the middle of it. The background step passes its
            loop's stop signal here so shutdown does not have to cancel a handler mid-flight.
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
        cipher = ctx.deps.provide(KeyringDepKey) if ctx.deps.exists(KeyringDepKey) else None

        processed = 0
        duplicates = 0
        parked = 0
        failed = 0

        # Consecutive configuration-kind decrypt failures; reset by any message that
        # decrypts. See _CONFIG_FAULT_PAUSE_STREAK.
        config_fault_streak = 0

        # Warn once if a poison ceiling was requested but this backend cannot report a
        # delivery count — the parking branch below can then never fire, so the broker's
        # dead-letter / redrive policy is the only ceiling.
        warned_no_delivery_count = False

        async with aclosing(port.consume(queue, timeout=timeout)) as messages:
            while True:
                message = await _next_or_stop(messages, stop)

                if message is None:
                    break

                # -- Decrypt: end-to-end ciphertext → typed model before the ladder. -- #
                try:
                    message = await _decrypt_message(
                        message, queue_spec=self.queue_spec, cipher=cipher
                    )

                except asyncio.CancelledError:
                    raise

                except CoreException as e:
                    # No keyring at all: *nothing* on this queue can decrypt, so churning
                    # through messages would requeue every one of them for no progress.
                    # Abort outright — a deployment fault, not per-message.
                    if e.code == _CIPHER_MISSING_CODE:
                        raise

                    # A configuration-kind failure (KMS key disabled / pending deletion /
                    # not found) is operator-actionable and reversible — never per-message
                    # poison: nack(requeue=False) once per message would destroy the
                    # backlog at consume speed (deleted outright on SQS FIFO without a
                    # retention queue, dropped on DLX-less RabbitMQ). But it is not
                    # necessarily deployment-wide either: on a multi-key queue (per-tenant
                    # CMKs) only that key's messages are affected, and stopping the loop
                    # would block every other key's messages behind them — a
                    # ``CONFIGURATION`` raise is a *terminal* crash to the supervisor, so
                    # it would not even restart. So requeue and keep consuming, and once
                    # an unbroken streak says nothing is decrypting, pace the loop with a
                    # stop-responsive pause instead of spinning on broker redelivery.
                    #
                    # ``count=False`` is honored per backend as best it can: app-tracked
                    # counts skip the increment, and SQS requeues a fresh copy so the
                    # broker receive count resets — every time on a standard queue, on
                    # FIFO only once the count nears the redrive threshold (keeping the
                    # order-preserving reset while it is safe). The redrive DLQ can no
                    # longer swallow an outage; a FIFO queue without a redrive policy
                    # simply blocks the affected group until the key returns.
                    if e.kind is ExceptionKind.CONFIGURATION:
                        config_fault_streak += 1

                        if config_fault_streak == _CONFIG_FAULT_PAUSE_STREAK:
                            logger.error(
                                "Queue consumer on %s: %s consecutive configuration-kind "
                                "decrypt failures — nothing is decrypting (deployment-"
                                "wide fault, e.g. the KMS key is disabled); continuing "
                                "with a %.1fs pause per failure until decryption "
                                "recovers",
                                queue,
                                config_fault_streak,
                                _CONFIG_FAULT_PAUSE_SECONDS,
                            )

                        else:
                            logger.warning(
                                "Queue consumer could not decrypt message %s on queue %s "
                                "(configuration fault, e.g. a disabled KMS key); "
                                "nack(requeue=True, count=False) and continuing",
                                message.id,
                                queue,
                                exc_info=True,
                            )

                        await _dispose(port, queue, message.id, requeue=True, count=False)
                        failed += 1

                        if config_fault_streak >= _CONFIG_FAULT_PAUSE_STREAK and (
                            await _pause_or_stop(stop, _CONFIG_FAULT_PAUSE_SECONDS)
                        ):
                            break  # shutdown requested mid-pause

                        continue

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

                # This message decrypted (or was plaintext): whatever key it used is
                # live, so the configuration faults seen so far are key-specific,
                # not deployment-wide.
                config_fault_streak = 0

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

                except Exception as e:
                    # -- Draining: shutdown artifact, not a handler defect. ----- #
                    # The drain gate closed under us (the runtime is quiescing) and refused
                    # admission before the loop's stop signal arrived. This is not a delivery
                    # attempt, so requeue with count=False — a counted requeue would push the
                    # message toward max_deliveries on every replica of a rolling deploy and
                    # park it as poison with no defect. Then stop: the gate is one-way, so
                    # continuing would just re-refuse every remaining message.
                    if isinstance(e, CoreException) and e.code == _DRAINING_CODE:
                        logger.info(
                            "Queue consumer draining: message %s on queue %s refused by the "
                            "drain gate; nack(requeue=True, count=False) and stopping the loop",
                            message.id,
                            queue,
                        )
                        await _dispose(port, queue, message.id, requeue=True, count=False)
                        break

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
