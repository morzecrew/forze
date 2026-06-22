"""Shared outbox claim, decode, and mark logic for transport relays.

Failure model
=============

Errors during a relay pass are classified by **where** they arise:

- **Poison** — the payload cannot be decoded into the codec model (the error is
  raised while *building* the message, before any broker call). The row can
  never publish, so it is marked ``failed`` immediately with its ``attempts``
  counter untouched; an operator fixes the cause and calls ``requeue_failed``.
- **Transient** — the broker *publish* call raised. The row is rescheduled via
  ``mark_retry`` with exponential backoff + jitter
  (``retry_base_delay * 2**attempts``, capped at ``retry_max_backoff``) until
  ``max_attempts`` is exhausted, then marked ``failed`` (terminal).

One row's failure never aborts the rest of the claimed batch. Delivery is
at-least-once and ordering is **not** preserved across failures/retries —
consumers must key on ``event_id`` and tolerate reordering as well as
redelivery.

Batched marking
===============

Marks are batched to amortize round-trips (per-row marks dominate relay cost:
each standalone ``UPDATE`` is BEGIN/UPDATE/COMMIT plus a pool checkout):

- ``mark_published`` — published ids are flushed in chunks of
  :data:`_MARK_CHUNK` during the pass plus one final flush. A crash between a
  publish and its mark now redelivers at most one chunk (vs one row before);
  delivery is already at-least-once and consumers must dedup on ``event_id``
  (the documented contract), so the chunk merely bounds the duplicate window.
- ``mark_retry`` — transiently-failed rows are grouped by their new
  ``attempts`` value and flushed at the end of the pass, one call per group
  with one jittered ``available_at`` shared by the group. Rows in a group
  share a retry slot — acceptable: they already shared a claim batch, and a
  single pass spans at most a few attempts buckets. The grouped error string
  keeps the first row's error and appends ``"(+N more)"`` when others differ
  (errors are operator diagnostics, not semantics).
- ``mark_failed`` — stays per-row: it is the rare terminal path and per-row
  error fidelity matters more than round-trips there.

If a mark flush raises it **propagates immediately** and aborts the pass:
rows already published but not yet marked stay ``processing`` and are later
reclaimed and redelivered — at-least-once holds, nothing is lost or silently
swallowed.
"""

import random
from collections.abc import Awaitable, Callable
from contextlib import AbstractContextManager, nullcontext
from datetime import timedelta
from typing import TYPE_CHECKING, Any
from uuid import UUID

from forze.application.contracts.crypto import KeyringDepKey
from forze.application.contracts.outbox import OutboxRelayResult, OutboxSpec
from forze.application.contracts.resilience import BackoffStrategy
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution.resilience.backoff import compute_delay
from forze.application.integrations.outbox import (
    decrypt_outbox_payload,
    is_encrypted_payload,
)
from forze.base.exceptions import exc
from forze.base.primitives import current_entropy_source, utcnow

if TYPE_CHECKING:
    from forze.application.contracts.outbox import OutboxClaim
    from forze.application.execution.context import ExecutionContext

# ----------------------- #

PublishOne = Callable[["OutboxClaim", Any], Awaitable[None]]


def _under_claim_tenant(
    ctx: "ExecutionContext", tenant_id: UUID | None
) -> AbstractContextManager[None]:
    """Bind a claim's staging tenant while it is forwarded to its destination.

    The relay runs as a tenant-less background process, so a tenant-aware destination —
    e.g. a per-tenant realtime stream key ``tenant:{id}:stream:…`` (RFC 0007) — would
    otherwise be written under no tenant, landing on the global key (and silently missed
    by a per-tenant consumer). ``claim.tenant_id`` is the trusted tenant the row was staged
    under (already used for at-rest decryption and the ``forze_tenant_id`` header), so
    binding it routes the forward to the right tenant. A tenant-global destination ignores
    the binding, so this is a no-op there.
    """

    if tenant_id is None:
        return nullcontext()

    return ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=tenant_id))

# Published ids are marked in chunks of this size (one UPDATE per chunk
# instead of one per row). Bounds the redelivery window after a crash to one
# chunk — see "Batched marking" in the module docstring.
_MARK_CHUNK = 32

# ....................... #


def validate_retry_options(
    *,
    max_attempts: int,
    retry_base_delay: timedelta,
    retry_max_backoff: timedelta,
) -> None:
    """Validate relay retry options shared by relay entry points."""

    if max_attempts < 1:
        raise exc.precondition("max_attempts must be >= 1")

    if retry_base_delay.total_seconds() <= 0:
        raise exc.precondition("retry_base_delay must be positive")

    if retry_max_backoff < retry_base_delay:
        raise exc.precondition("retry_max_backoff must be >= retry_base_delay")


# ....................... #


def compute_retry_delay(
    attempts: int,
    *,
    retry_base_delay: timedelta,
    retry_max_backoff: timedelta,
    rng: random.Random | None = None,
) -> timedelta:
    """Return the backoff delay before retry number *attempts* (1-based).

    Exponential growth (``retry_base_delay * 2**(attempts - 1)``) capped at
    *retry_max_backoff*, with equal jitter (the result lies in
    ``[raw / 2, raw]``).
    """

    strategy = BackoffStrategy(
        base=retry_base_delay,
        max=retry_max_backoff,
        multiplier=2.0,
        jitter="equal",
    )
    seconds = compute_delay(
        strategy, attempts, 0.0, rng or current_entropy_source().as_random()
    )

    return timedelta(seconds=seconds)


# ....................... #


async def relay_outbox_claims(
    ctx: "ExecutionContext",
    *,
    outbox_spec: OutboxSpec[Any],
    publish_one: PublishOne,
    limit: int | None = None,
    reclaim_stale_after: timedelta | None = timedelta(minutes=5),
    max_attempts: int = 5,
    retry_base_delay: timedelta = timedelta(seconds=1),
    retry_max_backoff: timedelta = timedelta(minutes=5),
) -> OutboxRelayResult:
    """Claim pending rows, decode payloads, invoke *publish_one*, and mark outcomes.

    Per-row outcome: published on success; ``failed`` immediately on decode
    (poison) errors; rescheduled with backoff via ``mark_retry`` on publish
    (transient) errors until *max_attempts* is exhausted, then ``failed``.
    See the module docstring for the full failure model. Ordering is not
    preserved across failures/retries.

    Marks are batched (see "Batched marking" in the module docstring):
    ``mark_published`` flushes in chunks of :data:`_MARK_CHUNK`, ``mark_retry``
    flushes once per ``attempts`` group at the end of the pass with a shared
    jittered ``available_at``, and ``mark_failed`` stays per-row. *publish_one*
    failures remain isolated per-row, but a failing mark flush propagates
    immediately and aborts the pass: published-but-unmarked rows stay
    ``processing`` and are reclaimed and redelivered later, so at-least-once
    delivery holds and no failure is swallowed.
    """

    if reclaim_stale_after is not None and reclaim_stale_after.total_seconds() <= 0:
        raise exc.internal("Reclaim stale after must be positive")

    validate_retry_options(
        max_attempts=max_attempts,
        retry_base_delay=retry_base_delay,
        retry_max_backoff=retry_max_backoff,
    )

    query = ctx.outbox.query(outbox_spec)
    reclaimed = 0

    if reclaim_stale_after is not None:
        older_than = utcnow() - reclaim_stale_after
        reclaimed = await query.reclaim_stale_processing(older_than=older_than)

    claims = await query.claim_pending(limit=limit)

    if not claims:
        return OutboxRelayResult(reclaimed=reclaimed)

    # Encryption tier decides what the relay publishes: ``at_rest`` decrypts here (the
    # broker/consumer see plaintext); ``end_to_end`` forwards the ciphertext opaquely for
    # the consumer to decrypt; ``none`` is plaintext. A ``None`` keyring + plaintext rows
    # is the unencrypted path; an encrypted row needing a key with none wired fails loud.
    cipher = ctx.deps.provide(KeyringDepKey) if ctx.deps.exists(KeyringDepKey) else None
    end_to_end = outbox_spec.encryption == "end_to_end"

    published = 0
    failed = 0
    retried = 0

    # Published ids awaiting a chunked mark_published flush.
    publish_buffer: list[UUID] = []
    # Transient failures grouped by NEW attempts value: ids + error strings.
    retry_groups: dict[int, tuple[list[UUID], list[str]]] = {}

    for claim in claims:
        try:
            if end_to_end and is_encrypted_payload(claim.payload):
                # Forward the ciphertext envelope unchanged; the consumer decrypts it.
                payload = claim.payload

            else:
                # ``at_rest`` decrypts here; ``none`` (and a legacy plaintext row on an
                # ``end_to_end`` route) passes plaintext through. Decode then publish so
                # the codec gets a model, not a raw dict. Decode/decrypt errors are
                # poison — the row can't publish.
                decrypted = await decrypt_outbox_payload(
                    cipher,
                    claim.payload,
                    tenant_id=claim.tenant_id,
                    event_id=claim.event_id,
                )
                payload = outbox_spec.codec.decode_mapping(decrypted)

        except Exception as e:
            # Terminal path stays per-row: rare, and error fidelity matters.
            await query.mark_failed([claim.id], error=str(e))
            failed += 1
            continue

        try:
            # Publish step: broker errors are transient — retry with backoff. Forward
            # under the claim's staging tenant so a tenant-aware destination routes to the
            # right tenant (no-op for a tenant-global destination).
            with _under_claim_tenant(ctx, claim.tenant_id):
                await publish_one(claim, payload)

        except Exception as e:
            attempts = claim.attempts + 1

            if attempts >= max_attempts:
                await query.mark_failed([claim.id], error=str(e))
                failed += 1
                continue

            ids, errors = retry_groups.setdefault(attempts, ([], []))
            ids.append(claim.id)
            errors.append(str(e))
            retried += 1
            continue

        publish_buffer.append(claim.id)

        if len(publish_buffer) >= _MARK_CHUNK:
            # A flush failure propagates: unmarked published rows stay
            # processing and are reclaimed/redelivered (at-least-once).
            await query.mark_published(publish_buffer)
            published += len(publish_buffer)
            publish_buffer = []

    if publish_buffer:
        await query.mark_published(publish_buffer)
        published += len(publish_buffer)

    for attempts, (ids, errors) in retry_groups.items():
        # One jittered slot per attempts group: rows in a group already
        # shared a claim batch, so sharing a retry slot is acceptable.
        delay = compute_retry_delay(
            attempts,
            retry_base_delay=retry_base_delay,
            retry_max_backoff=retry_max_backoff,
        )
        error = errors[0]
        differing = sum(1 for other in errors[1:] if other != error)

        if differing:
            error = f"{error} (+{differing} more)"

        await query.mark_retry(
            ids,
            attempts=attempts,
            available_at=utcnow() + delay,
            error=error,
        )

    return OutboxRelayResult(
        claimed=len(claims),
        published=published,
        failed=failed,
        retried=retried,
        reclaimed=reclaimed,
    )
