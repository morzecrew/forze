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
"""

from __future__ import annotations

import random
from collections.abc import Awaitable, Callable
from datetime import timedelta
from typing import TYPE_CHECKING, Any
from uuid import UUID

from forze.application.contracts.outbox import OutboxRelayResult, OutboxSpec
from forze.application.contracts.resilience import BackoffStrategy
from forze.application.execution.resilience.backoff import compute_delay
from forze.base.exceptions import exc
from forze.base.primitives import utcnow

if TYPE_CHECKING:
    from forze.application.contracts.outbox import OutboxClaim
    from forze.application.execution.context import ExecutionContext

# ----------------------- #

PublishOne = Callable[["OutboxClaim", Any], Awaitable[None]]

_RNG = random.Random()  # nosec B311 - backoff jitter, not cryptographic material

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
    seconds = compute_delay(strategy, attempts, 0.0, rng or _RNG)

    return timedelta(seconds=seconds)


# ....................... #


async def relay_outbox_claims(
    ctx: ExecutionContext,
    *,
    outbox_spec: OutboxSpec[Any],
    publish_one: PublishOne,
    limit: int | None = None,
    reclaim_stale_after: timedelta | None = timedelta(minutes=5),
    max_attempts: int = 5,
    retry_base_delay: timedelta = timedelta(seconds=1),
    retry_max_backoff: timedelta = timedelta(minutes=5),
) -> OutboxRelayResult:
    """Claim pending rows, decode payloads, invoke *publish_one*, and mark each row.

    Per-row outcome: published on success; ``failed`` immediately on decode
    (poison) errors; rescheduled with backoff via ``mark_retry`` on publish
    (transient) errors until *max_attempts* is exhausted, then ``failed``.
    See the module docstring for the full failure model. Ordering is not
    preserved across failures/retries.
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

    published_ids: list[UUID] = []
    failed_ids: list[UUID] = []
    retried_ids: list[UUID] = []

    for claim in claims:
        try:
            # Build step: decode errors are poison — the row can never publish.
            payload = outbox_spec.codec.decode_mapping(claim.payload)

        except Exception as e:
            await query.mark_failed([claim.id], error=str(e))
            failed_ids.append(claim.id)
            continue

        try:
            # Publish step: broker errors are transient — retry with backoff.
            await publish_one(claim, payload)
            await query.mark_published([claim.id])
            published_ids.append(claim.id)

        except Exception as e:
            attempts = claim.attempts + 1

            if attempts >= max_attempts:
                await query.mark_failed([claim.id], error=str(e))
                failed_ids.append(claim.id)
                continue

            delay = compute_retry_delay(
                attempts,
                retry_base_delay=retry_base_delay,
                retry_max_backoff=retry_max_backoff,
            )
            await query.mark_retry(
                [claim.id],
                attempts=attempts,
                available_at=utcnow() + delay,
                error=str(e),
            )
            retried_ids.append(claim.id)

    return OutboxRelayResult(
        claimed=len(claims),
        published=len(published_ids),
        failed=len(failed_ids),
        retried=len(retried_ids),
        reclaimed=reclaimed,
    )
