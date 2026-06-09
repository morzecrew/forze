"""Shared outbox claim, decode, and mark logic for transport relays."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import timedelta
from typing import TYPE_CHECKING, Any
from uuid import UUID

from forze.application.contracts.outbox import OutboxRelayResult, OutboxSpec
from forze.base.exceptions import exc
from forze.base.primitives import utcnow

if TYPE_CHECKING:
    from forze.application.contracts.outbox import OutboxClaim
    from forze.application.execution.context import ExecutionContext

# ----------------------- #

PublishOne = Callable[["OutboxClaim", Any], Awaitable[None]]

# ....................... #


async def relay_outbox_claims(
    ctx: ExecutionContext,
    *,
    outbox_spec: OutboxSpec[Any],
    publish_one: PublishOne,
    limit: int | None = None,
    reclaim_stale_after: timedelta | None = timedelta(minutes=5),
) -> OutboxRelayResult:
    """Claim pending rows, decode payloads, invoke *publish_one*, mark published or failed."""

    if reclaim_stale_after is not None and reclaim_stale_after.total_seconds() <= 0:
        raise exc.internal("Reclaim stale after must be positive")

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

    for claim in claims:
        try:
            payload = outbox_spec.codec.decode_mapping(claim.payload)

        except Exception as e:
            await query.mark_failed([claim.id], error=str(e))
            failed_ids.append(claim.id)
            continue

        try:
            await publish_one(claim, payload)
            await query.mark_published([claim.id])
            published_ids.append(claim.id)

        except Exception as e:
            await query.mark_failed([claim.id], error=str(e))
            failed_ids.append(claim.id)

    return OutboxRelayResult(
        claimed=len(claims),
        published=len(published_ids),
        failed=len(failed_ids),
        reclaimed=reclaimed,
    )
