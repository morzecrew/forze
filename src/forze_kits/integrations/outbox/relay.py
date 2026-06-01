"""Relay staged outbox rows to a configured queue."""

from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING, Any
from uuid import UUID

from forze.application.contracts.outbox import OutboxRelayResult, OutboxSpec
from forze.application.contracts.queue import QueueCommandDepKey, QueueSpec
from forze.base.exceptions import exc
from forze.base.primitives import utcnow


if TYPE_CHECKING:
    from forze.application.execution.context import ExecutionContext

# ----------------------- #


async def relay_outbox_to_queue(
    ctx: ExecutionContext,
    *,
    outbox_spec: OutboxSpec[Any],
    queue_spec: QueueSpec[Any],
    limit: int | None = None,
    reclaim_stale_after: timedelta | None = timedelta(minutes=5),
) -> OutboxRelayResult:
    """Claim pending outbox rows, enqueue payloads, and mark published or failed.

    Delivery is **at-least-once**: rows are claimed (``pending`` → ``processing``),
    enqueued one message per claim, then marked ``published``. Enqueue and
    ``mark_published`` are not atomic—consumers should deduplicate on
    :attr:`~forze.application.contracts.outbox.IntegrationEvent.event_id` (or the
    claim ``event_id``) when the queue supports idempotent handling.

    Each successful enqueue passes ``key=str(claim.event_id)`` to
    :meth:`~forze.application.contracts.queue.QueueCommandPort.enqueue` when the
    queue backend supports deduplication (for example SQS FIFO).

    When *reclaim_stale_after* is set, rows stuck in ``processing`` longer than that
    lease are reset to ``pending`` before claim (requires ``processing_at`` on the
    outbox store). Pass ``None`` to skip reclaim.

    Validation errors mark individual rows ``failed``. Enqueue failures mark the
    affected row ``failed``.

    The logical queue channel comes from
    :attr:`~forze.application.contracts.outbox.OutboxSpec.destination` when set;
    otherwise *queue_spec* ``name`` is used as the channel.
    """

    destination = outbox_spec.destination
    queue_channel = (
        destination.queue if destination is not None else str(queue_spec.name)
    )

    if destination is not None and str(destination.queue_route) != str(queue_spec.name):
        raise exc.precondition(
            "queue_spec.name must match OutboxSpec.destination.queue_route for relay"
        )

    query = ctx.outbox.query(outbox_spec)
    reclaimed = 0

    if reclaim_stale_after is not None:
        older_than = utcnow() - reclaim_stale_after
        reclaimed = await query.reclaim_stale_processing(older_than=older_than)

    claims = await query.claim_pending(limit=limit)

    if not claims:
        return OutboxRelayResult(reclaimed=reclaimed)

    command = ctx.deps.resolve_configurable(
        ctx,
        QueueCommandDepKey,
        queue_spec,
        route=queue_spec.name,
    )

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
            await command.enqueue(
                queue_channel,
                payload,
                key=str(claim.event_id),
            )
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
