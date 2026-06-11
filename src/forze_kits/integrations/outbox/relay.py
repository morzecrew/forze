"""Relay staged outbox rows to queue, stream, or pubsub backends."""

from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING, Any

from forze.application.contracts.base import BaseSpec
from forze.application.contracts.deps import DepKey
from forze.application.contracts.envelope import (
    HEADER_CAUSATION_ID,
    HEADER_CORRELATION_ID,
    HEADER_EVENT_ID,
    HEADER_EXECUTION_ID,
    HEADER_OCCURRED_AT,
    HEADER_TENANT_ID,
)
from forze.application.contracts.outbox import (
    OutboxDestinationKind,
    OutboxRelayResult,
    OutboxSpec,
)
from forze.application.contracts.pubsub import PubSubCommandDepKey, PubSubSpec
from forze.application.contracts.queue import QueueCommandDepKey, QueueSpec
from forze.application.contracts.stream import StreamCommandDepKey, StreamSpec
from forze.base.exceptions import exc

from ._relay_core import relay_outbox_claims

if TYPE_CHECKING:
    from forze.application.contracts.outbox import OutboxClaim, OutboxDestination
    from forze.application.execution.context import ExecutionContext

# ----------------------- #


def _require_destination(
    destination: OutboxDestination | None,
    *,
    expected_kind: OutboxDestinationKind,
) -> OutboxDestination:
    if destination is None:
        raise exc.precondition(
            f"outbox_spec.destination is required for {expected_kind} relay"
        )

    if destination.kind != expected_kind:
        raise exc.precondition(
            f"outbox_spec.destination.kind must be {expected_kind!r}, got {destination.kind!r}"
        )

    return destination


# ....................... #


def _assert_route_matches(destination: OutboxDestination, spec_name: str) -> None:
    if str(destination.route) != str(spec_name):
        raise exc.precondition(
            f"spec.name must match OutboxSpec.destination.route for relay "
            f"(expected {destination.route!r}, got {spec_name!r})"
        )


# ....................... #


def _resolve_channel(
    outbox_spec: OutboxSpec[Any],
    *,
    spec_name: str,
    expected_kind: OutboxDestinationKind,
    allow_unset: bool = False,
) -> str:
    """Validate the outbox destination against *expected_kind* and return its channel.

    When *allow_unset* is set and no destination is configured, *spec_name* is used
    as the channel (queue fallback).
    """

    destination = outbox_spec.destination

    if destination is None and allow_unset:
        return spec_name

    dest = _require_destination(destination, expected_kind=expected_kind)
    _assert_route_matches(dest, spec_name)

    return dest.channel


# ....................... #


def _claim_envelope_headers(claim: OutboxClaim) -> dict[str, str]:
    """Build the well-known envelope headers carried by a relayed claim.

    Every destination kind forwards the staged invocation envelope as
    transport headers (see :mod:`forze.application.contracts.envelope`):
    ``event_id`` always, ``occurred_at`` as ISO-8601, and the
    correlation/causation/execution/tenant ids only when set on the row.
    """

    headers: dict[str, str] = {HEADER_EVENT_ID: str(claim.event_id)}

    if claim.occurred_at is not None:
        headers[HEADER_OCCURRED_AT] = claim.occurred_at.isoformat()

    if claim.correlation_id is not None:
        headers[HEADER_CORRELATION_ID] = str(claim.correlation_id)

    if claim.causation_id is not None:
        headers[HEADER_CAUSATION_ID] = str(claim.causation_id)

    if claim.execution_id is not None:
        headers[HEADER_EXECUTION_ID] = str(claim.execution_id)

    if claim.tenant_id is not None:
        headers[HEADER_TENANT_ID] = str(claim.tenant_id)

    return headers


# ....................... #


async def _relay_outbox_to(
    ctx: ExecutionContext,
    *,
    outbox_spec: OutboxSpec[Any],
    spec: BaseSpec,
    dep_key: DepKey[Any],
    expected_kind: OutboxDestinationKind,
    method: str,
    allow_unset_destination: bool = False,
    limit: int | None,
    reclaim_stale_after: timedelta | None,
    max_attempts: int,
    retry_base_delay: timedelta,
    retry_max_backoff: timedelta,
) -> OutboxRelayResult:
    """Claim pending outbox rows and relay each via ``command.<method>(channel, ...)``.

    The command-port method (``enqueue``/``append``/``publish``) shares the same
    ``(channel, payload, *, type, key, headers)`` signature across queue, stream,
    and pubsub, so only the resolved command, dep key, and method name differ per
    transport. Each publish forwards the claim's invocation envelope as
    transport headers (see :func:`_claim_envelope_headers`); ``type``/``key``
    stay exactly as before for backward compatibility.
    """

    if reclaim_stale_after is not None and reclaim_stale_after.total_seconds() <= 0:
        raise exc.internal("Reclaim stale after must be positive")

    channel = _resolve_channel(
        outbox_spec,
        spec_name=str(spec.name),
        expected_kind=expected_kind,
        allow_unset=allow_unset_destination,
    )

    command = ctx.deps.resolve_configurable(ctx, dep_key, spec, route=spec.name)

    async def _publish(claim: OutboxClaim, payload: Any) -> None:
        await getattr(command, method)(
            channel,
            payload,
            type=claim.event_type,
            key=str(claim.event_id),
            headers=_claim_envelope_headers(claim),
        )

    return await relay_outbox_claims(
        ctx,
        outbox_spec=outbox_spec,
        publish_one=_publish,
        limit=limit,
        reclaim_stale_after=reclaim_stale_after,
        max_attempts=max_attempts,
        retry_base_delay=retry_base_delay,
        retry_max_backoff=retry_max_backoff,
    )


# ....................... #


async def relay_outbox_to_queue(
    ctx: ExecutionContext,
    *,
    outbox_spec: OutboxSpec[Any],
    queue_spec: QueueSpec[Any],
    limit: int | None = None,
    reclaim_stale_after: timedelta | None = timedelta(minutes=5),
    max_attempts: int = 5,
    retry_base_delay: timedelta = timedelta(seconds=1),
    retry_max_backoff: timedelta = timedelta(minutes=5),
) -> OutboxRelayResult:
    """Claim pending outbox rows, enqueue payloads, and mark each row's outcome.

    Delivery is **at-least-once**, and ordering is **not preserved across
    failures/retries**: a row rescheduled (or parked as ``failed``) does not
    stall later rows. Rows are claimed (``pending`` → ``processing``), enqueued
    one message per claim, then marked ``published``. Enqueue and
    ``mark_published`` are not atomic—consumers must deduplicate on
    :attr:`~forze.application.contracts.outbox.IntegrationEvent.event_id` (or the
    claim ``event_id``) and tolerate reordering.

    Failure handling per row (one row's failure never aborts the batch):

    - Payload decode errors (poison) → ``mark_failed`` immediately; an operator
      re-drives with ``requeue_failed`` after fixing the cause.
    - Broker publish errors (transient) → rescheduled with exponential backoff
      plus jitter (``retry_base_delay * 2**attempts``, capped at
      *retry_max_backoff*) until *max_attempts* publish attempts are exhausted,
      then ``mark_failed`` (terminal). ``requeue_failed`` resets the counter.

    Each successful enqueue passes ``key=str(claim.event_id)`` to
    :meth:`~forze.application.contracts.queue.QueueCommandPort.enqueue` when the
    queue backend supports deduplication (for example SQS FIFO).

    When *reclaim_stale_after* is set, rows stuck in ``processing`` longer than that
    lease are reset to ``pending`` before claim (requires ``processing_at`` on the
    outbox store). Pass ``None`` to skip reclaim.

    The logical queue channel comes from
    :attr:`~forze.application.contracts.outbox.OutboxSpec.destination` when set;
    otherwise *queue_spec* ``name`` is used as the channel.
    """

    return await _relay_outbox_to(
        ctx,
        outbox_spec=outbox_spec,
        spec=queue_spec,
        dep_key=QueueCommandDepKey,
        expected_kind="queue",
        method="enqueue",
        allow_unset_destination=True,
        limit=limit,
        reclaim_stale_after=reclaim_stale_after,
        max_attempts=max_attempts,
        retry_base_delay=retry_base_delay,
        retry_max_backoff=retry_max_backoff,
    )


# ....................... #


async def relay_outbox_to_stream(
    ctx: ExecutionContext,
    *,
    outbox_spec: OutboxSpec[Any],
    stream_spec: StreamSpec[Any],
    limit: int | None = None,
    reclaim_stale_after: timedelta | None = timedelta(minutes=5),
    max_attempts: int = 5,
    retry_base_delay: timedelta = timedelta(seconds=1),
    retry_max_backoff: timedelta = timedelta(minutes=5),
) -> OutboxRelayResult:
    """Claim pending outbox rows, append to a stream, and mark each row's outcome.

    Same failure model and ordering caveats as :func:`relay_outbox_to_queue`:
    at-least-once delivery, no ordering across failures/retries, poison rows
    fail immediately, transient publish errors retry with backoff up to
    *max_attempts*.
    """

    return await _relay_outbox_to(
        ctx,
        outbox_spec=outbox_spec,
        spec=stream_spec,
        dep_key=StreamCommandDepKey,
        expected_kind="stream",
        method="append",
        limit=limit,
        reclaim_stale_after=reclaim_stale_after,
        max_attempts=max_attempts,
        retry_base_delay=retry_base_delay,
        retry_max_backoff=retry_max_backoff,
    )


# ....................... #


async def relay_outbox_to_pubsub(
    ctx: ExecutionContext,
    *,
    outbox_spec: OutboxSpec[Any],
    pubsub_spec: PubSubSpec[Any],
    limit: int | None = None,
    reclaim_stale_after: timedelta | None = timedelta(minutes=5),
    max_attempts: int = 5,
    retry_base_delay: timedelta = timedelta(seconds=1),
    retry_max_backoff: timedelta = timedelta(minutes=5),
) -> OutboxRelayResult:
    """Claim pending outbox rows, publish to a topic, and mark each row's outcome.

    Same failure model and ordering caveats as :func:`relay_outbox_to_queue`:
    at-least-once delivery, no ordering across failures/retries, poison rows
    fail immediately, transient publish errors retry with backoff up to
    *max_attempts*.
    """

    return await _relay_outbox_to(
        ctx,
        outbox_spec=outbox_spec,
        spec=pubsub_spec,
        dep_key=PubSubCommandDepKey,
        expected_kind="pubsub",
        method="publish",
        limit=limit,
        reclaim_stale_after=reclaim_stale_after,
        max_attempts=max_attempts,
        retry_base_delay=retry_base_delay,
        retry_max_backoff=retry_max_backoff,
    )


# ....................... #


async def relay_outbox(
    ctx: ExecutionContext,
    *,
    outbox_spec: OutboxSpec[Any],
    queue_spec: QueueSpec[Any] | None = None,
    stream_spec: StreamSpec[Any] | None = None,
    pubsub_spec: PubSubSpec[Any] | None = None,
    limit: int | None = None,
    reclaim_stale_after: timedelta | None = timedelta(minutes=5),
    max_attempts: int = 5,
    retry_base_delay: timedelta = timedelta(seconds=1),
    retry_max_backoff: timedelta = timedelta(minutes=5),
) -> OutboxRelayResult:
    """Relay using :attr:`~forze.application.contracts.outbox.OutboxSpec.destination`."""

    destination = outbox_spec.destination

    if destination is None:
        raise exc.precondition("outbox_spec.destination is required for relay_outbox")

    match destination.kind:
        case "queue":
            if queue_spec is None:
                raise exc.precondition(
                    "queue_spec is required when destination.kind is queue"
                )
            return await relay_outbox_to_queue(
                ctx,
                outbox_spec=outbox_spec,
                queue_spec=queue_spec,
                limit=limit,
                reclaim_stale_after=reclaim_stale_after,
                max_attempts=max_attempts,
                retry_base_delay=retry_base_delay,
                retry_max_backoff=retry_max_backoff,
            )

        case "stream":
            if stream_spec is None:
                raise exc.precondition(
                    "stream_spec is required when destination.kind is stream"
                )
            return await relay_outbox_to_stream(
                ctx,
                outbox_spec=outbox_spec,
                stream_spec=stream_spec,
                limit=limit,
                reclaim_stale_after=reclaim_stale_after,
                max_attempts=max_attempts,
                retry_base_delay=retry_base_delay,
                retry_max_backoff=retry_max_backoff,
            )

        case "pubsub":
            if pubsub_spec is None:
                raise exc.precondition(
                    "pubsub_spec is required when destination.kind is pubsub"
                )
            return await relay_outbox_to_pubsub(
                ctx,
                outbox_spec=outbox_spec,
                pubsub_spec=pubsub_spec,
                limit=limit,
                reclaim_stale_after=reclaim_stale_after,
                max_attempts=max_attempts,
                retry_base_delay=retry_base_delay,
                retry_max_backoff=retry_max_backoff,
            )

        case _:  # pyright: ignore[reportUnnecessaryComparison]
            raise exc.precondition(
                f"unsupported outbox destination kind: {destination.kind!r}"
            )
