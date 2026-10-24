"""Relay staged outbox rows to queue, stream, or pubsub backends."""

from datetime import timedelta
from typing import TYPE_CHECKING, Any, cast, final

import attrs

from forze.application.contracts.base import BaseSpec
from forze.application.contracts.deps import DepKey
from forze.application.contracts.envelope import (
    HEADER_CAUSATION_ID,
    HEADER_CORRELATION_ID,
    HEADER_EVENT_ID,
    HEADER_EXECUTION_ID,
    HEADER_HLC,
    HEADER_OCCURRED_AT,
    HEADER_TENANT_ID,
    HEADER_TRACEPARENT,
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
from forze_kits.integrations._logger import logger
from forze_kits.integrations._wiring import OUTBOX_RELAY_MISCONFIGURED_CODE as _MISCONFIGURED

from ._relay_core import relay_outbox_claims, validate_retry_options

if TYPE_CHECKING:
    from forze.application.contracts.outbox import OutboxClaim, OutboxDestination
    from forze.application.execution.context import ExecutionContext

# ----------------------- #

_PUBSUB_DOWNGRADE_WARNED: set[str] = set()
"""Outbox routes already warned about the pubsub delivery downgrade (once per process)."""


# ....................... #


def _warn_pubsub_downgrade_once(outbox_route: str, channel: str) -> None:
    """Warn once per outbox route that a pubsub destination downgrades delivery.

    The outbox promises at-least-once up to the broker, but pubsub is
    at-most-once past it: rows are marked ``published`` after a
    fire-and-forget publish, so events with no live subscriber are silently
    lost. Logged once per route per process so periodic relay loops do not
    spam the log.
    """

    if outbox_route in _PUBSUB_DOWNGRADE_WARNED:
        return

    _PUBSUB_DOWNGRADE_WARNED.add(outbox_route)
    logger.warning(
        "Outbox route relays to a pubsub topic: delivery downgrades to "
        "at-most-once past the broker (rows are marked published after a "
        "fire-and-forget publish; zero live subscribers means silent loss). "
        "Legitimate for lossy broadcast; use a queue or stream destination "
        "for must-arrive events.",
        outbox_route=outbox_route,
        channel=channel,
    )


# ....................... #


def _require_destination(
    destination: "OutboxDestination | None",
    *,
    expected_kind: OutboxDestinationKind,
) -> "OutboxDestination":
    if destination is None:
        raise exc.configuration(
            f"outbox_spec.destination is required for {expected_kind} relay", code=_MISCONFIGURED
        )

    if destination.kind != expected_kind:
        raise exc.configuration(
            f"outbox_spec.destination.kind must be {expected_kind!r}, got {destination.kind!r}",
            code=_MISCONFIGURED,
        )

    return destination


# ....................... #


def _assert_route_matches(destination: "OutboxDestination", spec_name: str) -> None:
    if str(destination.route) != str(spec_name):
        raise exc.configuration(
            f"spec.name must match OutboxSpec.destination.route for relay "
            f"(expected {destination.route!r}, got {spec_name!r})",
            code=_MISCONFIGURED,
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


def _run_target(
    outbox_spec: OutboxSpec[Any],
    *,
    queue_spec: QueueSpec[Any] | None,
    stream_spec: StreamSpec[Any] | None,
    pubsub_spec: PubSubSpec[Any] | None,
) -> tuple[OutboxDestinationKind, BaseSpec]:
    """The destination kind :meth:`OutboxRelay.run` dispatches to, and its transport spec."""

    destination = outbox_spec.destination

    if destination is None:
        raise exc.configuration(
            "outbox_spec.destination is required for run()", code=_MISCONFIGURED
        )

    specs: dict[str, BaseSpec | None] = {
        "queue": queue_spec,
        "stream": stream_spec,
        "pubsub": pubsub_spec,
    }

    if destination.kind not in specs:
        raise exc.configuration(
            f"unsupported outbox destination kind: {destination.kind!r}", code=_MISCONFIGURED
        )

    spec = specs[destination.kind]

    if spec is None:
        raise exc.configuration(
            f"{destination.kind}_spec is required when destination.kind is {destination.kind}",
            code=_MISCONFIGURED,
        )

    return destination.kind, spec


# ....................... #


def validate_relay_wiring(
    outbox_spec: OutboxSpec[Any],
    *,
    transport: OutboxDestinationKind,
    queue_spec: QueueSpec[Any] | None,
    stream_spec: StreamSpec[Any] | None,
    pubsub_spec: PubSubSpec[Any] | None,
) -> None:
    """Check, before any pass runs, the destination a relay over *transport* would resolve.

    A ``queue`` transport relays with :meth:`OutboxRelay.to_queue`, whose destination may be
    unset; any other dispatches through :meth:`OutboxRelay.run` on the destination's kind.

    :raises CoreException: ``configuration`` with code ``outbox_relay_misconfigured``, the
        same refusal the first pass would raise.
    """

    if transport == "queue":
        if queue_spec is None:
            raise exc.configuration(
                "queue_spec is required when transport is queue", code=_MISCONFIGURED
            )

        kind: OutboxDestinationKind = "queue"
        spec: BaseSpec = queue_spec
        allow_unset = True

    else:
        kind, spec = _run_target(
            outbox_spec, queue_spec=queue_spec, stream_spec=stream_spec, pubsub_spec=pubsub_spec
        )
        allow_unset = False

    _resolve_channel(
        outbox_spec, spec_name=str(spec.name), expected_kind=kind, allow_unset=allow_unset
    )


# ....................... #


def _claim_envelope_headers(claim: "OutboxClaim") -> dict[str, str]:
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

    if claim.hlc is not None:
        headers[HEADER_HLC] = claim.hlc.encode()

    if claim.traceparent is not None:
        headers[HEADER_TRACEPARENT] = claim.traceparent

    return headers


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class OutboxRelay:
    """Configurable relay for one outbox route to a queue, stream, or pubsub backend.

    Holds the relay *configuration* — the outbox route and its retry / reclaim policy,
    validated once on construction; :meth:`to_queue` / :meth:`to_stream` / :meth:`to_pubsub`
    (or :meth:`run`, which dispatches on the configured destination kind) take the per-run
    context, the target transport spec, and an optional ``limit``.

    Delivery is **at-least-once**, and ordering is **not preserved across failures/retries**:
    a row rescheduled (or parked as ``failed``) does not stall later rows — including later
    rows of the same ``ordering_key`` (deliberate trade-off: no per-key head-of-line
    blocking). Rows are claimed (``pending`` → ``processing``) in ``created_at`` order,
    relayed one message per claim, then marked ``published``; a store that consumes its
    rows locks them at the claim instead and deletes each once published, in one
    transaction per pass. Relay and ``mark_published`` are not atomic — consumers must
    deduplicate on
    :attr:`~forze.application.contracts.outbox.IntegrationEvent.event_id` (the
    ``forze_event_id`` header) and tolerate reordering.

    Per-row failure handling (one row's failure never aborts the batch): payload-decode
    errors (poison) → ``mark_failed`` immediately (an operator re-drives with
    ``requeue_failed`` after fixing the cause); broker publish errors (transient) →
    rescheduled with exponential backoff + jitter (``retry_base_delay * 2**attempts``,
    capped at :attr:`retry_max_backoff`) until :attr:`max_attempts` is exhausted, then
    ``mark_failed``. Each publish forwards the claim's invocation envelope as transport
    headers and passes ``key = ordering_key or str(event_id)`` for partitioning on capable
    transports; the event id always rides the ``forze_event_id`` header for consumer dedup.
    """

    outbox_spec: OutboxSpec[Any]
    """The outbox route this relay drains."""

    reclaim_stale_after: timedelta | None = timedelta(minutes=5)
    """Rows stuck in ``processing`` longer than this lease are reset to ``pending`` before
    claim (requires ``processing_at`` on the store). ``None`` skips reclaim."""

    max_attempts: int = 5
    """Publish attempts before a transiently-failing row is parked ``failed``."""

    retry_base_delay: timedelta = timedelta(seconds=1)
    """Base of the exponential backoff between publish retries."""

    retry_max_backoff: timedelta = timedelta(minutes=5)
    """Cap on the backoff delay."""

    publish_timeout: timedelta | None = None
    """Longest one publish may take; a publish that outlasts it is a transient failure,
    retried with backoff. ``None`` leaves a publish unbounded, unless the store bounds the
    pass (consume storage), which caps each publish at what is left of the pass."""

    publish_until: float | None = None
    """Event-loop time (``loop.time()``) after which a run starts no further publish, and caps
    each publish at, then marks what it published, for a caller that must be done by a
    deadline. Set it per run. Rows claimed but not reached are left as a cut pass leaves
    them: pending again for a store that runs a pass in one transaction, ``processing``
    until reclaim for one that does not."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        validate_retry_options(
            max_attempts=self.max_attempts,
            retry_base_delay=self.retry_base_delay,
            retry_max_backoff=self.retry_max_backoff,
            publish_timeout=self.publish_timeout,
        )

        if self.reclaim_stale_after is not None and self.reclaim_stale_after.total_seconds() <= 0:
            raise exc.configuration("Reclaim stale after must be positive")

    # ....................... #

    async def to_queue(
        self,
        ctx: "ExecutionContext",
        queue_spec: QueueSpec[Any],
        *,
        limit: int | None = None,
    ) -> OutboxRelayResult:
        """Claim pending rows, enqueue payloads, and mark each row's outcome.

        The logical queue channel comes from
        :attr:`~forze.application.contracts.outbox.OutboxSpec.destination` when set;
        otherwise *queue_spec* ``name`` is used as the channel.
        """

        return await self._relay(
            ctx,
            queue_spec,
            dep_key=QueueCommandDepKey,
            expected_kind="queue",
            method="enqueue",
            allow_unset_destination=True,
            limit=limit,
        )

    # ....................... #

    async def to_stream(
        self,
        ctx: "ExecutionContext",
        stream_spec: StreamSpec[Any],
        *,
        limit: int | None = None,
    ) -> OutboxRelayResult:
        """Claim pending rows, append to a stream, and mark each row's outcome.

        Same failure model and ordering caveats as :meth:`to_queue`.
        """

        return await self._relay(
            ctx,
            stream_spec,
            dep_key=StreamCommandDepKey,
            expected_kind="stream",
            method="append",
            limit=limit,
        )

    # ....................... #

    async def to_pubsub(
        self,
        ctx: "ExecutionContext",
        pubsub_spec: PubSubSpec[Any],
        *,
        limit: int | None = None,
    ) -> OutboxRelayResult:
        """Claim pending rows, publish to a topic, and mark each row's outcome.

        Same failure model and ordering caveats as :meth:`to_queue` — **but the
        at-least-once guarantee ends at the broker**: pubsub is at-most-once
        (fire-and-forget), and a row is marked ``published`` after a publish that no
        live subscriber may have received (see
        :meth:`~forze.application.contracts.outbox.OutboxDestination.pubsub`). A one-time
        warning per outbox route is logged to make the downgrade visible.
        """

        destination = self.outbox_spec.destination

        if destination is not None and destination.kind == "pubsub":
            # Missing/mismatched destinations fall through to the configuration error
            # inside ``_relay`` without a spurious warning.
            _warn_pubsub_downgrade_once(str(self.outbox_spec.name), destination.channel)

        return await self._relay(
            ctx,
            pubsub_spec,
            dep_key=PubSubCommandDepKey,
            expected_kind="pubsub",
            method="publish",
            limit=limit,
        )

    # ....................... #

    async def run(
        self,
        ctx: "ExecutionContext",
        *,
        queue_spec: QueueSpec[Any] | None = None,
        stream_spec: StreamSpec[Any] | None = None,
        pubsub_spec: PubSubSpec[Any] | None = None,
        limit: int | None = None,
    ) -> OutboxRelayResult:
        """Relay using the configured :attr:`OutboxSpec.destination` kind.

        Dispatches to :meth:`to_queue` / :meth:`to_stream` / :meth:`to_pubsub` by the
        destination kind; the matching transport spec must be supplied.
        """

        kind, spec = _run_target(
            self.outbox_spec,
            queue_spec=queue_spec,
            stream_spec=stream_spec,
            pubsub_spec=pubsub_spec,
        )

        match kind:
            case "queue":
                return await self.to_queue(ctx, cast(QueueSpec[Any], spec), limit=limit)

            case "stream":
                return await self.to_stream(ctx, cast(StreamSpec[Any], spec), limit=limit)

            case "pubsub":
                return await self.to_pubsub(ctx, cast(PubSubSpec[Any], spec), limit=limit)

    # ....................... #

    async def _relay(
        self,
        ctx: "ExecutionContext",
        spec: BaseSpec,
        *,
        dep_key: DepKey[Any],
        expected_kind: OutboxDestinationKind,
        method: str,
        allow_unset_destination: bool = False,
        limit: int | None,
    ) -> OutboxRelayResult:
        """Resolve the transport command port and relay each claim via ``command.<method>``.

        The command-port method (``enqueue``/``append``/``publish``) shares the same
        ``(channel, payload, *, type, key, headers)`` signature across transports, so only
        the resolved command, dep key, and method name differ.
        """

        channel = _resolve_channel(
            self.outbox_spec,
            spec_name=str(spec.name),
            expected_kind=expected_kind,
            allow_unset=allow_unset_destination,
        )

        command = ctx.deps.resolve_configurable(ctx, dep_key, spec, route=spec.name)

        async def _publish(claim: "OutboxClaim", payload: Any) -> None:
            await getattr(command, method)(
                channel,
                payload,
                type=claim.event_type,
                key=claim.ordering_key or str(claim.event_id),
                headers=_claim_envelope_headers(claim),
            )

        return await relay_outbox_claims(
            ctx,
            outbox_spec=self.outbox_spec,
            publish_one=_publish,
            limit=limit,
            reclaim_stale_after=self.reclaim_stale_after,
            max_attempts=self.max_attempts,
            retry_base_delay=self.retry_base_delay,
            retry_max_backoff=self.retry_max_backoff,
            publish_timeout=self.publish_timeout,
            publish_until=self.publish_until,
        )
