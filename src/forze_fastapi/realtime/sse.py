"""The SSE egress route — replay-from-cursor, then live tail; mailbox-first.

The offline-mailbox doctrine (the mailbox is the source of truth for durable
principal delivery; the live emit is a latency optimization) is natural on SSE,
because SSE's reconnect model — ``Last-Event-ID`` — *is* a cursor protocol:

- **Connect**: the governed middlewares already authenticated the request; the route
  resolves the client key (``device_id`` → per-principal default), then replays the
  mailbox past the cursor as SSE frames whose ``id:`` is the durable event id. A
  browser-supplied ``Last-Event-ID`` (sent on auto-reconnect) takes precedence over
  the stored cursor — the browser's native resume beats a stale server cursor.
- **Live**: after replay, the response tails the node's
  :class:`~forze_fastapi.realtime.RealtimeSseHub` and forwards matching signals.
  Without a hub the stream ends after replay (catch-up mode — the browser
  auto-reconnects with ``Last-Event-ID``, giving long-poll-style delivery).
- **Ack**: SSE has no upstream channel, so the cumulative ack rides a small POST
  endpoint attached alongside, sharing the kernel's cursor machinery.

Every frame carries the same ``{id, data}`` envelope as the Socket.IO transport —
one protocol (versioned via the ``protocol`` query parameter), two transports. The
gateway's admission/dedup story is **not** duplicated here: durables come from the
mailbox (already deduped and HLC-ordered by the store-then-forward gateway), the
live leg is at-most-once, and clients dedup by envelope id.
"""

from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import aclosing
from datetime import timedelta
from typing import Annotated, Any, final
from uuid import UUID

import attrs
from fastapi import APIRouter, Query, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from forze.application.contracts.authn import ClientIdentity
from forze.application.contracts.realtime import MailboxEntry
from forze.application.execution import ExecutionContext
from forze.application.execution.context import ExecutionContextFactory
from forze.application.integrations.realtime import (
    BacklogDrain,
    MailboxCursors,
    RealtimeAck,
    RealtimeMailbox,
    RealtimePresence,
    acknowledge_up_to,
    encode_frame,
    iter_backlog,
    negotiate_realtime_protocol,
    resolve_client_key,
)
from forze.base.exceptions import exc
from forze.base.logging import Logger
from forze.base.primitives import HlcTimestamp, uuid7

from .._logging import ForzeFastAPILogger
from .hub import RealtimeSseHub, SseSubscription, presence_rooms

# ----------------------- #

_logger = Logger(ForzeFastAPILogger.ERRORS)

__all__ = [
    "attach_realtime_sse_route",
    "RealtimeAckBody",
    "RealtimeAckResult",
    "TopicAuthorizer",
]

LAST_EVENT_ID_HEADER = "Last-Event-ID"
"""The browser-native SSE resume header, sent automatically on reconnect."""

TopicAuthorizer = Callable[
    [ExecutionContext, str, UUID | None, frozenset[str]], Awaitable[frozenset[str]]
]
"""Authorize a connection's requested topic subscriptions: return the granted subset.

``(ctx, principal, tenant, requested) -> granted``. The SSE analog of the app-side
``enter_room`` decision on Socket.IO: topic membership is an authorization the app
owns, never a client-asserted fact. The route refuses the connection unless every
requested topic is granted — a silently-narrowed subscription would read as
"subscribed" while delivering nothing."""

_SSE_HEADERS = {
    "Cache-Control": "no-store",
    "X-Accel-Buffering": "no",  # proxies must not buffer an event stream
}


class RealtimeAckBody(RealtimeAck):
    """``POST .../ack`` body — the kernel's shared ack shape, one wire contract."""


class RealtimeAckResult(BaseModel):
    """Whether the ack advanced the cursor (``False``: id no longer retained)."""

    acked: bool


# ....................... #


def _sse_frame(*, event: str, event_id: str | None, payload: dict[str, Any]) -> str:
    """One ``text/event-stream`` frame carrying the shared ``{id, data}`` envelope.

    ``id:`` is set only for durable signals, so the browser's ``Last-Event-ID``
    always names a mailbox entry the resume can anchor on.
    """

    lines = [f"id: {event_id}"] if event_id is not None else []
    envelope = {"id": event_id, "data": payload}
    lines.append(f"event: {event}")
    # encode_frame: a live hub payload's JsonDict shape is a claim, not an
    # enforcement — an unencodable one costs this frame, never the stream.
    lines.append(f"data: {encode_frame(envelope)}")

    return "\n".join(lines) + "\n\n"


# ....................... #


def _authenticated_principal(ctx: ExecutionContext) -> str:
    authn = ctx.inv_ctx.get_authn()

    if authn is None:
        raise exc.authentication("The realtime SSE stream requires an authenticated principal")

    return str(authn.principal_id)


def _client_key(principal: str, device_id: str | None) -> str:
    # The kernel ladder, with an SSE-shaped fallback: no per-connection identifier
    # survives an SSE reconnect, so device-less browsers share one per-principal
    # cursor — Last-Event-ID still resumes each of them precisely.
    return resolve_client_key(ClientIdentity(device_id=device_id), fallback=f"sse:{principal}")


def _parse_topics(raw: str | None) -> frozenset[str]:
    if not raw:
        return frozenset()

    return frozenset(topic.strip() for topic in raw.split(",") if topic.strip())


async def _require_topic_grant(
    ctx: ExecutionContext,
    authorizer: TopicAuthorizer | None,
    *,
    principal: str,
    tenant: UUID | None,
    requested: frozenset[str],
    max_topics: int,
) -> None:
    """Fail closed on topic subscriptions: bounded, and granted by the app or refused."""

    if len(requested) > max_topics:
        # client-controlled fan-out state (hub matching + presence rooms) must be bounded
        raise exc.validation(
            f"Too many topic subscriptions ({len(requested)}; the limit is {max_topics})",
            code="realtime_topics_limit",
        )

    if authorizer is None:
        raise exc.authorization(
            "Topic subscriptions are refused: no authorize_topics resolver is wired, "
            "and topic membership is the app's authorization decision",
            code="realtime_topics_unauthorized",
        )

    granted = await authorizer(ctx, principal, tenant, requested)
    denied = requested - granted

    if denied:
        raise exc.authorization(
            f"Topic subscription denied: {sorted(denied)}",
            code="realtime_topics_unauthorized",
            details={"denied": sorted(denied)},
        )


# ....................... #


_HUB_READY_TIMEOUT = 15.0
"""Seconds a connection waits for the live tail's startup fast-forward.

After startup the gate is a no-op (the event stays set). The timeout is the
fail-open bound for a miswired hub (configured but never fed by a tail step):
the connection proceeds in catch-up quality, loudly, instead of hanging.
"""


async def _await_hub_ready(hub: RealtimeSseHub) -> None:
    """Wait until live signals flow into the hub; fail open (logged) on timeout."""

    try:
        await asyncio.wait_for(hub.ready.wait(), _HUB_READY_TIMEOUT)

    except TimeoutError:
        _logger.error(
            "SSE hub not ready after %ss — serving catch-up quality; is the tail "
            "lifecycle step registered and healthy?",
            _HUB_READY_TIMEOUT,
        )


@final
@attrs.define(slots=True, frozen=True)
class _ReplayResume:
    """Where a replay starts, plus the id anchor that makes a mid-run resume lossless."""

    since: HlcTimestamp | None
    """The replay's strict-greater start position."""

    anchor_hlc: HlcTimestamp | None = None
    """The resumed entry's position: replayed entries strictly below it were
    delivered before the tear and are dropped instead of framed."""

    anchor_id: str | None = None
    """The resumed entry itself — the one id at ``anchor_hlc`` known delivered."""

    # ....................... #

    def admits(self, entry: MailboxEntry) -> bool:
        """Whether a replayed entry is new to the resuming client (see
        :func:`_resolve_since` — the covered prefix and the resumed id drop)."""

        if self.anchor_hlc is None:
            return True

        return entry.hlc >= self.anchor_hlc and entry.event_id != self.anchor_id


async def _resolve_since(
    mailbox: RealtimeMailbox,
    cursors: MailboxCursors,
    *,
    principal: str,
    client_key: str,
    last_event_id: str | None,
) -> _ReplayResume:
    """The replay start: ``Last-Event-ID`` (browser resume) beats the stored cursor.

    A resumed id cannot become the start position directly: distinct entries can
    share one HLC, and the strict-greater replay would then skip the resumed
    entry's undelivered equal-HLC siblings on every reconnect. So the replay
    starts from the stored cursor (whose position claims its whole run) and the
    resumed id becomes an **anchor**: the already-delivered prefix below it is
    dropped, its own id is dropped, and everything else — the siblings included —
    is framed. Siblings delivered before the tear re-send; the client dedups by
    envelope id either way, as it must (a durable signal can also arrive twice
    through the mailbox + live races).

    An id no longer retained falls back to the cursor alone.
    """

    cursor = await cursors.get(principal=principal, client_key=client_key)

    if last_event_id:
        position = await mailbox.position_of(principal=principal, event_id=last_event_id)

        if position is not None and (cursor is None or cursor < position):
            return _ReplayResume(since=cursor, anchor_hlc=position, anchor_id=last_event_id)

    return _ReplayResume(since=cursor)


async def _replay_frames(
    mailbox: RealtimeMailbox,
    *,
    principal: str,
    resume: _ReplayResume,
    outcome: BacklogDrain,
) -> AsyncIterator[str]:
    """The backlog past the resume position, as SSE frames (oldest-first).

    *outcome* reports whether the backlog confirmably drained — only then may the
    stream proceed to its live tail (see the caller).
    """

    # This generator is aclosed when the response tears mid-replay; ``aclosing``
    # propagates that closure into ``iter_backlog`` (and through it into the
    # mailbox's paged stream) instead of leaving them to GC finalization.
    async with aclosing(
        iter_backlog(mailbox, principal=principal, since=resume.since, outcome=outcome)
    ) as entries:
        async for entry in entries:
            if not resume.admits(entry):
                continue  # delivered before the tear (see _resolve_since)

            yield _sse_frame(event=entry.event, event_id=entry.event_id, payload=entry.payload)


async def _live_frames(
    subscription: SseSubscription, *, keepalive_interval: timedelta
) -> AsyncIterator[str]:
    """The live tail: matched hub signals as SSE frames, keepalive comments between."""

    pending: asyncio.Task[Any] | None = None

    try:
        while True:
            if pending is None:
                pending = asyncio.ensure_future(subscription.queue.get())

            # Never cancel the pending get on timeout — a cancelled Queue.get can
            # drop a wakeup; the same task just carries over the iteration.
            done, _ = await asyncio.wait({pending}, timeout=keepalive_interval.total_seconds())

            if not done:
                yield ": keepalive\n\n"
                continue

            signal, event_id = pending.result()
            pending = None
            yield _sse_frame(event=signal.event, event_id=event_id, payload=dict(signal.payload))

    finally:
        if pending is not None:
            pending.cancel()


# ----------------------- #


def attach_realtime_sse_route(
    router: APIRouter,
    *,
    ctx_dep: ExecutionContextFactory,
    mailbox_factory: Callable[[ExecutionContext], RealtimeMailbox],
    cursors_factory: Callable[[ExecutionContext], MailboxCursors],
    hub: RealtimeSseHub | None = None,
    presence: RealtimePresence | None = None,
    authorize_topics: TopicAuthorizer | None = None,
    max_topics: int = 32,
    path: str = "/realtime/sse",
    keepalive_interval: timedelta = timedelta(seconds=15),
) -> APIRouter:
    """Attach the authenticated SSE egress endpoint (and its ack endpoint) to *router*.

    Requires the governed middlewares (identity is read from the bound context — the
    route never parses credentials itself). Pass a *hub* fed by
    :func:`~forze_fastapi.realtime.realtime_sse_tail_lifecycle_step` to serve live
    signals after the replay; without one the endpoint is catch-up-only. The ack
    endpoint is attached at ``{path}/ack`` and shares the handshake parameters, so
    both derive the same client key.

    Pass the **same** *presence* store the Socket.IO side uses and open SSE streams
    join their principal/topic rooms for the connection's lifetime, so presence-based
    decisions count SSE-connected users as online. With a TTL-backed store also
    register :func:`~forze_fastapi.realtime.realtime_sse_presence_heartbeat_lifecycle_step`
    (sharing the hub) so live streams re-assert within the TTL.

    Topic subscriptions (``?topics=a,b``) are **fail-closed**: they require an
    *authorize_topics* resolver, and the connection is refused unless every requested
    topic is granted — topic membership is the app's authorization decision (the
    Socket.IO analog is app code calling ``enter_room``), never a client-asserted
    fact. Principal-addressed delivery needs no authorizer.
    """

    if keepalive_interval.total_seconds() <= 0:
        raise exc.configuration("SSE keepalive interval must be positive")

    if max_topics <= 0:
        raise exc.configuration("max_topics must be positive")

    ack_path = f"{path}/ack"

    # ....................... #

    async def stream_endpoint(
        request: Request,
        protocol: Annotated[str | None, Query()] = None,
        device_id: Annotated[str | None, Query()] = None,
        topics: Annotated[str | None, Query()] = None,
    ) -> StreamingResponse:
        ctx = ctx_dep()
        principal = _authenticated_principal(ctx)
        negotiate_realtime_protocol(protocol)

        tenant_identity = ctx.inv_ctx.get_tenant()
        tenant = tenant_identity.tenant_id if tenant_identity is not None else None
        client_key = _client_key(principal, device_id)
        mailbox = mailbox_factory(ctx)
        cursors = cursors_factory(ctx)
        last_event_id = request.headers.get(LAST_EVENT_ID_HEADER)
        topic_set = _parse_topics(topics)

        if topic_set:
            await _require_topic_grant(
                ctx,
                authorize_topics,
                principal=principal,
                tenant=tenant,
                requested=topic_set,
                max_topics=max_topics,
            )

        subscription: SseSubscription | None = (
            hub.subscribe(principal=principal, tenant=tenant, topics=topic_set)
            if hub is not None
            else None
        )

        # Presence: an open SSE stream occupies the same rooms a Socket.IO connection
        # would, under a per-response member key (the sid analog) — one store, one
        # naming scheme, so "is this principal online" is transport-agnostic.
        member_key = subscription.key if subscription is not None else f"sse:{uuid7()}"
        rooms = (
            presence_rooms(principal=principal, tenant=tenant, topics=topic_set)
            if presence is not None
            else ()
        )

        async def stream() -> AsyncIterator[str]:
            # Subscribed before the replay drains, so a signal landing mid-replay is
            # queued rather than lost; a durable one may then arrive twice (mailbox +
            # live), which the client's id-dedup collapses.
            try:
                for room in rooms:
                    await presence.joined(room, member_key)  # type: ignore[union-attr]

                if hub is not None:
                    # The replay cursor resolves only once the hub is live: a durable
                    # the tail's startup fast-forward skipped is in the mailbox by
                    # then, so this replay delivers it instead of losing it until
                    # the client's next reconnect.
                    await _await_hub_ready(hub)

                resume = await _resolve_since(
                    mailbox,
                    cursors,
                    principal=principal,
                    client_key=client_key,
                    last_event_id=last_event_id,
                )

                outcome = BacklogDrain(claim_floor=resume.since)

                async for frame in _replay_frames(
                    mailbox, principal=principal, resume=resume, outcome=outcome
                ):
                    yield frame

                if subscription is None:
                    return  # catch-up mode: the browser reconnects with Last-Event-ID

                # Entering the live tail is only safe after a CONFIRMED drain: a
                # backlog still cap-filled after ``iter_backlog``'s rounds may hold
                # an undelivered middle, and a later unclamped ack would advance the
                # cursor over it (the all-device trim then hard-deletes it). An
                # unconfirmed drain ends the stream instead — the browser reconnects
                # with Last-Event-ID and the next replay continues from there.
                if not outcome.complete:
                    return

                async for frame in _live_frames(
                    subscription, keepalive_interval=keepalive_interval
                ):
                    yield frame

            finally:
                if subscription is not None:
                    hub.unsubscribe(subscription)  # type: ignore[union-attr]

                for room in rooms:
                    # best-effort: a failed leave must not mask the stream's own exit
                    # (the TTL store expires the row; the in-memory one leaks one key)
                    try:
                        await presence.left(room, member_key)  # type: ignore[union-attr]

                    except Exception:
                        _logger.exception("SSE presence leave failed", room=room)

        return StreamingResponse(stream(), media_type="text/event-stream", headers=_SSE_HEADERS)

    # ....................... #

    async def ack_endpoint(
        body: RealtimeAckBody,
        device_id: Annotated[str | None, Query()] = None,
    ) -> RealtimeAckResult:
        ctx = ctx_dep()
        principal = _authenticated_principal(ctx)

        if not device_id:
            # Device-less streams share ONE per-principal fallback cursor: a cumulative
            # ack on that multi-writer cursor lets one browser tab advance the trim
            # floor over another tab's undelivered backlog, which the all-device trim
            # then hard-deletes. Streams stay device-less-friendly (Last-Event-ID
            # resumes each tab precisely); the durable cursor needs a device identity.
            raise exc.validation(
                "The SSE ack requires ?device_id=... — without one every tab of this "
                "principal shares a single cursor, and one tab's cumulative ack would "
                "let the trim delete another tab's undelivered backlog. Pass a stable "
                "per-device id (on the stream endpoint too), or skip acks and rely on "
                "Last-Event-ID resume.",
                code="realtime_ack_requires_device",
            )

        position = await acknowledge_up_to(
            mailbox_factory(ctx),
            cursors_factory(ctx),
            principal=principal,
            client_key=_client_key(principal, device_id),
            event_id=body.up_to,
        )

        return RealtimeAckResult(acked=position is not None)

    # ....................... #

    router.get(
        path,
        name="realtime_sse_stream",
        summary="Realtime egress over Server-Sent Events (replay, then live)",
        response_class=StreamingResponse,
    )(stream_endpoint)

    router.post(
        ack_path,
        name="realtime_sse_ack",
        summary="Cumulative realtime ack (advance this device's replay cursor)",
    )(ack_endpoint)

    return router
