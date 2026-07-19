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
import json
from collections.abc import AsyncIterator, Callable
from datetime import timedelta
from typing import Annotated, Any

from fastapi import APIRouter, Query, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from forze.application.contracts.authn import ClientIdentity
from forze.application.execution import ExecutionContext
from forze.application.execution.context import ExecutionContextFactory
from forze.application.integrations.realtime import (
    MailboxCursors,
    RealtimeMailbox,
    acknowledge_up_to,
    iter_replay,
    negotiate_realtime_protocol,
    resolve_client_key,
)
from forze.base.exceptions import exc
from forze.base.primitives import HlcTimestamp

from .hub import RealtimeSseHub, SseSubscription

# ----------------------- #

__all__ = [
    "attach_realtime_sse_route",
    "RealtimeAckBody",
    "RealtimeAckResult",
]

LAST_EVENT_ID_HEADER = "Last-Event-ID"
"""The browser-native SSE resume header, sent automatically on reconnect."""

_SSE_HEADERS = {
    "Cache-Control": "no-store",
    "X-Accel-Buffering": "no",  # proxies must not buffer an event stream
}


class RealtimeAckBody(BaseModel):
    """``POST .../ack`` body: cumulative ack up to (and including) an event id."""

    up_to: str


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
    lines.append(f"data: {json.dumps(envelope, separators=(',', ':'))}")

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


# ....................... #


async def _replay_frames(
    mailbox: RealtimeMailbox, *, principal: str, since: HlcTimestamp | None
) -> AsyncIterator[str]:
    """The backlog past *since*, as SSE frames (oldest-first, ids anchor the resume)."""

    async for entry in iter_replay(mailbox, principal=principal, since=since):
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
    """

    if keepalive_interval.total_seconds() <= 0:
        raise exc.configuration("SSE keepalive interval must be positive")

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

        # Last-Event-ID (browser resume) beats the stored cursor; an id no longer
        # retained falls back to the cursor — the client dedups by envelope id.
        since: HlcTimestamp | None = None
        last_event_id = request.headers.get(LAST_EVENT_ID_HEADER)

        if last_event_id:
            since = await mailbox.position_of(principal=principal, event_id=last_event_id)

        if since is None:
            since = await cursors.get(principal=principal, client_key=client_key)

        subscription: SseSubscription | None = (
            hub.subscribe(principal=principal, tenant=tenant, topics=_parse_topics(topics))
            if hub is not None
            else None
        )

        async def stream() -> AsyncIterator[str]:
            # Subscribed before the replay drains, so a signal landing mid-replay is
            # queued rather than lost; a durable one may then arrive twice (mailbox +
            # live), which the client's id-dedup collapses.
            try:
                async for frame in _replay_frames(mailbox, principal=principal, since=since):
                    yield frame

                if subscription is None:
                    return  # catch-up mode: the browser reconnects with Last-Event-ID

                async for frame in _live_frames(
                    subscription, keepalive_interval=keepalive_interval
                ):
                    yield frame

            finally:
                if subscription is not None:
                    hub.unsubscribe(subscription)  # type: ignore[union-attr]

        return StreamingResponse(stream(), media_type="text/event-stream", headers=_SSE_HEADERS)

    # ....................... #

    async def ack_endpoint(
        body: RealtimeAckBody,
        device_id: Annotated[str | None, Query()] = None,
    ) -> RealtimeAckResult:
        ctx = ctx_dep()
        principal = _authenticated_principal(ctx)

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
