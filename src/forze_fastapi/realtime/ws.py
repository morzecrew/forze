"""The raw-WebSocket realtime transport — egress twin of SSE, duplex twin of Socket.IO.

One governed route carries both halves of the wire protocol:

- **Egress** (always): on connect — identity resolved by the app's resolver from the
  upgrade request, protocol negotiated, topics authorized fail-closed — the socket
  replays the offline mailbox past the device's cursor and then forwards live hub
  signals, every frame the shared envelope with the event name in-band:
  ``{"event": <name>, "id": <id|null>, "data": <payload>}``.
- **Ingress** (frame-typed): ``{"type": "realtime.ack", "up_to": …}`` advances the
  replay cursor; ``{"type": "realtime.reauth", "auth": …}`` re-verifies a rotating
  credential in place (same principal and tenant only); and — when a registry and
  command routes are attached — ``{"type": "cmd", "event", "cid", "payload"}``
  dispatches through the same frozen-registry discipline as HTTP and Socket.IO,
  acknowledged as ``{"type": "ack", "cid", "data"}`` or
  ``{"type": "ack", "cid", "error": <shared error envelope>}``.

The middlewares refuse websocket scopes, so list this route's path in their
``allowed_websocket_paths`` — never ``allow_raw_websockets=True``, which would
reopen the hole for every route. Keepalive is WS protocol-level ping/pong (the
ASGI server's; e.g. uvicorn's ``ws_ping_interval``) — no application frames.
Limits are fail-loud: an oversized frame closes the socket (1009), a command past
the in-flight bound is error-acked without running.
"""

from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

import asyncio
import json
import math
from collections.abc import Awaitable, Callable, Mapping, Sequence
from datetime import datetime
from inspect import isawaitable
from typing import Any, cast, final
from uuid import UUID

import attrs
from fastapi import APIRouter, WebSocket
from pydantic import ValidationError
from starlette.websockets import WebSocketDisconnect

from forze.application.contracts.authn import AuthnIdentity, ClientIdentity
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import ExecutionContext
from forze.application.execution.context import ExecutionContextFactory
from forze.application.execution.operations import FrozenOperationRegistry, run_operation
from forze.application.integrations.realtime import (
    MailboxCursors,
    RealtimeCommandRoute,
    RealtimeMailbox,
    RealtimePresence,
    acknowledge_up_to,
    iter_replay,
    negotiate_realtime_protocol,
    resolve_client_key,
)
from forze.base.exceptions import (
    CoreException,
    ErrorEnvelope,
    FrameErr,
    error_envelope,
    exc,
    guard_frame,
)
from forze.base.logging import Logger
from forze.base.primitives import utcnow, uuid7
from forze.base.scrubbing import sanitize_pydantic_errors

from .._logging import ForzeFastAPILogger
from ..middlewares.raw_websocket import GOVERNED_WEBSOCKET_ATTR
from .hub import RealtimeSseHub, SseSubscription, presence_rooms
from .sse import (
    TopicAuthorizer,
    _await_hub_ready,  # pyright: ignore[reportPrivateUsage]
    _parse_topics,  # pyright: ignore[reportPrivateUsage]
    _require_topic_grant,  # pyright: ignore[reportPrivateUsage]
    _resolve_since,  # pyright: ignore[reportPrivateUsage]
)

# ----------------------- #

_logger = Logger(ForzeFastAPILogger.ERRORS)

__all__ = [
    "WsConnect",
    "WsConnection",
    "WsConnectionResolver",
    "attach_realtime_ws_route",
]

WS_POLICY_CLOSE = 1008
"""Close code for refused connects (auth, protocol, topics) — policy violation."""

WS_TOO_BIG_CLOSE = 1009
"""Close code for an oversized inbound frame."""

WS_UNSUPPORTED_DATA_CLOSE = 1003
"""Close code for a non-text (binary) inbound frame — the protocol is JSON text."""

_EXPIRY_CHECK_CEILING_SECONDS = 30.0
"""Upper bound between credential-expiry checks, so a reauth that *shortens*
``expires_at`` is enforced within this window even mid-sleep."""

FRAME_ACK = "realtime.ack"
FRAME_REAUTH = "realtime.reauth"
FRAME_CMD = "cmd"


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class WsConnect:
    """What the connection resolver sees — the socket plus an optional auth payload.

    At connect time *auth* is ``None`` (resolve from the upgrade request's headers /
    query parameters); at ``realtime.reauth`` it carries the frame's fresh payload.
    """

    websocket: WebSocket
    """The live socket — upgrade-request headers and query parameters included."""

    auth: Mapping[str, Any] | None = None
    """A ``realtime.reauth`` payload, or ``None`` for the initial connect."""


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class WsConnection:
    """The resolved identity of a live WebSocket connection."""

    authn: AuthnIdentity
    """Authenticated principal."""

    tenant: UUID | None = None
    """The connection's tenant; scopes its rooms, mailbox, and live matching."""

    client: ClientIdentity | None = None
    """The device/session this connection is, keying its offline-replay cursor."""

    expires_at: datetime | None = None
    """When the connection's credential expires — a **timezone-aware** (UTC) instant;
    ``None`` never expires.

    Captured from the verified token at connect/reauth time. The route enforces it
    continuously — a socket past this instant is closed (policy code), so a long-lived
    connection can't outlive the credential that authenticated it. Set it when
    resolving, or a stolen token stays live for the socket's whole lifetime. A naive
    datetime is refused at construction: it cannot be compared against the aware
    enforcement clock, so it would surface as a connection-killing ``TypeError`` at
    expiry-check time instead of an actionable error here."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.expires_at is not None and self.expires_at.tzinfo is None:
            raise exc.configuration(
                "WsConnection.expires_at must be a timezone-aware (UTC) datetime; a "
                "naive value cannot be compared against the aware enforcement clock. "
                "Resolve it with tzinfo set (e.g. from the token's exp claim in UTC).",
                code="realtime_expiry_naive",
            )

    # ....................... #

    @property
    def principal(self) -> str:
        return str(self.authn.principal_id)

    # ....................... #

    def client_key(self, fallback: str) -> str:
        """The stable cursor key: ``device_id``/``session_id``, else *fallback*."""

        return resolve_client_key(self.client, fallback=fallback)


WsConnectionResolver = Callable[[WsConnect], "WsConnection | None | Awaitable[WsConnection | None]"]
"""Resolve a connection's identity from the upgrade request (and reauth payloads).

Return ``None`` for anonymous — which this route refuses (replay, ack, and command
dispatch all need a principal) — or raise a client-safe
:class:`~forze.base.exceptions.CoreException` to refuse the connection.
"""


# ----------------------- #


def _render_error(envelope: ErrorEnvelope) -> dict[str, Any]:
    """The shared error envelope on the WS wire — same shape as the Socket.IO ack."""

    payload: dict[str, Any] = {
        "detail": envelope.detail,
        "code": envelope.code,
        "kind": envelope.kind.value,
    }

    if envelope.context is not None:
        payload["context"] = envelope.context

    return payload


def _egress_frame(*, event: str, event_id: str | None, payload: Mapping[str, Any]) -> str:
    """One delivery frame: the `{id, data}` envelope with the event name in-band."""

    return json.dumps(
        {"event": event, "id": event_id, "data": dict(payload)}, separators=(",", ":")
    )


async def _resolve(resolver: WsConnectionResolver, connect: WsConnect) -> WsConnection | None:
    result = resolver(connect)

    return await result if isawaitable(result) else result


def _bind(ctx: ExecutionContext, connection: WsConnection) -> Any:
    """Bind the connection's identity for a unit of work (mailbox, ack, dispatch)."""

    tenant = TenantIdentity(tenant_id=connection.tenant) if connection.tenant is not None else None

    return ctx.inv_ctx.bind_identity(authn=connection.authn, tenant=tenant)


def _log_server_error(core: CoreException | None, error: BaseException) -> None:
    del core  # the raised exception carries the full picture for the log
    _logger.critical_exception("WebSocket realtime unit failed", exc=error)


def _require_allowed_origin(websocket: WebSocket, allowed: frozenset[str] | None) -> None:
    """Refuse a browser upgrade whose ``Origin`` is not on the allowlist.

    The browser is the one client that attaches ambient credentials (cookies) to a
    cross-site WebSocket upgrade and enforces **nothing** itself — no CORS preflight
    guards a WS handshake — so the server-side Origin check is the whole cross-site
    perimeter. A request with no ``Origin`` header is a non-browser client (curl, a
    mobile SDK, a service): it carries no ambient credentials to launder, so it passes —
    the allowlist gates browsers, authentication gates everyone.
    """

    if allowed is None:
        return

    origin = websocket.headers.get("origin")

    if origin is None:
        return

    if origin.strip().rstrip("/").lower() not in allowed:
        raise exc.authorization(
            "Origin not allowed for the realtime WebSocket",
            code="realtime_origin_forbidden",
        )


def _is_socket_teardown_error(error: BaseException) -> bool:
    """Whether a ``RuntimeError`` is starlette's send/receive-after-close race.

    The sender legitimately races the client's disconnect; only that race is normal
    teardown — any other ``RuntimeError`` (hub, presence, dispatch) must propagate.
    """

    detail = str(error)

    return (
        "once a close message has been sent" in detail
        or "once a disconnect message has been received" in detail
    )


# ----------------------- #


@final
@attrs.define(slots=True)
class _WsSession:
    """Mutable per-connection state — reauth swaps the identity in place."""

    connection: WsConnection


# ----------------------- #


def attach_realtime_ws_route(
    router: APIRouter,
    *,
    ctx_dep: ExecutionContextFactory,
    resolve: WsConnectionResolver,
    mailbox_factory: Callable[[ExecutionContext], RealtimeMailbox],
    cursors_factory: Callable[[ExecutionContext], MailboxCursors],
    hub: RealtimeSseHub | None = None,
    presence: RealtimePresence | None = None,
    authorize_topics: TopicAuthorizer | None = None,
    max_topics: int = 32,
    registry: FrozenOperationRegistry | None = None,
    commands: Sequence[RealtimeCommandRoute[Any, Any]] | None = None,
    max_frame_bytes: int = 64 * 1024,
    max_inflight_commands: int = 16,
    allowed_origins: Sequence[str] | None = None,
    path: str = "/realtime/ws",
) -> APIRouter:
    """Attach the governed realtime WebSocket endpoint to *router*.

    Identity is resolved by *resolve* from the upgrade request — the middlewares
    skip websocket scopes, so add the route's **full mounted path** (router prefixes
    included) to their ``allowed_websocket_paths`` — ``check_websocket_allowlist``
    (run by ``runtime_lifespan`` at startup) fails the boot on a mismatch or on a
    non-governed route at an allowlisted path.
    Share *hub* (and its tail lifecycle step) with the SSE route for the live leg;
    without one the socket serves replay + acks only. *registry* + *commands*
    enable ``cmd`` frame dispatch — the same
    :class:`~forze.application.integrations.realtime.RealtimeCommandRoute`
    declarations a Socket.IO namespace router registers.

    *allowed_origins* is the browser perimeter: when set, an upgrade whose ``Origin``
    header is not in the list is refused (policy close) — the WS handshake has no CORS
    preflight, so this check is the only cross-site defense the transport gets. Pass
    your app origins (e.g. ``["https://app.example.com"]``); requests without an
    ``Origin`` header (non-browser clients) pass. ``None`` disables the check — safe
    only when the resolver never honors cookie/ambient credentials.

    Credential expiry is enforced continuously from
    :attr:`WsConnection.expires_at`: the socket is closed once past it (a
    ``realtime.reauth`` swaps in a fresh ``expires_at`` without reconnecting), the
    same contract as the Socket.IO expiry sweep.
    """

    if max_topics <= 0:
        raise exc.configuration("max_topics must be positive")

    origin_allowlist: frozenset[str] | None = None

    if allowed_origins is not None:
        origin_allowlist = frozenset(o.strip().rstrip("/").lower() for o in allowed_origins)

        if not origin_allowlist or "" in origin_allowlist:
            raise exc.configuration(
                "allowed_origins must be a non-empty list of origins "
                "(e.g. ['https://app.example.com']); pass None to disable the check"
            )

    if max_frame_bytes <= 0:
        raise exc.configuration("max_frame_bytes must be positive")

    if max_inflight_commands <= 0:
        raise exc.configuration("max_inflight_commands must be positive")

    if commands and registry is None:
        raise exc.configuration(
            "WebSocket command routes need a registry to dispatch against — pass registry="
        )

    command_table: dict[str, RealtimeCommandRoute[Any, Any]] = {}

    for route in commands or ():
        if route.event in command_table:
            raise exc.configuration(f"WebSocket command event {route.event!r} is declared twice")

        command_table[route.event] = route

    # ....................... #

    async def ws_endpoint(websocket: WebSocket) -> None:
        ctx = ctx_dep()

        async def _handshake() -> tuple[WsConnection, frozenset[str]]:
            _require_allowed_origin(websocket, origin_allowlist)
            negotiate_realtime_protocol(websocket.query_params.get("protocol"))
            connection = await _resolve(resolve, WsConnect(websocket=websocket))

            if connection is None:
                raise exc.authentication(
                    "The realtime WebSocket requires an authenticated principal"
                )

            topic_set = _parse_topics(websocket.query_params.get("topics"))

            if topic_set:
                await _require_topic_grant(
                    ctx,
                    authorize_topics,
                    principal=connection.principal,
                    tenant=connection.tenant,
                    requested=topic_set,
                    max_topics=max_topics,
                )

            return connection, topic_set

        outcome = await guard_frame(_handshake, on_server_error=_log_server_error)

        if isinstance(outcome, FrameErr):
            # accepted-then-closed so the client-safe summary rides the close reason
            # (a pre-accept close is an opaque 403 handshake rejection)
            await websocket.accept()
            await websocket.close(code=WS_POLICY_CLOSE, reason=outcome.envelope.detail[:120])
            return

        connection, topic_set = outcome.value
        session = _WsSession(connection=connection)
        await websocket.accept()

        subscription: SseSubscription | None = (
            hub.subscribe(
                principal=connection.principal, tenant=connection.tenant, topics=topic_set
            )
            if hub is not None
            else None
        )
        member_key = subscription.key if subscription is not None else f"ws:{uuid7()}"
        rooms = (
            presence_rooms(
                principal=connection.principal, tenant=connection.tenant, topics=topic_set
            )
            if presence is not None
            else ()
        )

        def _client_key() -> str:
            # From the LIVE session identity, not a connect-time closure: a reauth may
            # rotate the client's session id, and the ack cursor must follow the
            # refreshed identity (Socket.IO recomputes per ack the same way).
            return session.connection.client_key(member_key)

        # ....................... #

        send_lock = asyncio.Lock()

        async def _send(payload: str) -> None:
            # One serialized writer: the live sender, command acks, and error frames
            # all target this socket concurrently — unserialized ASGI sends can
            # interleave frames or race the close state.
            async with send_lock:
                await websocket.send_text(payload)

        async def _send_json(payload: dict[str, Any]) -> None:
            await _send(json.dumps(payload, separators=(",", ":")))

        async def _send_error(message: str) -> None:
            await _send_json(
                {
                    "type": "error",
                    "error": _render_error(
                        error_envelope(exc.validation(message, code="realtime_invalid_frame"))
                    ),
                }
            )

        async def _sender() -> None:
            if hub is not None:
                await _await_hub_ready(hub)  # a fast-forward-skipped durable is
                # in the mailbox by now — this replay delivers it (same gate as SSE)

            with _bind(ctx, session.connection):
                mailbox = mailbox_factory(ctx)
                cursors = cursors_factory(ctx)
                since = await _resolve_since(
                    mailbox,
                    cursors,
                    principal=session.connection.principal,
                    client_key=_client_key(),
                    last_event_id=websocket.query_params.get("last_event_id"),
                )

                async for entry in iter_replay(
                    mailbox, principal=session.connection.principal, since=since
                ):
                    await _send(
                        _egress_frame(
                            event=entry.event, event_id=entry.event_id, payload=entry.payload
                        )
                    )

            if subscription is None:
                return  # replay + acks only; the socket stays open for the ingress half

            while True:
                signal, event_id = await subscription.queue.get()
                await _send(
                    _egress_frame(event=signal.event, event_id=event_id, payload=signal.payload)
                )

        # ....................... #

        async def _handle_ack(event_id: str) -> None:
            # Contained like reauth and dispatch: a flaky cursor store must cost one
            # ack (surfaced as an error frame), never the whole connection.
            async def _unit() -> None:
                with _bind(ctx, session.connection):
                    await acknowledge_up_to(
                        mailbox_factory(ctx),
                        cursors_factory(ctx),
                        principal=session.connection.principal,
                        client_key=_client_key(),
                        event_id=event_id,
                    )

            outcome = await guard_frame(_unit, on_server_error=_log_server_error)

            if isinstance(outcome, FrameErr):
                await _send_json({"type": "error", "error": _render_error(outcome.envelope)})

        async def _handle_reauth(frame: Mapping[str, Any], cid: Any) -> None:
            async def _unit() -> dict[str, Any]:
                refreshed = await _resolve(
                    resolve, WsConnect(websocket=websocket, auth=frame.get("auth"))
                )

                if (
                    refreshed is None
                    or refreshed.principal != session.connection.principal
                    or refreshed.tenant != session.connection.tenant
                ):
                    raise exc.authentication(
                        "Reauth must resolve the same principal and tenant — anything else "
                        "is a re-login, which reconnects"
                    )

                session.connection = refreshed

                return {"ok": True}

            await _ack_outcome(cid, await guard_frame(_unit, on_server_error=_log_server_error))

        async def _dispatch(frame: Mapping[str, Any], cid: Any) -> None:
            async def _unit() -> Any:
                route = command_table.get(str(frame.get("event")))

                if route is None:
                    raise exc.not_found(
                        f"Unknown realtime command {frame.get('event')!r}",
                        code="realtime_command_unknown",
                    )

                try:
                    args = route.parse_payload(frame.get("payload"))

                except ValidationError as error:
                    raise exc.validation(
                        "Invalid command payload",
                        code="realtime_invalid_payload",
                        details={"errors": sanitize_pydantic_errors(error.errors())},
                    ) from error

                # Governance fields are typed, never coerced: a silently-stringified
                # object key or a dropped string budget would weaken dedup/timeout
                # behavior instead of erroring — the frame is refused loudly.
                idempotency_key = frame.get("idempotency_key")

                if idempotency_key is not None and not isinstance(idempotency_key, str):
                    raise exc.validation(
                        "idempotency_key must be a string", code="realtime_invalid_frame"
                    )

                budget = frame.get("deadline_budget")

                if budget is not None:
                    if isinstance(budget, bool) or not isinstance(budget, (int, float)):
                        raise exc.validation(
                            "deadline_budget must be a number of seconds",
                            code="realtime_invalid_frame",
                        )

                    budget = float(budget)

                    if math.isnan(budget) or budget <= 0 or math.isinf(budget):
                        raise exc.validation(
                            "deadline_budget must be a positive, finite number of seconds",
                            code="realtime_invalid_frame",
                        )

                with (
                    _bind(ctx, session.connection),
                    ctx.inv_ctx.bind_idempotency(idempotency_key or None),
                    # None is a no-op passthrough; a bound budget is tighten-only
                    ctx.inv_ctx.bind_deadline(budget),
                ):
                    result = await run_operation(
                        registry,  # type: ignore[arg-type] # attach refused commands without it
                        route.operation,
                        args,
                        ctx,
                    )

                return route.parse_ack(result)

            await _ack_outcome(cid, await guard_frame(_unit, on_server_error=_log_server_error))

        async def _ack_outcome(cid: Any, outcome: Any) -> None:
            if isinstance(outcome, FrameErr):
                await _send_json(
                    {"type": "ack", "cid": cid, "error": _render_error(outcome.envelope)}
                )
                return

            # Serialize before sending: an ack value that json.dumps cannot encode (a
            # datetime from an untyped parse_ack) raises past guard_frame's protection —
            # unguarded, that TypeError escapes every except* clause and cancels every
            # in-flight command on the socket. It must cost this command an error ack.
            try:
                payload = json.dumps(
                    {"type": "ack", "cid": cid, "data": outcome.value}, separators=(",", ":")
                )

            except (TypeError, ValueError) as error:
                _logger.critical_exception(
                    "WebSocket command ack is not JSON-serializable", exc=error
                )
                await _send_json(
                    {
                        "type": "ack",
                        "cid": cid,
                        "error": _render_error(
                            error_envelope(
                                exc.internal(
                                    "Command ack could not be serialized",
                                    code="realtime_ack_unserializable",
                                )
                            )
                        ),
                    }
                )
                return

            await _send(payload)

        # ....................... #

        receiver_done = asyncio.Event()

        async def _expiry_guard() -> None:
            # The Socket.IO twin sweeps its connection registry on an interval; a
            # FastAPI socket has no registry, so each connection guards itself. The
            # live session identity is re-read every cycle: a reauth that extends (or
            # shortens) expires_at takes effect within one check ceiling. The guard
            # exits on its own once the receiver ends (interruptible wait, not a bare
            # sleep) so the ordinary teardown never has to cancel it — a live sibling
            # at TaskGroup exit races any external cancellation of the endpoint task.
            while True:
                expires_at = session.connection.expires_at
                now = utcnow()

                if expires_at is not None and now >= expires_at:
                    async with send_lock:
                        # Re-read under the lock: a reauth that swapped in a fresh
                        # credential while this task waited for the lock must win —
                        # closing on the stale deadline would disconnect the client
                        # right after its successful reauth ack.
                        refreshed = session.connection.expires_at

                        if refreshed is None or utcnow() < refreshed:
                            continue

                        await websocket.close(code=WS_POLICY_CLOSE, reason="credential expired")

                    raise WebSocketDisconnect(WS_POLICY_CLOSE)

                remaining = (
                    _EXPIRY_CHECK_CEILING_SECONDS
                    if expires_at is None
                    else (expires_at - now).total_seconds()
                )

                try:
                    await asyncio.wait_for(
                        receiver_done.wait(),
                        timeout=max(0.0, min(remaining, _EXPIRY_CHECK_CEILING_SECONDS)),
                    )

                except TimeoutError:
                    continue  # interval elapsed — re-check the live deadline

                return  # the receiver ended: the socket is tearing down anyway

        # ....................... #

        async def _receiver(tasks: asyncio.TaskGroup) -> None:
            inflight = asyncio.Semaphore(max_inflight_commands)

            async def _bounded(coro: Awaitable[None]) -> None:
                try:
                    await coro

                finally:
                    inflight.release()

            while True:
                # Raw receive, not receive_text: a client is free to send a binary
                # frame, and receive_text would surface it as a KeyError that escapes
                # every except* clause — one hostile frame cancelling every in-flight
                # command. Non-text input is refused with a deliberate 1003 close.
                message = await websocket.receive()

                if message["type"] == "websocket.disconnect":
                    raise WebSocketDisconnect(int(message.get("code") or 1000))

                raw = message.get("text")

                if raw is None:
                    async with send_lock:
                        await websocket.close(
                            code=WS_UNSUPPORTED_DATA_CLOSE, reason="text frames only"
                        )

                    raise WebSocketDisconnect(WS_UNSUPPORTED_DATA_CLOSE)

                if len(raw.encode()) > max_frame_bytes:
                    # the close is a socket write too — serialize it with the sender
                    async with send_lock:
                        await websocket.close(code=WS_TOO_BIG_CLOSE, reason="frame too large")

                    raise WebSocketDisconnect(WS_TOO_BIG_CLOSE)

                try:
                    parsed: Any = json.loads(raw)

                except ValueError:
                    parsed = None

                if not isinstance(parsed, Mapping):
                    await _send_error("Frames must be JSON objects")
                    continue

                frame = cast("Mapping[str, Any]", parsed)
                frame_type = frame.get("type")
                cid = frame.get("cid")

                if frame_type == FRAME_ACK:
                    up_to = frame.get("up_to")

                    # a malformed ack must error, not silently leave the cursor put
                    # (the client would believe it acked and re-receive on reconnect)
                    if not isinstance(up_to, str) or not up_to:
                        await _send_error("realtime.ack requires a non-empty string up_to")
                        continue

                    await _handle_ack(up_to)

                elif frame_type == FRAME_REAUTH:
                    await _handle_reauth(frame, cid)

                elif frame_type == FRAME_CMD and command_table:
                    if inflight.locked():
                        # past the in-flight bound: refuse loudly instead of queueing
                        await _ack_outcome(
                            cid,
                            FrameErr(
                                envelope=error_envelope(
                                    exc.validation(
                                        "Too many in-flight commands "
                                        f"(limit {max_inflight_commands})",
                                        code="realtime_commands_limit",
                                    )
                                )
                            ),
                        )
                        continue

                    await inflight.acquire()
                    tasks.create_task(_bounded(_dispatch(frame, cid)))

                else:
                    await _send_error(f"Unknown frame type {frame_type!r}")

        # ....................... #

        try:
            for room in rooms:
                await presence.joined(room, member_key)  # type: ignore[union-attr]

            async def _receive_then_signal() -> None:
                # however the receiver ends (disconnect, refused frame, failure), the
                # signal lets the expiry guard finish on its own instead of being
                # cancelled at TaskGroup exit
                try:
                    await _receiver(tasks)

                finally:
                    receiver_done.set()

            async with asyncio.TaskGroup() as tasks:
                tasks.create_task(_sender())
                tasks.create_task(_receive_then_signal())
                tasks.create_task(_expiry_guard())

        except* WebSocketDisconnect:
            pass  # client went away — normal teardown

        except* RuntimeError as group:
            unrelated = group.split(_is_socket_teardown_error)[1]

            if unrelated is not None:
                # not a chained failure — the surviving subgroup IS the original error
                raise unrelated from None

        finally:
            if subscription is not None:
                hub.unsubscribe(subscription)  # type: ignore[union-attr]

            for room in rooms:
                try:
                    await presence.left(room, member_key)  # type: ignore[union-attr]

                except Exception:
                    _logger.exception("WS presence leave failed", room=room)

    # The governed marker check_websocket_allowlist verifies at startup: an
    # allowlisted path must serve exactly this kind of endpoint, nothing else.
    setattr(ws_endpoint, GOVERNED_WEBSOCKET_ATTR, True)
    router.websocket(path, name="realtime_ws")(ws_endpoint)

    return router
