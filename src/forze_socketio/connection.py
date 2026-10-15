"""Connection lifecycle for the realtime gateway — auto-join + presence.

On **connect**, a connection is auto-joined to its tenant-scoped *principal* room
(derived from the authenticated identity), so a later emit to that principal
reaches it. On **disconnect**, Socket.IO drops the room membership automatically;
the handler here drops the stored identity and updates presence.

Presence is optional and pluggable (:class:`RealtimePresence`): the in-memory
tracker is fine for a single node; a cross-node deployment wants a TTL-backed
store (e.g. Redis) so a crashed node's entries expire rather than leak.
"""

from ._compat import require_socketio

require_socketio()

# ....................... #

import asyncio
from collections.abc import Awaitable, Callable, Mapping
from contextlib import AbstractContextManager, aclosing
from datetime import datetime
from inspect import isawaitable
from typing import (
    Any,
    final,
)
from uuid import UUID

import attrs
from socketio.async_server import AsyncServer
from socketio.exceptions import ConnectionRefusedError as SocketIOConnectionRefusedError

from forze.application.contracts.authn import AuthnIdentity, ClientIdentity
from forze.application.contracts.realtime import Audience
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import ExecutionContext, ExecutionRuntime
from forze.application.integrations.realtime import (
    REALTIME_ACK_EVENT,
    REALTIME_DRAINING_CODE,
    REALTIME_REAUTH_EVENT,
    BacklogDrain,
    acknowledge_up_to,
    iter_backlog,
    negotiate_realtime_protocol,
    refuse_delegated_identity,
    resolve_client_key,
)
from forze.application.integrations.realtime import (
    InMemoryRealtimePresence as InMemoryRealtimePresence,  # re-export: established home
)
from forze.application.integrations.realtime import (
    RealtimePresence as RealtimePresence,  # re-export: established home
)
from forze.base.exceptions import CoreException, exc
from forze.base.primitives import (
    HlcTimestamp,
    current_entropy_source,
    stable_payload_fingerprint,
    utcnow,
)

from .drain import close_transport, require_close_internals
from .exceptions import (
    GENERIC_INTERNAL_DETAIL,
    build_core_exception_ack,
    build_unhandled_exception_ack,
    error_logger,
    is_server_error_kind,
    log_server_error,
)
from .gateway import room_for
from .mailbox import MailboxCursors, RealtimeMailbox
from .observability import ConnectRefusal, RealtimeConnectionStats
from .routing import IDENTITY_SESSION_KEY, SocketIOConnect

# ----------------------- #

CONNECTION_SESSION_KEY = "forze.realtime_connection"
"""Session key holding the :class:`RealtimeConnection` for disconnect presence."""

# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class RealtimeConnection:
    """The resolved identity of a live connection, for room scoping + presence."""

    authn: AuthnIdentity
    """Authenticated principal (also stored for inbound command auth)."""

    tenant: UUID | None = None
    """The connection's tenant, used to scope its rooms."""

    expires_at: datetime | None = None
    """When the connection's credential expires — a **timezone-aware** (UTC) instant;
    ``None`` never expires.

    Captured from the verified assertion/token at connect time (``AuthnIdentity``
    itself is principal-only). A sweeper drops the connection once past this, so a
    long-lived socket can't outlive the credential that authenticated it. A naive
    datetime is refused at construction: the sweep isolates per-connection failures,
    so a naive value would not error the connect — it would make every expiry check
    raise inside the sweep and be logged-and-skipped, silently never enforcing."""

    client: ClientIdentity | None = None
    """The device/session this connection is, keying its offline-replay cursor.

    Resolve it from the connect handshake (a client-supplied ``device_id``) and/or
    the token ``sid``; absent one, the cursor falls back to the per-connection sid."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.expires_at is not None and self.expires_at.tzinfo is None:
            raise exc.configuration(
                "RealtimeConnection.expires_at must be a timezone-aware (UTC) datetime; "
                "a naive value cannot be compared against the aware sweep clock. "
                "Resolve it with tzinfo set (e.g. from the token's exp claim in UTC).",
                code="realtime_expiry_naive",
            )

    # ....................... #

    @property
    def principal_room(self) -> str:
        return room_for(Audience.principal(str(self.authn.principal_id)), self.tenant)

    # ....................... #

    @property
    def principal(self) -> str:
        return str(self.authn.principal_id)

    # ....................... #

    def client_key(self, sid: str) -> str:
        """The stable cursor key: the client's ``device_id``/``session_id``, else *sid*."""

        return resolve_client_key(self.client, fallback=sid)

    # ....................... #

    def is_expired(self, now: datetime) -> bool:
        """Whether the connection's credential has expired as of *now*."""

        return self.expires_at is not None and now >= self.expires_at


# ....................... #

ConnectionResolver = Callable[
    [SocketIOConnect],
    "RealtimeConnection | Awaitable[RealtimeConnection | None] | None",
]
"""Resolve a connection's identity at connect time. Return ``None`` for anonymous
(no principal room joined), or raise a client-safe :class:`CoreException`
(e.g. ``exc.authentication``) to refuse the connection. Return the identity with its
``actor`` chain intact: a delegated identity is refused from it, and one normalized to its
subject would join the subject's room as the subject."""


# ....................... #

ConnectionAdmission = Callable[[SocketIOConnect], Awaitable[None]]
"""Admit a connect before it is authenticated: return to admit it, raise a
:class:`CoreException` to refuse it. Awaiting delays the connect, so an app can pace
admissions by its own rules."""


# ....................... #

# RealtimePresence / InMemoryRealtimePresence live in the transport-neutral kernel
# (forze.application.integrations.realtime), imported above and re-exported here —
# presence is only honest if every transport reports into the same store.

# ----------------------- #


async def _resolve(
    resolver: ConnectionResolver,
    connect: SocketIOConnect,
) -> RealtimeConnection | None:
    result = resolver(connect)
    connection = await result if isawaitable(result) else result

    # Every accepted identity passes here, at connect and at reauth, whatever the resolver.
    refuse_delegated_identity(connection.authn if connection is not None else None)

    return connection


# ....................... #

_SHARED_IDENTITY_KEY = "forze.shared_identity"
"""Engine.IO session key holding the identities one connection resolved, for its namespaces.

Socket.IO keeps each namespace's session in the Engine.IO session under the namespace's own
name, which always starts with ``/``; this key cannot collide with one, and it is freed with
the connection."""

_TURN_AWAY_HOLD_SECONDS = (1.0, 5.0)
"""How long a connect turned away while draining is held before its connection closes, drawn
at random from this range. socket.io's JavaScript client starts its backoff over whenever a
connection opens, so it would come straight back at its base delay; it sets no timeout on a
namespace connect, so holding it is the backoff, and the jitter keeps clients apart."""

_ACCEPTED_KEY = "forze.accepted_namespaces"
"""Engine.IO session key holding the namespaces whose connect this layer accepted on the
connection — not one still connecting — so a turn-away can tell what closing it would end."""

_SharedIdentities = dict[tuple[int, str], "asyncio.Task[RealtimeConnection | None]"]
"""Keyed by the resolver's identity, so any callable can be one, and the credential's
fingerprint."""


def _credential_key(auth: Any) -> str | None:
    """A fingerprint of the ``auth`` payload a namespace connect presents, or ``None`` when one
    cannot be taken (then the connect is authenticated on its own).

    No payload fingerprints too: the credential is then the handshake's cookie, header or
    query parameter, which every namespace of one Engine.IO connection shares."""

    try:
        return stable_payload_fingerprint(auth)

    except (TypeError, ValueError):
        return None


def _reusable(task: "asyncio.Task[RealtimeConnection | None] | None") -> bool:
    """Whether a later connect may wait on or reuse *task*: one in flight, or one that
    succeeded with an identity that has not expired. A failed or cancelled one is a miss,
    whether or not anybody was waiting on it when it failed."""

    if task is None:
        return False

    if not task.done():
        return True

    if task.cancelled() or task.exception() is not None:
        return False

    connection = task.result()

    return connection is None or not connection.is_expired(utcnow())


# ....................... #


def _draining_refusal() -> SocketIOConnectionRefusedError:
    """The refusal a draining replica raises once it has closed the connection: its
    ``data`` names the code, and reaches the client only if that close failed."""

    return SocketIOConnectionRefusedError("Server is draining", {"code": REALTIME_DRAINING_CODE})


def _core_refusal(error: CoreException) -> SocketIOConnectionRefusedError:
    """The client-safe summary, or — for a server-side kind — the generic detail, logged."""

    if is_server_error_kind(error.kind):
        log_server_error(error, core=error)

        return SocketIOConnectionRefusedError(GENERIC_INTERNAL_DETAIL)

    return SocketIOConnectionRefusedError(error.summary)


async def _best_effort(work: Awaitable[object]) -> None:
    try:
        await work

    except Exception as error:
        log_server_error(error)


# ....................... #


def _serving_context(runtime: ExecutionRuntime) -> ExecutionContext:
    """The serving runtime's live context, which replay and ack run on."""

    try:
        return runtime.get_context()

    except CoreException as error:
        raise exc.configuration(
            "Offline replay and acks run on the serving runtime's context, and its scope is "
            "not open: pass attach_realtime_connection the runtime that serves the app, with "
            "its scope held open while the server serves",
            code="realtime_replay_scope_closed",
        ) from error


# ....................... #


def _bind_connection(
    ctx: ExecutionContext,
    connection: RealtimeConnection,
) -> AbstractContextManager[None]:
    """Bind the connection's authenticated identity **and** tenant on the serving context.

    Replay and ack run in a socket.io handler's task, where nothing is bound — so the
    connection's own identity (captured at connect time) must be re-established, or the
    mailbox/cursor operations run unauthenticated (and a tenant-aware store fails closed).
    The binding is the task's own, so connections replaying at once on one context each see
    their own.
    """

    tenant = TenantIdentity(tenant_id=connection.tenant) if connection.tenant is not None else None

    return ctx.inv_ctx.bind_identity(authn=connection.authn, tenant=tenant)


# ....................... #


@final
@attrs.define(slots=True)
class _ReplayProgress:
    """How far a connection's replay has contiguously delivered — the ack clamp's input.

    Socket.IO live emits (room fan-out) race the connect-time replay, so a client can
    receive — and ack — a live frame while older mailbox entries are still draining.
    Without this, that ack's cumulative claim would advance the cursor over the
    undelivered middle and the all-device trim would delete it. Node-local by design:
    the replay runs on the node holding the socket, and ``realtime.ack`` arrives on
    that same node.
    """

    floor: HlcTimestamp | None = None
    """The highest position delivered in order so far (the replay-start cursor until
    the first entry drains); ``None`` when nothing is contiguously delivered yet."""

    complete: bool = False
    """The replay drained fully — every retained entry up to the live tail was sent,
    so cumulative acks need no clamp from here on."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True)
class _ConnectionLifecycle:
    """The connect / ack / disconnect handlers, as methods so each can be unit-tested in
    isolation (the closures they replaced could not be reached from a test)."""

    sio: AsyncServer
    namespace: str
    resolve: ConnectionResolver
    presence: RealtimePresence | None = None
    mailbox_factory: Callable[[ExecutionContext], RealtimeMailbox] | None = None
    cursors_factory: Callable[[ExecutionContext], MailboxCursors] | None = None
    runtime: ExecutionRuntime | None = None
    share_identity: bool = False
    draining: Callable[[], bool] | None = None
    admission: ConnectionAdmission | None = None
    stats: RealtimeConnectionStats | None = None

    _replay_progress: dict[str, _ReplayProgress] = attrs.field(factory=dict, init=False)
    """Per-sid replay progress on this node, created at connect, dropped at disconnect."""

    _counted: set[str] = attrs.field(factory=set, init=False)
    """Sids counted in :attr:`stats`, so a disconnect uncounts only a connect that counted."""

    # ....................... #

    @property
    def replay_enabled(self) -> bool:
        return (
            self.mailbox_factory is not None
            and self.cursors_factory is not None
            and self.runtime is not None
        )

    # ....................... #

    async def replay(self, connection: RealtimeConnection, sid: str) -> None:
        """Drain everything past this device's cursor to the freshly-connected socket."""

        if self.mailbox_factory is None or self.cursors_factory is None or self.runtime is None:
            return

        client_key = connection.client_key(sid)
        progress = self._replay_progress.get(sid)

        # Stream the backlog page-by-page and emit as we go, so peak memory is one page
        # rather than the whole (up to ``cap``) backlog per reconnecting device. The
        # document query ports do not pin a connection between paged reads.
        ctx = _serving_context(self.runtime)

        with _bind_connection(ctx, connection):  # nothing is bound in a handler's task
            mailbox = self.mailbox_factory(ctx)
            cursors = self.cursors_factory(ctx)
            since = await cursors.get(principal=connection.principal, client_key=client_key)

            if progress is not None and since is not None:
                progress.floor = since  # the already-acked prefix counts as delivered

            outcome = BacklogDrain(claim_floor=since)

            # ``iter_backlog`` drains past a bounded replay window in rounds (a cap-filled
            # pass is ambiguous: drained, a live arrival, or a truncated remainder) and
            # reports whether the drain confirmably completed. ``aclosing``: a raise
            # mid-drain (an emit failure, a cancel) must close the paged stream
            # deterministically.
            async with aclosing(
                iter_backlog(mailbox, principal=connection.principal, since=since, outcome=outcome)
            ) as entries:
                async for entry in entries:
                    await self.sio.emit(
                        entry.event,
                        {"id": entry.event_id, "data": entry.payload},
                        to=sid,
                        namespace=self.namespace,
                    )

                    if progress is not None:
                        # The floor an ack may claim mid-drain is the drain's RUN-PROVEN
                        # position, not this entry's own: a claim trims the whole
                        # equal-HLC run, and this entry's run may still have undelivered
                        # siblings in flight — an ack claiming it would delete them from
                        # the store before the replay reaches them.
                        progress.floor = outcome.claim_floor

        if progress is not None:
            # The claim floor lands on the final run only once the drain confirmed
            # it complete; otherwise it stays one proven run behind, and the clamp
            # holds there until the device reconnects and replays from the cursor.
            progress.floor = outcome.claim_floor

            if outcome.complete:
                # Only a confirmed drain — that did not raise — lifts the clamp: a
                # live frame acked past an undelivered middle would advance the
                # cursor over it and the all-device trim would hard-delete it.
                progress.complete = True

    # ....................... #

    async def on_reauth(self, sid: str, data: Any = None) -> dict[str, Any]:
        """``realtime.reauth {token, ...}``: re-verify credentials in place.

        A rotating access token otherwise forces the client to disconnect and reconnect —
        a reconnect storm at scale, and a mailbox replay per device — just to stay
        authenticated past the old token's expiry. This re-runs the **same** connection
        resolver with the fresh auth payload and swaps the stored identity (including a
        new ``expires_at``, so the expiry sweep keeps its hands off).

        **Same principal, same tenant.** Rooms, presence, replay cursors — everything
        keys off the (tenant, principal) pair, so a payload resolving to a different
        principal, a different tenant, or to anonymous is refused: the socket would stay
        in its old tenant-scoped rooms while everything else used the new identity. That
        is a re-login, and a re-login reconnects.
        """

        session = await self.sio.get_session(sid, namespace=self.namespace)
        current: RealtimeConnection | None = session.get(CONNECTION_SESSION_KEY)

        if current is None:
            return build_core_exception_ack(
                exc.authentication("Reauth on an unauthenticated connection — reconnect")
            )

        connect = SocketIOConnect(sid=sid, namespace=self.namespace, environ={}, auth=data)

        try:
            refreshed = await _resolve(self.resolve, connect)

        except CoreException as error:
            # the ack builder logs server-side kinds itself — no second log here
            return build_core_exception_ack(error)

        except Exception as error:
            # the ack builder logs the traceback itself — no second log here
            return build_unhandled_exception_ack(error)

        if (
            refreshed is None
            or refreshed.principal != current.principal
            or refreshed.tenant != current.tenant
        ):
            return build_core_exception_ack(
                exc.authentication(
                    "Reauth must resolve the same principal and tenant — anything else "
                    "is a re-login, which reconnects. Rooms, replay cursors, and presence "
                    "are all scoped by both; swapping identity in place would leave the "
                    "socket in the old identity's rooms."
                )
            )

        # The resolve awaited: the connection may have ended since — a later connect of the
        # namespace on the same transport shares its session — or another reauth refreshed
        # it. The identity is checked and written with no await between.
        if not self.sio.manager.is_connected(sid, self.namespace):
            return build_core_exception_ack(
                exc.authentication("The connection ended during reauth — reconnect")
            )

        session = await self.sio.get_session(sid, namespace=self.namespace)

        if session.get(CONNECTION_SESSION_KEY) is not current:
            return build_core_exception_ack(
                exc.conflict("The connection's identity changed during this reauth; not applied")
            )

        session[IDENTITY_SESSION_KEY] = refreshed.authn
        session[CONNECTION_SESSION_KEY] = refreshed
        await self.sio.save_session(sid, session, namespace=self.namespace)

        return {"ok": True}

    # ....................... #

    async def on_ack(self, sid: str, data: Any = None) -> None:
        """``realtime.ack {up_to}``: advance the device cursor, trim the all-device floor."""

        if self.mailbox_factory is None or self.cursors_factory is None or self.runtime is None:
            return

        session = await self.sio.get_session(sid, namespace=self.namespace)
        connection: RealtimeConnection | None = session.get(CONNECTION_SESSION_KEY)

        raw = (  # pyright: ignore[reportUnknownVariableType]
            data.get("up_to")  # pyright: ignore[reportUnknownMemberType]
            if isinstance(data, Mapping)
            else None
        )
        event_id = (
            str(raw) if raw else None  # pyright: ignore[reportUnknownArgumentType]
        )

        if connection is None or event_id is None:
            return

        # Live room emits race the connect-time replay, so an ack can name a frame
        # delivered ahead of the drain. Clamp it to the replay's contiguous floor —
        # unclamped it would advance the cursor over undelivered entries, and the
        # all-device trim below the new floor would delete them (silent loss).
        progress = self._replay_progress.get(sid)
        delivered_floor: HlcTimestamp | None = None

        if progress is not None and not progress.complete:
            if progress.floor is None:
                return  # nothing contiguously delivered yet — nothing an ack can claim

            delivered_floor = progress.floor

        ctx = _serving_context(self.runtime)

        with _bind_connection(ctx, connection):
            await acknowledge_up_to(
                self.mailbox_factory(ctx),
                self.cursors_factory(ctx),
                principal=connection.principal,
                client_key=connection.client_key(sid),
                event_id=event_id,
                delivered_floor=delivered_floor,
            )

    # ....................... #

    async def _identify(self, connect: SocketIOConnect) -> RealtimeConnection | None:
        """Resolve the connect, or reuse what this Engine.IO connection already resolved.

        With :attr:`share_identity`, the namespaces one Engine.IO connection opens with the
        same credential, through the same resolver, share one resolution: a connect arriving
        while another is resolving waits for it rather than verifying the credential again.
        A failed resolution is not kept, so the next connect tries afresh, and an identity
        that has expired is resolved again.
        """

        if not self.share_identity:
            return await _resolve(self.resolve, connect)

        eio_sid = self.sio.manager.eio_sid_from_sid(connect.sid, self.namespace)
        credential = _credential_key(connect.auth)

        if eio_sid is None or credential is None:
            return await _resolve(self.resolve, connect)

        eio_session = await self.sio.eio.get_session(eio_sid)
        shared: _SharedIdentities = eio_session.setdefault(_SHARED_IDENTITY_KEY, {})
        key = (id(self.resolve), credential)
        task = shared.get(key)

        if task is None or not _reusable(task):
            task = asyncio.ensure_future(_resolve(self.resolve, connect))
            shared[key] = task

        try:
            # Shielded: one namespace's connect being cancelled must not cancel the resolution
            # another namespace is waiting on.
            return await asyncio.shield(task)

        except BaseException:
            if task.done() and shared.get(key) is task:
                shared.pop(key, None)

            raise

    # ....................... #

    # ....................... #

    def _refused(
        self,
        reason: ConnectRefusal,
        refusal: SocketIOConnectionRefusedError,
    ) -> SocketIOConnectionRefusedError:
        if self.stats is not None:
            self.stats.refused(self.namespace, reason)

        return refusal

    # ....................... #

    async def _engine_session(self, sid: str) -> dict[str, Any] | None:
        """The Engine.IO session of *sid*'s connection, or ``None`` once it is gone."""

        eio_sid = self.sio.manager.eio_sid_from_sid(sid, self.namespace)

        if eio_sid is None:
            return None

        try:
            session: dict[str, Any] = await self.sio.eio.get_session(eio_sid)

        except KeyError:
            return None

        return session

    # ....................... #

    async def _accepted(self, sid: str) -> None:
        session = await self._engine_session(sid)

        if session is not None:
            session.setdefault(_ACCEPTED_KEY, set()).add(self.namespace)

        if self.stats is not None:
            self._counted.add(sid)
            self.stats.connected(self.namespace)

    # ....................... #

    async def _beside_an_accepted_namespace(self, sid: str) -> bool:
        """Whether *sid*'s connection already carries another namespace this layer accepted.

        Turning the connect away closes the connection, and with it every namespace it carries
        — including one the drain was told to keep, which is why the drain keeps a connection
        whose every namespace it may not close. Such a connect is admitted instead: it adds no
        connection, and goes when the drain or the process closes this one. A sibling still
        connecting does not count; it is turned away in its turn.
        """

        session = await self._engine_session(sid)
        accepted: set[str] = session.get(_ACCEPTED_KEY, set()) if session is not None else set()

        return any(namespace != self.namespace for namespace in accepted)

    # ....................... #

    async def _goes_on(self, sid: str, connection: RealtimeConnection | None = None) -> bool:
        """Whether the connect may go on; checked before each stage of work and as its last step.

        ``False`` when its transport is gone — closed by the drain or by the client while the
        connect waited: it stops quietly, having been through :meth:`on_disconnect` already,
        and would otherwise fail on the session it no longer has. Raises the refusal to raise
        when the replica is draining, or when the draining source fails; a draining connect is
        held first, and admitted after all if its connection meanwhile carries another
        accepted namespace. What *connection* stored is undone when the connect stops without
        its connection closing, since such a connect never reaches :meth:`on_disconnect`.
        """

        if not self.sio.manager.is_connected(sid, self.namespace):
            if connection is not None:
                await self._undo_connect(sid, connection, gone=True)

            return False

        try:
            draining = self.draining is not None and self.draining()

        except Exception as error:
            log_server_error(error)

            if connection is not None:
                await self._undo_connect(sid, connection)

            raise self._refused(
                "internal", SocketIOConnectionRefusedError(GENERIC_INTERNAL_DETAIL)
            ) from error

        if not draining or await self._beside_an_accepted_namespace(sid):
            return True

        # Held first, in its own task and never the drain's: the client's backoff starts over
        # whenever a connection opens, so the hold is the backoff. Desynchronization jitter,
        # not security randomness.
        await asyncio.sleep(current_entropy_source().as_random().uniform(*_TURN_AWAY_HOLD_SECONDS))

        if not self.sio.manager.is_connected(sid, self.namespace):
            # Closed while it was held — by the drain, or by the client.
            if self.stats is not None:
                self.stats.refused(self.namespace, "draining")

            if connection is not None:
                await self._undo_connect(sid, connection, gone=True)

            return False

        if await self._beside_an_accepted_namespace(sid):
            return True  # a sibling was accepted while it was held: closing would end it too

        raise await self._turn_away(sid, connection)

    # ....................... #

    async def _staged[T](self, stage: ConnectRefusal, work: Callable[[], Awaitable[T]]) -> T:
        """Run one stage of the connect, refusing under *stage* if it fails."""

        try:
            return await work()

        except SocketIOConnectionRefusedError:
            if self.stats is not None:
                self.stats.refused(self.namespace, stage)

            raise

        except CoreException as error:
            raise self._refused(stage, _core_refusal(error)) from error

        except Exception as error:
            log_server_error(error)
            raise self._refused(
                "internal", SocketIOConnectionRefusedError(GENERIC_INTERNAL_DETAIL)
            ) from error

    # ....................... #

    async def _turn_away(
        self, sid: str, connection: RealtimeConnection | None = None
    ) -> SocketIOConnectionRefusedError:
        """Turn a held connect away while the replica drains; return the refusal to raise.

        The Engine.IO connection is closed, as the drain closes its connections: socket.io's
        JavaScript client then reconnects with its backoff, through the balancer, to a
        replica still ready. A refused connect (CONNECT_ERROR) alone would make it give up
        for good. Only a connect that would be its connection's first accepted namespace
        gets here (see :meth:`_beside_an_accepted_namespace`). The close runs the connect's
        own disconnect handlers, which undo what it stored; only if the close fails is the
        connect undone here, and the refusal then reaches the client.
        """

        refusal = self._refused("draining", _draining_refusal())
        eio_sid = self.sio.manager.eio_sid_from_sid(sid, self.namespace)

        try:
            if eio_sid is not None:
                await close_transport(self.sio, eio_sid)

        except Exception as error:
            log_server_error(error)

            if connection is not None:
                await self._undo_connect(sid, connection)

        return refusal

    # ....................... #

    async def _undo_connect(
        self, sid: str, connection: RealtimeConnection, *, gone: bool = False
    ) -> None:
        """Undo what an accepted connect did, before it is refused after all.

        A refused connect never reaches :meth:`on_disconnect`, so without this a refused
        client would stay online in shared presence until its entry expired, and its
        identity would stay in the namespace's session. Each step is best-effort: a failure
        is logged and must not replace the refusal. When the connect's transport is *gone*,
        its rooms and session went with its sid, and only presence and the replay progress
        are left to undo — presence because the connect may have joined after its
        :meth:`on_disconnect` ran.
        """

        self._replay_progress.pop(sid, None)
        room = connection.principal_room

        if self.presence is not None:
            await _best_effort(self.presence.left(room, sid))

        if gone:
            return

        await _best_effort(self.sio.leave_room(sid, room, namespace=self.namespace))
        await _best_effort(self._drop_identity(sid))

    # ....................... #

    async def _drop_identity(self, sid: str) -> RealtimeConnection | None:
        """Remove the identity stored in *sid*'s namespace session; return the connection.

        python-socketio keeps a namespace's session inside its Engine.IO connection's session,
        so it outlives the namespace's disconnect: a client may connect the namespace again
        over the same connection, and that connect must not find this one's identity.
        """

        try:
            session = await self.sio.get_session(sid, namespace=self.namespace)

        except KeyError:
            return None  # the Engine.IO connection is gone, and its sessions with it

        session.pop(IDENTITY_SESSION_KEY, None)
        connection: RealtimeConnection | None = session.pop(CONNECTION_SESSION_KEY, None)
        await self.sio.save_session(sid, session, namespace=self.namespace)

        return connection

    # ....................... #

    async def on_connect(self, sid: str, environ: Mapping[str, Any], auth: Any = None) -> None:
        """Admit, negotiate the protocol, authenticate, auto-join the principal room, replay.

        While the replica is draining the connect is turned away — before any of that work,
        again after admission and after authentication, and as the last step, for a connect
        that was already under way.
        """

        if not await self._goes_on(sid):
            return

        await self._drop_identity(sid)  # an earlier connect's, should one be left

        connect = SocketIOConnect(sid=sid, namespace=self.namespace, environ=environ, auth=auth)

        if self.admission is not None:
            admission = self.admission
            await self._staged("admission", lambda: admission(connect))

            if not await self._goes_on(sid):
                return

        async def negotiate() -> None:
            # Handshake first: the connection speaks exactly one protocol version for its
            # lifetime (missing = 1); an unsupported one is refused before any auth work.
            negotiate_realtime_protocol(auth.get("protocol") if isinstance(auth, Mapping) else None)  # pyright: ignore[reportUnknownMemberType, reportUnknownArgumentType]

        await self._staged("protocol", negotiate)
        connection = await self._staged("auth", lambda: self._identify(connect))

        if not await self._goes_on(sid):
            return

        if connection is None:
            await self._accepted(sid)
            return  # anonymous: no principal room, may still join topics later

        session = await self.sio.get_session(sid, namespace=self.namespace)
        session[IDENTITY_SESSION_KEY] = connection.authn
        session[CONNECTION_SESSION_KEY] = connection
        await self.sio.save_session(sid, session, namespace=self.namespace)

        if self.replay_enabled:
            # Registered before the room join: from the first live emit onward, acks
            # must be clamped to what the (not yet started) replay has delivered.
            self._replay_progress[sid] = _ReplayProgress()

        async def join() -> None:
            room = connection.principal_room
            await self.sio.enter_room(sid, room, namespace=self.namespace)

            if self.presence is not None:
                await self.presence.joined(room, sid)

        try:
            await self._staged("internal", join)

            if self.replay_enabled:
                # replay is best-effort: a drain error must not refuse the live connection
                try:
                    await self.replay(connection, sid)

                except Exception as error:
                    log_server_error(error)

        except BaseException:
            # A raise here refuses the connect, and a refused connect never reaches
            # on_disconnect: undo its room, presence, replay progress and stored identity.
            await self._undo_connect(sid, connection)
            raise

        # Last: a connect that waited in replay while the drain listed its connections may
        # have missed the drain, and its transport may have closed meanwhile.
        if await self._goes_on(sid, connection):
            await self._accepted(sid)

    # ....................... #

    async def on_disconnect(self, sid: str) -> None:
        """Drop the stored identity and update presence (Socket.IO drops room membership)."""

        self._replay_progress.pop(sid, None)

        if self.stats is not None and sid in self._counted:
            self._counted.discard(sid)
            self.stats.disconnected(self.namespace)

        engine_session = await self._engine_session(sid)

        if engine_session is not None:
            engine_session.get(_ACCEPTED_KEY, set()).discard(self.namespace)

        connection = await self._drop_identity(sid)

        if self.presence is not None and connection is not None:
            await self.presence.left(connection.principal_room, sid)


# ....................... #


def attach_realtime_connection(
    sio: AsyncServer,
    *,
    namespace: str = "/",
    resolve: ConnectionResolver,
    presence: RealtimePresence | None = None,
    mailbox_factory: Callable[[ExecutionContext], RealtimeMailbox] | None = None,
    cursors_factory: Callable[[ExecutionContext], MailboxCursors] | None = None,
    runtime: ExecutionRuntime | None = None,
    share_identity: bool = False,
    draining: Callable[[], bool] | None = None,
    admission: ConnectionAdmission | None = None,
    stats: RealtimeConnectionStats | None = None,
) -> None:
    """Register connect/disconnect handlers that auto-join the principal room.

    The resolved identity is stored under both the inbound identity key (so
    command handlers see it) and a gateway key (for disconnect presence), until the
    namespace connection ends. A
    client-safe :class:`CoreException` from *resolve* refuses the connection;
    a server-side one is logged and refused generically.

    When *mailbox_factory*, *cursors_factory* and *runtime* are all supplied, offline
    replay is enabled: on connect the connection's device is replayed
    everything past its cursor, and a ``realtime.ack {up_to}`` event advances that cursor
    so a device never re-receives what it acked. *runtime* is the **serving** runtime:
    replay and ack run on its live context, under the connection's identity and tenant, so
    its scope must be open while the server serves (as the app's lifespan holds it). With
    it closed, each connect's replay logs a configuration error coded
    ``realtime_replay_scope_closed`` and delivers nothing, so that connection's acks are
    ignored: there is nothing for them to claim. The factories build their store
    with ports resolved against that ctx (e.g. ``build_realtime_mailbox``); the device is
    keyed by :meth:`RealtimeConnection.client_key` (its ``device_id``/``session_id``, else
    the per-connection sid).

    A ``realtime.reauth`` event is always registered: a client whose token rotates sends
    the fresh auth payload and the **same** resolver re-verifies it in place — same
    principal only — swapping the stored identity and ``expires_at`` without a reconnect
    (and without the replay a reconnect costs). Set ``expires_at`` when resolving so the
    expiry sweep has something to enforce between reauths.

    *share_identity* (opt-in) authenticates once per Engine.IO connection rather than once
    per namespace: a client opening several namespaces with the same credential has it
    resolved once, by the first, and every later namespace attached with
    ``share_identity=True`` and the same resolver reuses that identity until it expires or
    the connection closes. A different credential is resolved again, and the resolver must
    not depend on the namespace it is asked for. The cost is when a revocation is noticed:
    a namespace connecting just after the first no longer re-checks the session. Nothing
    else changes; ``realtime.reauth`` still resolves the fresh credential.

    While *draining* returns true a connect is turned away by closing its Engine.IO
    connection — as the drain closes connections — so socket.io's JavaScript client
    reconnects with its backoff, through the balancer, to another replica; a refused connect
    alone would make it give up for good. The connect is held for a random 1–5 s first: the
    client's backoff starts over whenever a connection opens, so the hold is the backoff. A connect on a connection that already carries
    another namespace this layer accepted is admitted instead, since closing the connection
    would end that one too. It is checked before any other work, again after
    admission and after authentication, and as the connect's last step; after the hold it is
    admitted after all if its connection meanwhile carries another accepted namespace, and
    a connect turned away late is undone by its own disconnect handlers. A connect whose
    transport closed meanwhile stops quietly at the same checks, and a *draining* that raises
    refuses the connect as an internal error. The refusal ``Server is draining``, data
    ``{"code": "realtime_draining"}``, is raised too and reaches the client only if the close
    failed. Pass the serving runtime's, ``draining=lambda: runtime.draining``: nothing
    defaults it, not even when replay is wired with that runtime.

    *admission* runs after the first draining check and before the protocol is negotiated or
    the connection authenticated: it returns to admit the connect, and awaiting it delays the
    connect, so an app can pace admissions by its own rules. A :class:`CoreException` it
    raises refuses the connect like a resolver's: its client-safe summary, or for a
    server-side kind the generic detail, logged.

    *stats* counts this namespace's open connections and its refused connects by reason
    (``protocol``, ``auth``, ``draining``, ``admission``, ``internal``); export it with
    :func:`~forze_socketio.instrument_realtime_connections`.

    .. important::
        Socket.IO keeps **one** ``connect`` handler per namespace, so this is the
        *single* connect path: it both authenticates (stores the ``AuthnIdentity``)
        and auto-joins. Do **not** also pass an ``identity_resolver`` to
        :class:`ForzeSocketIOAdapter` for the same namespace — its connect handler
        would silently overwrite this one (or vice versa). Resolve identity here.
    """

    # Offline replay needs all three of mailbox_factory / cursors_factory / runtime. Partial
    # wiring would silently disable replay (broken offline delivery, no warning) — fail closed.
    replay_parts = (mailbox_factory, cursors_factory, runtime)

    if sum(p is not None for p in replay_parts) not in (0, len(replay_parts)):
        raise exc.configuration(
            "Offline replay needs all of mailbox_factory, cursors_factory, and runtime "
            "(or none) — partial wiring would silently disable replay"
        )

    if draining is not None:
        # A turn-away closes connections the way the drain does.
        require_close_internals(sio)

    if runtime is not None and draining is None:
        error_logger.warning(
            "attach_realtime_connection on %s: replay is wired without draining=, so connects "
            "are not refused while the replica drains — pass draining=lambda: runtime.draining",
            namespace,
        )

    lifecycle = _ConnectionLifecycle(
        sio=sio,
        namespace=namespace,
        resolve=resolve,
        presence=presence,
        mailbox_factory=mailbox_factory,
        cursors_factory=cursors_factory,
        runtime=runtime,
        share_identity=share_identity,
        draining=draining,
        admission=admission,
        stats=stats,
    )

    sio.on("connect", handler=lifecycle.on_connect, namespace=namespace)
    sio.on("disconnect", handler=lifecycle.on_disconnect, namespace=namespace)
    sio.on(REALTIME_REAUTH_EVENT, handler=lifecycle.on_reauth, namespace=namespace)

    if lifecycle.replay_enabled:
        sio.on(REALTIME_ACK_EVENT, handler=lifecycle.on_ack, namespace=namespace)


# ----------------------- #


def _local_connections(sio: AsyncServer, namespace: str) -> list[str]:
    """The sids connected to *namespace* on this node (room ``None`` = all)."""

    return [sid for sid, _eio in sio.manager.get_participants(namespace, None)]


# ....................... #


async def sweep_expired_connections(
    sio: AsyncServer,
    *,
    namespace: str = "/",
    now: datetime | None = None,
) -> int:
    """Disconnect connections whose credential has expired; return how many.

    Identity is bound once at connect, so without this a socket outlives the
    credential that authenticated it. Run it periodically (see
    :func:`~forze_socketio.realtime_identity_expiry_lifecycle_step`). Only this
    node's connections are visible — each node sweeps its own.
    """

    moment = now if now is not None else utcnow()
    dropped = 0

    for sid in _local_connections(sio, namespace):
        # Per-connection isolation: one socket that fails to read or disconnect (already
        # gone, transport hiccup) must not shield every later socket from the sweep —
        # that would extend expired credentials past their lifetime for as long as the
        # early failure persists. CancelledError is a BaseException and still propagates.
        try:
            session = await sio.get_session(sid, namespace=namespace)
            connection: RealtimeConnection | None = session.get(CONNECTION_SESSION_KEY)

            if connection is not None and connection.is_expired(moment):
                await sio.disconnect(sid, namespace=namespace)
                dropped += 1

        except Exception as error:
            log_server_error(error)

    return dropped


# ....................... #


async def refresh_presence(
    sio: AsyncServer, presence: RealtimePresence, *, namespace: str = "/"
) -> int:
    """Re-assert presence for every connection this node holds; return how many.

    A TTL-backed presence store (e.g. Redis) expires entries so a crashed node's
    rows don't leak — which means live connections must re-assert (heartbeat) or
    they'd wrongly expire too. Run this on an interval shorter than the store's TTL
    (see :func:`~forze_socketio.realtime_presence_heartbeat_lifecycle_step`).
    """

    refreshed = 0

    for sid in _local_connections(sio, namespace):
        # Per-connection isolation: one failed heartbeat must not starve every later
        # connection's — under a TTL store, a persistently-failing early entry would
        # otherwise expire healthy connections and the gateway would skip their live
        # deliveries. CancelledError is a BaseException and still propagates.
        try:
            session = await sio.get_session(sid, namespace=namespace)
            connection: RealtimeConnection | None = session.get(CONNECTION_SESSION_KEY)

            if connection is not None:
                await presence.joined(connection.principal_room, sid)
                refreshed += 1

        except Exception as error:
            log_server_error(error)

    return refreshed
