"""Socket.IO transport integration for Forze.

Command events dispatched through :class:`ForzeSocketIOAdapter` run inside an
error boundary: a :class:`~forze.base.exceptions.CoreException` raised during
dispatch is acknowledged with a structured error payload::

    {
        "error": {
            "detail": "<client-safe summary>",
            "code": "<exception code>",
            "kind": "<exception kind>",
            "context": {...}  # only when the egress policy exposes details
        }
    }

Server-side kinds (internal, infrastructure, configuration, concurrency) are
logged and acked with a generic detail; unhandled exceptions are logged at
CRITICAL level and acked with the generic internal-error payload. The payload
is the handler's return value, which python-socketio delivers as the event
acknowledgement.

Server-side details never leak: only the exception ``code``, ``kind``, and a
client-safe ``detail`` cross the wire, mirroring the FastAPI error boundary.

An optional ``identity_resolver`` authenticates connections at connect time and
binds the resolved :class:`~forze.application.contracts.authn.AuthnIdentity`
onto the invocation context around each event dispatch. Tenant resolution
remains the ``context_factory``'s responsibility::

    from forze_socketio import (
        ForzeSocketIOAdapter,
        SocketIOConnect,
        SocketIONamespaceRouter,
        build_socketio_server,
    )

    async def resolve_identity(connect: SocketIOConnect) -> AuthnIdentity | None:
        token = (connect.auth or {}).get("token")
        if token is None:
            raise exc.authentication("Missing token")  # connection refused
        return await verify_token(token)  # or None for anonymous

    sio = build_socketio_server()
    adapter = ForzeSocketIOAdapter(
        sio=sio,
        context_factory=build_context,        # tenant/deps wiring stays here
        operation_resolver=registry.resolve,  # frozen OperationRegistry
        identity_resolver=resolve_identity,   # optional connect-time authn
    )
    adapter.include_router(
        SocketIONamespaceRouter(namespace="/chat").command(
            event="message.send",
            operation="messages.create",
            payload_type=SendMessage,
            ack_type=ReadMessage,
        )
    )

A resolver raising a client-safe :class:`~forze.base.exceptions.CoreException`
(for example ``exc.authentication``) refuses the connection via python-socketio's
``ConnectionRefusedError`` with the exception summary; unexpected resolver
failures are logged and refused with a generic message.

Without an ``identity_resolver`` behavior is unchanged from earlier releases:
no connect handler is registered and no identity is bound, so handlers run
unauthenticated — any governance hook that treats identity as required will
deny those operations. Bind identity yourself (or supply a resolver) if your
operations need an authenticated principal.
"""

from ._compat import require_socketio

require_socketio()

# ....................... #

from .emitter import (
    SocketIOEventEmitter,
    SocketIONamespaceEmitter,
    SocketIOServerEvent,
)
from .gateway import (
    RealtimeGateway,
    RealtimeSignalSource,
    StreamGroupSignalSource,
    room_for,
)
from .gateway_lifecycle import realtime_gateway_lifecycle_step
from .exceptions import (
    GENERIC_INTERNAL_DETAIL,
    build_core_exception_ack,
    build_unhandled_exception_ack,
)
from .routing import (
    IDENTITY_SESSION_KEY,
    ExecutionContextFactoryPort,
    ForzeSocketIOAdapter,
    HandlerResolverPort,
    IdentityResolverPort,
    SocketIOCommandRoute,
    SocketIOConnect,
    SocketIONamespaceRouter,
    SocketIORequest,
)
from .server import build_socketio_asgi_app, build_socketio_server

# ----------------------- #

__all__ = [
    "GENERIC_INTERNAL_DETAIL",
    "IDENTITY_SESSION_KEY",
    "ExecutionContextFactoryPort",
    "HandlerResolverPort",
    "IdentityResolverPort",
    "SocketIOConnect",
    "SocketIORequest",
    "SocketIOCommandRoute",
    "SocketIONamespaceRouter",
    "ForzeSocketIOAdapter",
    "SocketIOServerEvent",
    "SocketIOEventEmitter",
    "SocketIONamespaceEmitter",
    "RealtimeGateway",
    "RealtimeSignalSource",
    "StreamGroupSignalSource",
    "realtime_gateway_lifecycle_step",
    "room_for",
    "build_core_exception_ack",
    "build_socketio_server",
    "build_socketio_asgi_app",
    "build_unhandled_exception_ack",
]
