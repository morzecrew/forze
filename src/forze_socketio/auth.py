"""The shipped connection resolver for Socket.IO — the WebSocket ladder, other transport.

The credential ladder lives in :mod:`forze.application.integrations.realtime.auth`,
so a deployment that serves both transports authenticates by one set of rules and
one set of knobs. Only the handshake shape differs: python-socketio hands the
connect handler a WSGI-style ``environ`` where the raw-WebSocket route has a
starlette socket.
"""

from collections.abc import Mapping
from http.cookies import CookieError, SimpleCookie
from urllib.parse import parse_qsl

from forze.application.contracts.authn import AuthnSpec
from forze.application.execution.context import ExecutionContextFactory
from forze.application.integrations.realtime.auth import (
    RealtimeCredentialSources,
    RealtimeHandshake,
    require_origin_attestation,
    resolve_realtime_identity,
)

from .connection import ConnectionResolver, RealtimeConnection
from .routing import SocketIOConnect

# ----------------------- #

__all__ = ["build_socketio_connection_resolver", "socketio_handshake"]

_HEADER_PREFIX = "HTTP_"
"""WSGI's spelling of a request header — ``HTTP_X_FOO`` is ``X-Foo``."""


def _cookies(raw: str | None) -> dict[str, str]:
    """The ``Cookie`` header as a mapping; an unparseable header yields nothing.

    A malformed cookie header is client input, not a server fault: it must read as
    "no cookie presented" so the ladder moves on, never as a 500 out of the connect
    handler.
    """

    if not raw:
        return {}

    jar = SimpleCookie()

    try:
        jar.load(raw)
    except CookieError:
        return {}

    return {name: morsel.value for name, morsel in jar.items()}


# ....................... #


def socketio_handshake(connect: SocketIOConnect) -> RealtimeHandshake:
    """The connect metadata as the ladder reads it.

    A ``realtime.reauth`` arrives with an empty ``environ`` and the fresh payload, so
    the payload rung is the one that answers it — the same shape as the raw-WebSocket
    route's reauth.
    """

    environ = connect.environ
    headers = {
        key[len(_HEADER_PREFIX) :].replace("_", "-"): value
        for key, value in environ.items()
        if key.startswith(_HEADER_PREFIX) and isinstance(value, str)
    }
    query_string = environ.get("QUERY_STRING")
    auth = connect.auth if isinstance(connect.auth, Mapping) else None

    return RealtimeHandshake(
        cookies=_cookies(headers.get("COOKIE")),
        headers=headers,
        query=dict(parse_qsl(query_string)) if isinstance(query_string, str) else {},
        auth=auth,
    )


# ....................... #


def build_socketio_connection_resolver(
    *,
    ctx_dep: ExecutionContextFactory,
    authn_spec: AuthnSpec,
    cookie_name: str | None = None,
    header_name: str | None = "Authorization",
    query_param: str | None = None,
    origin_allowlist_attested: bool = False,
    tenancy_route: str | None = None,
    device_id_key: str = "device_id",
    session_id_key: str = "session_id",
) -> ConnectionResolver:
    """Build the connection resolver for :func:`attach_realtime_connection`.

    Same ladder, same order, same refusals as the raw-WebSocket resolver: the connect
    or ``realtime.reauth`` payload first, then the cookie, then ``Authorization:
    Bearer``, then the query parameter; the first source that presents a credential is
    the one used, and an invalid one refuses the connection instead of falling through.

    **Cookie mode requires an origin allowlist**, here the server's own
    ``cors_allowed_origins`` — a browser attaches cookies to a cross-site Socket.IO
    handshake by itself. This factory cannot see the ``AsyncServer``, so enabling
    *cookie_name* requires ``origin_allowlist_attested=True``.

    :param ctx_dep: The execution-context factory the namespace is wired with.
    :param authn_spec: The authn route to verify credentials through.
    :param cookie_name: Cookie carrying the access token; ``None`` disables it.
    :param header_name: Header carrying ``Bearer <token>``; ``None`` disables it.
    :param query_param: Query parameter carrying a bare token; ``None`` (the default)
        disables it — query strings land in access and proxy logs.
    :param origin_allowlist_attested: Your attestation that the server restricts
        ``cors_allowed_origins``; required for cookie mode.
    :param tenancy_route: Tenancy route whose resolver binds the principal's tenant;
        defaults to the authn spec's own name, which is how the shipped tenancy module
        registers it and how the HTTP boundary looks it up. A route-less lookup finds
        nothing there, and the connection would silently bind no tenant.
    :param device_id_key: Query / payload key carrying the client's device id.
    :param session_id_key: Query / payload key carrying the client's session id.
    :returns: A resolver to pass as ``attach_realtime_connection(resolve=...)``.
    :raises CoreException: (configuration) when cookie mode is built without the
        origin-allowlist attestation.
    """

    require_origin_attestation(
        cookie_name=cookie_name,
        attested=origin_allowlist_attested,
        perimeter="the server's cors_allowed_origins",
        alternative="authenticate from the Authorization header instead",
    )

    # `TenancyDepsModule` registers the resolver **routed**, so a route-less lookup
    # finds nothing and the connection binds no tenant on a credential that
    # authenticated perfectly. A credential's tenancy belongs to the profile it
    # authenticated against, which is what the HTTP boundary uses too; a routed lookup
    # falls back to a plain registration, so this is right for both wirings.
    route = tenancy_route or str(authn_spec.name)

    sources = RealtimeCredentialSources(
        cookie_name=cookie_name,
        header_name=header_name,
        query_param=query_param,
    )

    async def resolve(connect: SocketIOConnect) -> RealtimeConnection | None:
        ctx = ctx_dep()

        identity = await resolve_realtime_identity(
            authn=ctx.authn.authn(authn_spec),
            sources=sources,
            handshake=socketio_handshake(connect),
            tenants=ctx.tenancy.resolver(route),
            device_id_key=device_id_key,
            session_id_key=session_id_key,
        )

        if identity is None:
            return None

        return RealtimeConnection(
            authn=identity.authn,
            tenant=identity.tenant,
            client=identity.client,
            expires_at=identity.expires_at,
        )

    return resolve
