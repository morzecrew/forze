"""The shipped :data:`WsConnectionResolver` — the ~30 lines every adopter writes once.

``attach_realtime_ws_route`` takes identity from an app-supplied resolver, and the
subtle parts of writing one are the parts a quick version skips: the credential's
expiry (aware, or the route refuses it), the reauth payload, and the cookie/Origin
interaction. This builds the resolver instead, over the transport-neutral ladder in
:mod:`forze.application.integrations.realtime.auth`, so the Socket.IO gateway
authenticates by the same rules.
"""

from collections.abc import Mapping
from typing import Any

from starlette.websockets import WebSocket

from forze.application.contracts.authn import AuthnSpec
from forze.application.execution.context import ExecutionContextFactory
from forze.application.integrations.realtime.auth import (
    RealtimeCredentialSources,
    RealtimeHandshake,
    require_origin_attestation,
    resolve_realtime_identity,
)

from .ws import WsConnect, WsConnection, WsConnectionResolver

# ----------------------- #

__all__ = ["build_ws_connection_resolver", "ws_handshake"]


def ws_handshake(websocket: WebSocket, auth: Mapping[str, Any] | None) -> RealtimeHandshake:
    """The upgrade request as the ladder reads it — cookies, headers, query, payload."""

    return RealtimeHandshake(
        cookies=websocket.cookies,
        headers=dict(websocket.headers),
        query=dict(websocket.query_params),
        auth=auth,
    )


# ....................... #


def build_ws_connection_resolver(
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
) -> WsConnectionResolver:
    """Build the connection resolver for :func:`attach_realtime_ws_route`.

    The credential ladder is fixed: a ``realtime.reauth`` payload first, then the
    cookie, then ``Authorization: Bearer``, then the query parameter — each source
    enabled by naming it. The first source that *presents* a credential is the one
    used; an invalid one refuses the connection rather than falling through to the
    next. The verified credential fills the whole :class:`WsConnection`: principal,
    tenant, client identity, and the ``expires_at`` the route enforces continuously.

    **Cookie mode requires the route's Origin allowlist.** A browser attaches cookies
    to a cross-site WebSocket upgrade on its own and the handshake has no CORS
    preflight, so ``allowed_origins`` on the attach call is the entire cross-site
    perimeter. This factory cannot see that argument — they meet only at
    ``attach_realtime_ws_route`` — so enabling *cookie_name* requires
    ``origin_allowlist_attested=True``, your statement that the paired allowlist is
    there.

    :param ctx_dep: The same execution-context factory the route is attached with.
    :param authn_spec: The authn route to verify credentials through.
    :param cookie_name: Cookie carrying the access token (e.g. ``"forze_access"``,
        the framework carrier's default); ``None`` disables the cookie source.
    :param header_name: Header carrying ``Bearer <token>``; ``None`` disables it.
    :param query_param: Query parameter carrying a bare token — ``None`` (the default)
        disables it. Query strings land in access and proxy logs: enable it only for
        clients that can set neither cookie nor header, with short-lived tokens.
    :param origin_allowlist_attested: Your attestation that the route is attached with
        ``allowed_origins``; required for cookie mode.
    :param tenancy_route: Tenancy route whose resolver binds the principal's tenant.
    :param device_id_key: Query / payload key carrying the client's device id.
    :param session_id_key: Query / payload key carrying the client's session id.
    :returns: A resolver to pass as ``attach_realtime_ws_route(resolve=...)``.
    :raises CoreException: (configuration) when cookie mode is built without the
        Origin-allowlist attestation.
    """

    require_origin_attestation(
        cookie_name=cookie_name,
        attested=origin_allowlist_attested,
        perimeter="attach_realtime_ws_route(allowed_origins=[...])",
        alternative="authenticate from the Authorization header instead",
    )

    sources = RealtimeCredentialSources(
        cookie_name=cookie_name,
        header_name=header_name,
        query_param=query_param,
    )

    async def resolve(connect: WsConnect) -> WsConnection | None:
        ctx = ctx_dep()

        identity = await resolve_realtime_identity(
            authn=ctx.authn.authn(authn_spec),
            sources=sources,
            handshake=ws_handshake(connect.websocket, connect.auth),
            tenants=ctx.tenancy.resolver(tenancy_route),
            device_id_key=device_id_key,
            session_id_key=session_id_key,
        )

        if identity is None:
            return None

        return WsConnection(
            authn=identity.authn,
            tenant=identity.tenant,
            client=identity.client,
            expires_at=identity.expires_at,
        )

    return resolve
