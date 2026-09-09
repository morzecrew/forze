"""The realtime credential ladder — transport-neutral, one implementation for every transport.

A realtime connection authenticates once, from the handshake, and then lives for
hours. Every transport therefore needs the same four things and gets them the same
way: pick a credential from the handshake, verify it through the wired authn plane,
carry the credential's own expiry so the connection cannot outlive it, and name the
device the connection is.

Only the *shape* of a handshake differs — a starlette WebSocket, a WSGI environ, a
client-supplied payload — so the ladder reads a :class:`RealtimeHandshake` of plain
mappings and each transport's factory fills one. What it returns is likewise
transport-neutral: the connection value objects belong to their transports.
"""

from collections.abc import Mapping
from datetime import datetime
from typing import Any, Final, Literal, final
from uuid import UUID

import attrs

from forze.application.contracts.authn import (
    AccessTokenCredentials,
    AuthnIdentity,
    AuthnPort,
    AuthnResult,
    ClientIdentity,
)
from forze.application.contracts.tenancy import TenantResolverPort, parse_tenant_hint
from forze.base.exceptions import exc

# ----------------------- #

__all__ = [
    "REALTIME_AUTH_TOKEN_KEY",
    "RealtimeCredentialSources",
    "RealtimeHandshake",
    "RealtimeIdentity",
    "PresentedCredential",
    "client_identity",
    "present_credential",
    "resolve_realtime_identity",
]

REALTIME_AUTH_TOKEN_KEY: Final[str] = "token"
"""The key carrying a token in a connect / ``realtime.reauth`` auth payload."""

CredentialSource = Literal["payload", "cookie", "header", "query"]
"""Where a presented credential came from — carried into the refusal, so a client
that presented a revoked cookie is not told to check its ``Authorization`` header."""


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class RealtimeCredentialSources:
    """Which handshake sources carry a credential, and under which names.

    The order is fixed and not configurable — an auth payload (the ``realtime.reauth``
    frame, and the Socket.IO connect handshake) first, then the cookie, then the
    ``Authorization`` header, then the query parameter. Each source is enabled by
    naming it and disabled by ``None``.

    **The query parameter is off by default.** Query strings land in access logs,
    proxy logs and anything that reads a URL, so enabling it is a decision: pair it
    with short-lived tokens and check what your ingress logs.
    """

    cookie_name: str | None = None
    """Cookie carrying the access token — ``"forze_access"`` matches the framework's
    own carrier. ``None`` disables the cookie source; enabling it is what makes the
    connection's Origin allowlist load-bearing (a browser attaches this cookie to a
    cross-site upgrade by itself, and the WS handshake has no CORS preflight)."""

    header_name: str | None = "Authorization"
    """Header carrying ``Bearer <token>`` — the non-browser path (services, mobile,
    CLIs, anything that can set a header on the upgrade). ``None`` disables it."""

    query_param: str | None = None
    """Query parameter carrying a bare token, e.g. ``"token"``. ``None`` (the default)
    disables it — a ``?token=`` upgrade against the defaults authenticates nothing."""

    payload_key: str = REALTIME_AUTH_TOKEN_KEY
    """Key carrying the token inside an auth payload."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.cookie_name is None and self.header_name is None and self.query_param is None:
            raise exc.configuration(
                "A realtime credential ladder with every request source disabled can only "
                "authenticate a reauth payload, so no connection could ever be established. "
                "Enable at least one of cookie_name / header_name / query_param.",
                code="realtime_auth_no_sources",
            )


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class RealtimeHandshake:
    """One handshake, as the ladder reads it — plain mappings, no transport types.

    *headers* is looked up case-insensitively (transports normalize differently);
    everything else is read as given.
    """

    cookies: Mapping[str, str] = attrs.field(factory=dict[str, str])
    """Cookies on the upgrade request."""

    headers: Mapping[str, str] = attrs.field(factory=dict[str, str])
    """Headers on the upgrade request."""

    query: Mapping[str, str] = attrs.field(factory=dict[str, str])
    """Query parameters on the upgrade request."""

    auth: Mapping[str, Any] | None = None
    """The connect / ``realtime.reauth`` auth payload, when the transport carries one."""

    # ....................... #

    def header(self, name: str) -> str | None:
        """Case-insensitive header lookup."""

        wanted = name.lower()

        for key, value in self.headers.items():
            if key.lower() == wanted:
                return value

        return None


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True, repr=False)
class PresentedCredential:
    """A credential the handshake presented, and which source presented it."""

    token: str = attrs.field(repr=False)
    """The raw token string."""

    source: CredentialSource
    """Which ladder rung presented it."""

    scheme: str = "Bearer"
    """The scheme label, as presented (the header source reads it off the header)."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class RealtimeIdentity:
    """What the ladder resolves — the transport maps it onto its own connection type.

    :class:`~forze_fastapi.realtime.WsConnection` and
    :class:`~forze_socketio.RealtimeConnection` carry exactly these four fields; they
    stay separate types because they are constructed and enforced by separate
    transports.
    """

    authn: AuthnIdentity
    """The authenticated principal."""

    tenant: UUID | None = None
    """The connection's tenant."""

    client: ClientIdentity | None = None
    """The device / session this connection is, keying its offline-replay cursor."""

    expires_at: datetime | None = None
    """When the presented credential expires — timezone-aware, from the verifier."""


# ----------------------- #


def _split_bearer(raw: str) -> tuple[str, str]:
    """``("Bearer", token)`` from an ``Authorization`` value, or ``("Bearer", raw)``.

    A bare token with no scheme is accepted and labeled ``Bearer``: the WS upgrade is
    the one place clients most often set the header by hand.
    """

    parts = raw.strip().split(maxsplit=1)

    if len(parts) == 2:
        return parts[0], parts[1].strip()

    return "Bearer", raw.strip()


# ....................... #


def present_credential(
    sources: RealtimeCredentialSources,
    handshake: RealtimeHandshake,
) -> PresentedCredential | None:
    """The first credential the handshake presents, in the ladder's fixed order.

    Returns ``None`` when no enabled source carries one — an anonymous handshake,
    which every realtime route refuses on its own terms. **A presented credential
    ends the ladder**, valid or not: verification failure refuses the connection
    rather than falling through, or a revoked cookie would become an attempt to
    authenticate as somebody else via the next source.
    """

    payload = handshake.auth

    if payload is not None:
        raw = payload.get(sources.payload_key)

        if isinstance(raw, str) and raw.strip():
            return PresentedCredential(token=raw.strip(), source="payload")

    if sources.cookie_name is not None:
        raw = handshake.cookies.get(sources.cookie_name)

        if raw is not None and raw.strip():
            return PresentedCredential(token=raw.strip(), source="cookie")

    if sources.header_name is not None:
        raw = handshake.header(sources.header_name)

        if raw is not None and raw.strip():
            scheme, token = _split_bearer(raw)

            if token:
                return PresentedCredential(token=token, source="header", scheme=scheme)

    if sources.query_param is not None:
        raw = handshake.query.get(sources.query_param)

        if raw is not None and raw.strip():
            return PresentedCredential(token=raw.strip(), source="query")

    return None


# ....................... #


def _expiry(result: AuthnResult, *, source: CredentialSource) -> datetime | None:
    """The credential's expiry, refusing a naive instant rather than guessing UTC.

    A naive value cannot be compared against the aware clock the transports enforce
    with, and assuming UTC would enforce expiry at the wrong instant on a verifier
    that meant something else — so this fails loudly, at the seam that produced it.
    """

    expires_at = result.expires_at

    if expires_at is not None and expires_at.tzinfo is None:
        raise exc.configuration(
            f"The token verifier asserted a naive expires_at for the {source} credential; "
            "a realtime connection enforces expiry against an aware clock. Fix the verifier "
            "to return a timezone-aware instant (the token's exp claim is UTC).",
            code="realtime_auth_expiry_naive",
        )

    return expires_at


# ....................... #


def client_identity(
    handshake: RealtimeHandshake,
    *,
    device_id_key: str = "device_id",
    session_id_key: str = "session_id",
) -> ClientIdentity | None:
    """The device / session the handshake names, or ``None`` when it names neither.

    Read from the query parameters and the auth payload — client-supplied either way,
    which is why the cursor key built from it is namespaced under the principal
    downstream: a spoofed device id can only ever address its own principal's state.
    """

    payload = handshake.auth or {}

    def _value(key: str) -> str | None:
        raw = payload.get(key)

        if not isinstance(raw, str) or not raw.strip():
            raw = handshake.query.get(key)

        return raw.strip() if isinstance(raw, str) and raw.strip() else None

    device_id, session_id = _value(device_id_key), _value(session_id_key)

    if device_id is None and session_id is None:
        return None

    return ClientIdentity(device_id=device_id, session_id=session_id)


# ....................... #


async def _tenant(
    result: AuthnResult,
    *,
    tenants: TenantResolverPort | None,
) -> UUID | None:
    """The connection's tenant: the resolver's answer, else the verified issuer hint.

    Same ladder the HTTP boundary runs, minus its ``X-Tenant-Id`` fallback — that one
    trusts a gateway header, and a WebSocket upgrade's headers are set by the client
    the route is authenticating. The issuer hint is kept because it rides a credential
    the authn plane just verified, and a resolver that has no binding for a principal
    returns ``None`` (a genuine mismatch raises instead, inside the call).
    """

    hint = parse_tenant_hint(result.issuer_tenant_hint)

    if tenants is None:
        return hint

    resolved = await tenants.resolve_from_principal(
        result.identity.principal_id,
        requested_tenant_id=hint,
    )

    return resolved.tenant_id if resolved is not None else hint


# ....................... #


async def resolve_realtime_identity(
    *,
    authn: AuthnPort,
    sources: RealtimeCredentialSources,
    handshake: RealtimeHandshake,
    tenants: TenantResolverPort | None = None,
    device_id_key: str = "device_id",
    session_id_key: str = "session_id",
) -> RealtimeIdentity | None:
    """Verify the handshake's credential through *authn* and resolve the connection's identity.

    Returns ``None`` for an anonymous handshake (no enabled source presented a
    credential). Raises whatever the authn plane raises for an invalid one — a
    client-safe :class:`~forze.base.exceptions.CoreException` the transport turns
    into its own refusal.

    The client identity is read from the handshake's query parameters and auth payload
    under *device_id_key* / *session_id_key*.
    """

    presented = present_credential(sources, handshake)

    if presented is None:
        return None

    result = await authn.authenticate_with_token(
        AccessTokenCredentials(token=presented.token, scheme=presented.scheme)
    )

    return RealtimeIdentity(
        authn=result.identity,
        tenant=await _tenant(result, tenants=tenants),
        client=client_identity(
            handshake,
            device_id_key=device_id_key,
            session_id_key=session_id_key,
        ),
        expires_at=_expiry(result, source=presented.source),
    )
