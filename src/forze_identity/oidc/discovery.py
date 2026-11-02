"""OIDC discovery: read an issuer's ``.well-known/openid-configuration`` once, at wiring.

Discovery is a construction-time convenience, never a per-request dependency: an outage can
delay startup, never fail a request. A hand-configured :class:`~forze_identity.oidc.OidcIdpPreset`
stays the pinned, offline path.
"""

from ._compat import require_oidc

require_oidc()

# ....................... #

from ipaddress import ip_address
from typing import Any, cast, final
from urllib.parse import urlsplit

import attrs
import httpx

from forze.base.exceptions import exc

# ----------------------- #


def _is_loopback(host: str) -> bool:
    if host == "localhost":
        return True

    try:
        return ip_address(host).is_loopback

    except ValueError:
        return False


# ....................... #


def require_https(url: str, *, what: str) -> None:
    """Refuse an endpoint that would carry tokens or client credentials in clear.

    The same rule as :mod:`forze_identity.oauth.authorize`: ``https``, or ``http`` on a
    loopback host (a developer's own machine). The URL is not echoed — it is named by *what*.

    :raises CoreException: ``configuration`` when *url* is not an absolute https URL.
    """

    try:
        parts = urlsplit(url)

    except ValueError as e:
        raise exc.configuration(f"{what} is not a parseable URL") from e

    host = parts.hostname or ""

    if parts.scheme == "https" and host:
        return

    if parts.scheme == "http" and _is_loopback(host):
        return

    raise exc.configuration(
        f"{what} must be an absolute https URL (http is allowed only on a loopback host)",
    )


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class OidcProviderMetadata:
    """The parts of an OIDC discovery document Forze reads, after validation."""

    issuer: str
    """The issuer, equal to the one discovery was asked for."""

    jwks_uri: str
    """JWKS URL for signature verification."""

    introspection_endpoint: str | None = None
    """RFC 7662 introspection endpoint, when the provider advertises one."""

    token_endpoint: str | None = None
    """OAuth2 token endpoint, when advertised."""

    userinfo_endpoint: str | None = None
    """OIDC userinfo endpoint, when advertised."""


# ....................... #


def _optional_endpoint(document: dict[str, Any], name: str) -> str | None:
    value = document.get(name)

    if value is None:
        return None

    if not isinstance(value, str):
        raise exc.configuration(
            f"OIDC discovery document field {name!r} must be a string",
            code="oidc_discovery_invalid",
        )

    require_https(value, what=f"OIDC discovery {name}")

    return value


# ....................... #


async def fetch_oidc_configuration(
    issuer: str,
    *,
    timeout: float = 10.0,
    transport: httpx.AsyncBaseTransport | None = None,
) -> OidcProviderMetadata:
    """Fetch and validate ``{issuer}/.well-known/openid-configuration``.

    The document's ``issuer`` must equal *issuer* exactly (OIDC Discovery §4.3, RFC 8414
    §3.3); anything else is refused, since a document naming another issuer would point the
    verifier at keys that are not this issuer's. Every endpoint read from it must be https
    (http only on loopback). Redirects are not followed.

    Algorithms are **not** read from the document: widening a verifier's allowlist from a
    remote file is how an ``alg`` confusion gets in. Pass them explicitly.

    :param issuer: The issuer URL exactly as tokens carry it in ``iss``.
    :param timeout: Request timeout in seconds.
    :param transport: Optional httpx transport (inject :class:`httpx.MockTransport` in tests).
    :returns: The validated metadata.
    :raises CoreException: ``infrastructure`` (``oidc_discovery_unavailable``) when the
        request fails in transport; ``configuration`` when the issuer is not https, the
        response is not a 2xx JSON object, a required field is missing or malformed, or the
        document names another issuer (``oidc_issuer_mismatch``).
    """

    require_https(issuer, what="OIDC issuer")

    parts = urlsplit(issuer)

    if parts.query or parts.fragment:
        raise exc.configuration("OIDC issuer must not carry a query or fragment")

    if timeout <= 0:
        raise exc.configuration("Timeout must be positive")

    # OIDC Discovery §4: a terminating "/" is removed before appending the well-known path.
    url = issuer.rstrip("/") + "/.well-known/openid-configuration"

    try:
        async with httpx.AsyncClient(timeout=timeout, transport=transport) as client:
            response = await client.get(url, headers={"Accept": "application/json"})

    except httpx.HTTPError as e:
        raise exc.infrastructure(
            "OIDC discovery request failed",
            code="oidc_discovery_unavailable",
        ) from e

    if not response.is_success:
        raise exc.configuration(
            "OIDC discovery document could not be fetched",
            code="oidc_discovery_invalid",
            details={"status": response.status_code},
        )

    try:
        document = response.json()

    except ValueError as e:
        raise exc.configuration(
            "OIDC discovery document is not valid JSON",
            code="oidc_discovery_invalid",
        ) from e

    if not isinstance(document, dict):
        raise exc.configuration(
            "OIDC discovery document must be a JSON object",
            code="oidc_discovery_invalid",
        )

    document = cast(dict[str, Any], document)

    if document.get("issuer") != issuer:
        raise exc.configuration(
            "OIDC discovery document names a different issuer",
            code="oidc_issuer_mismatch",
        )

    jwks_uri = _optional_endpoint(document, "jwks_uri")

    if jwks_uri is None:
        raise exc.configuration(
            "OIDC discovery document has no jwks_uri",
            code="oidc_discovery_invalid",
        )

    return OidcProviderMetadata(
        issuer=issuer,
        jwks_uri=jwks_uri,
        introspection_endpoint=_optional_endpoint(document, "introspection_endpoint"),
        token_endpoint=_optional_endpoint(document, "token_endpoint"),
        userinfo_endpoint=_optional_endpoint(document, "userinfo_endpoint"),
    )
