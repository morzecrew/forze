"""Session verification at a whoami endpoint (Ory Kratos ``/sessions/whoami``)."""

from ._compat import require_oidc

require_oidc()

# ....................... #

from collections.abc import Mapping
from datetime import datetime, timedelta
from typing import Any, Final, Literal, cast, final

import attrs
import httpx

from forze.application.contracts.authn import (
    AccessTokenCredentials,
    AuthnSpec,
    TokenVerifierPort,
    VerifiedAssertion,
)
from forze.application.execution import ExecutionContext
from forze.base.exceptions import exc
from forze.base.primitives import utcnow

from ._cache import TokenCache
from .claims import OidcClaimMapper
from .discovery import require_https, require_timeout

# ----------------------- #

_CREDENTIAL_STYLES: Final[frozenset[str]] = frozenset({"cookie", "x_session_token"})

_COOKIE_UNSAFE: Final[frozenset[str]] = frozenset('";,\\')
"""Characters RFC 6265 excludes from a cookie value, beyond controls and whitespace."""


_TOKEN_SEPARATORS: Final[frozenset[str]] = frozenset('()<>@,;:\\"/[]?={}')
"""RFC 2616 separators, which RFC 6265 excludes from a cookie name."""


def _is_safe_credential(value: str) -> bool:
    """Printable ASCII with no cookie separators, so it cannot smuggle a cookie or header."""

    return bool(value) and all("!" <= c <= "~" and c not in _COOKIE_UNSAFE for c in value)


def _is_cookie_name(value: str) -> bool:
    """An RFC 6265 cookie name: a non-empty token of printable ASCII without separators."""

    return bool(value) and all("!" <= c <= "~" and c not in _TOKEN_SEPARATORS for c in value)


def _timestamp(raw: object) -> datetime | None:
    """An RFC 3339 timestamp as an aware datetime; ``None`` when absent."""

    if raw is None:
        return None

    if not isinstance(raw, str):
        raise ValueError("timestamp must be a string")

    parsed = datetime.fromisoformat(raw)

    if parsed.tzinfo is None:
        raise ValueError("timestamp must carry an offset")

    return parsed


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class WhoamiConfig:
    """Where the session endpoint is, how to present the credential, and what to record."""

    endpoint: str
    """The whoami endpoint (Kratos public API ``/sessions/whoami``)."""

    issuer: str
    """Issuer recorded on every assertion — the principal resolver's discriminator, so keep it
    stable (the Kratos public URL is the natural choice)."""

    credential_style: Literal["cookie", "x_session_token"]
    """``"cookie"`` forwards the credential as :attr:`cookie_name` (the value a
    ``CookieTokenAuthn`` ingress delivers); ``"x_session_token"`` sends it as the
    ``X-Session-Token`` header (a Kratos session token from a native or API client)."""

    cookie_name: str = "ory_kratos_session"
    """Session cookie name, for ``credential_style="cookie"``."""

    traits_to_claims: bool = True
    """Copy ``identity.traits`` and ``identity.metadata_public`` into the claims as ``traits``
    and ``metadata``. Kratos lets users edit their own traits: derive roles from
    ``metadata`` (admin-written), never from ``traits``."""

    cache_ttl: timedelta = timedelta(seconds=30)
    """How long an active session verdict is reused; clamped by the session's
    ``expires_at``, at most five minutes, zero to disable. A revoked session keeps working
    until its entry expires."""

    cache_max_entries: int = 1024
    """Cached verdicts kept per verifier."""

    timeout: float = 10.0
    """Request timeout in seconds."""

    claim_mapper: OidcClaimMapper = attrs.field(factory=OidcClaimMapper)
    """Maps the session claims (``iss``, ``sub``, ``exp``, ``iat``, ``sid``, ``traits``,
    ``metadata``) onto the assertion — ``email_claim="traits.email"`` and
    ``roles=ClaimsPath("metadata.roles")`` work here exactly as for a token."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        require_https(self.endpoint, what="Whoami endpoint")

        if not self.issuer.strip():
            raise exc.configuration("Whoami issuer must be non-empty")

        if self.credential_style not in _CREDENTIAL_STYLES:
            # The Literal is not enforced at runtime; an unknown style must not fall through.
            raise exc.configuration(
                f"credential_style must be one of {sorted(_CREDENTIAL_STYLES)}",
            )

        if not _is_cookie_name(self.cookie_name):
            raise exc.configuration("cookie_name must be an RFC 6265 token (no separators)")

        if self.claim_mapper.issuer_claim != "iss" or self.claim_mapper.subject_claim != "sub":
            # The session claims carry the configured issuer as `iss` and identity.id as
            # `sub`; a mapper reading other claims could record something else.
            raise exc.configuration(
                "Whoami claim_mapper must read the issuer from 'iss' and the subject from 'sub'",
            )

        require_timeout(self.timeout, what="Whoami timeout")


# ....................... #


def _invalid() -> Exception:
    return exc.authentication("Invalid session", code="invalid_session_token")


def _invalid_response(message: str) -> Exception:
    return exc.infrastructure(message, code="session_whoami_invalid_response")


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class WhoamiSessionVerifier(TokenVerifierPort):
    """Verify a session credential by asking the session endpoint who it belongs to.

    Kratos-shaped: the subject is ``identity.id`` and the expiry the session's
    ``expires_at``. Fails closed: 401/403 (no valid session, or one below the required
    assurance level), ``active`` other than ``true``, an identity whose ``state`` is not
    ``active`` (a missing ``state`` included), an expired session or a credential that is not
    a plain cookie-safe string is an ``authentication`` failure (``invalid_session_token``). A
    transport failure, timeout, other status, a body that is not a JSON object, or a session
    whose ``expires_at`` is missing or malformed is an ``infrastructure`` failure.

    Active verdicts are cached per credential digest; build it once and reuse it (the
    :class:`ConfigurableWhoamiVerifier` factory does). The credential is never logged.
    """

    config: WhoamiConfig
    """Endpoint, credential presentation and claims policy."""

    transport: httpx.AsyncBaseTransport | None = attrs.field(default=None, eq=False)
    """Optional httpx transport override (inject :class:`httpx.MockTransport` in tests)."""

    cache: TokenCache = attrs.field(
        eq=False,
        repr=False,
        default=attrs.Factory(
            lambda self: TokenCache(
                ttl=self.config.cache_ttl,
                max_entries=self.config.cache_max_entries,
            ),
            takes_self=True,
        ),
    )
    """Active verdicts by credential digest. One per verifier: the key is the credential alone,
    so a cache shared with another verifier would serve that verifier's verdicts here."""

    # ....................... #

    async def verify_token(
        self,
        credentials: AccessTokenCredentials,
    ) -> VerifiedAssertion:
        token = credentials.token

        if not _is_safe_credential(token):
            raise _invalid()

        cached = self.cache.get(token)

        if cached is not None:
            return cached

        began = self.cache.begin()
        assertion, expires_at = self._accept(await self._whoami(token))
        self.cache.put(token, assertion, began=began, expires_at=expires_at)

        return assertion

    # ....................... #

    async def _whoami(self, token: str) -> dict[str, Any]:
        cfg = self.config
        headers = {"Accept": "application/json"}

        if cfg.credential_style == "cookie":
            headers["Cookie"] = f"{cfg.cookie_name}={token}"

        else:
            headers["X-Session-Token"] = token

        # shortcut: a client per call, as the VK verifier and the code-exchange helpers do, so
        # a cache miss pays a TLS handshake; a lifecycle-closed client if miss latency matters.
        try:
            async with httpx.AsyncClient(timeout=cfg.timeout, transport=self.transport) as client:
                response = await client.get(cfg.endpoint, headers=headers)

        except httpx.HTTPError as e:
            raise exc.infrastructure(
                "Session whoami request failed",
                code="session_whoami_unavailable",
            ) from e

        if response.status_code in (401, 403):
            raise _invalid()

        if response.status_code != 200:
            raise exc.infrastructure(
                "Session whoami was refused",
                code="session_whoami_unavailable",
                details={"status": response.status_code},
            )

        try:
            payload = response.json()

        except ValueError as e:
            raise _invalid_response("Session whoami returned a non-JSON response") from e

        if not isinstance(payload, dict):
            raise _invalid_response("Session whoami response must be a JSON object")

        return cast(dict[str, Any], payload)

    # ....................... #

    def _accept(self, session: dict[str, Any]) -> tuple[VerifiedAssertion, float]:
        """The session's assertion, and the expiry it was checked against."""

        identity = session.get("identity")

        if session.get("active") is not True or not isinstance(identity, Mapping):
            raise _invalid()

        identity = cast(Mapping[str, Any], identity)
        subject = identity.get("id")
        state = identity.get("state")

        if not isinstance(subject, str) or not subject.strip() or state != "active":
            raise _invalid()

        issued_raw = session.get("issued_at")

        if issued_raw is None:
            issued_raw = session.get("authenticated_at")

        try:
            expires_at = _timestamp(session.get("expires_at"))
            issued_at = _timestamp(issued_raw)

        except ValueError as e:
            raise _invalid_response("Session whoami returned a malformed timestamp") from e

        if expires_at is None:
            # Kratos always sets it; a session without one would be cached as if it never ends.
            raise _invalid_response("Session whoami returned a session without expires_at")

        if expires_at <= utcnow():
            raise _invalid()

        claims: dict[str, Any] = {
            "iss": self.config.issuer,
            "sub": subject,
            "exp": expires_at.timestamp(),
        }

        if issued_at is not None:
            claims["iat"] = issued_at.timestamp()

        if isinstance(session.get("id"), str):
            claims["sid"] = session["id"]

        if self.config.traits_to_claims:
            for source, target in (("traits", "traits"), ("metadata_public", "metadata")):
                value = identity.get(source)

                if isinstance(value, Mapping):
                    claims[target] = dict(cast(Mapping[str, Any], value))

        return self.config.claim_mapper.map(claims), claims["exp"]


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ConfigurableWhoamiVerifier:
    """Build :class:`WhoamiSessionVerifier` once and return it for every call.

    Built once so its verdict cache spans requests; wire this as the route's
    ``TokenVerifierPort`` factory and do not reconstruct it per request.
    """

    config: WhoamiConfig
    """Endpoint, credential presentation and claims policy."""

    _verifier: WhoamiSessionVerifier = attrs.field(
        init=False,
        repr=False,
        default=attrs.Factory(
            lambda self: WhoamiSessionVerifier(config=self.config),
            takes_self=True,
        ),
    )
    """The verifier built once from :attr:`config`."""

    # ....................... #

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: AuthnSpec,
    ) -> TokenVerifierPort:
        _ = ctx, spec
        return self._verifier
