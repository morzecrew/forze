"""RFC 7662 token introspection: verify opaque access tokens at the issuer."""

from ._compat import require_oidc

require_oidc()

# ....................... #

import base64
import math
from collections.abc import Sequence
from datetime import timedelta
from typing import Any, Final, cast, final
from urllib.parse import quote_plus

import attrs
import httpx
import jwt
from pydantic import SecretStr

from forze.application.contracts.authn import (
    AccessTokenCredentials,
    AuthnSpec,
    TokenVerifierPort,
    VerifiedAssertion,
)
from forze.application.execution import ExecutionContext
from forze.base.exceptions import exc
from forze.base.primitives import secure_token_urlsafe, utcnow

from ._cache import TokenCache
from .claims import OidcClaimMapper
from .discovery import require_https, require_timeout

# ----------------------- #

_JWT_BEARER_ASSERTION: Final[str] = "urn:ietf:params:oauth:client-assertion-type:jwt-bearer"

_ASYMMETRIC_ALGORITHMS: Final[frozenset[str]] = frozenset(
    {"RS256", "RS384", "RS512", "PS256", "PS384", "PS512", "ES256", "ES384", "ES512", "EdDSA"},
)
"""``private_key_jwt`` signs with a private key; an HMAC algorithm is ``client_secret_jwt``."""

_ASSERTION_LIFETIME: Final[timedelta] = timedelta(seconds=60)


def _reveal(value: str | SecretStr) -> str:
    return value.get_secret_value() if isinstance(value, SecretStr) else value


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class BasicClientAuth:
    """``client_secret_basic`` (RFC 6749 §2.3.1): HTTP Basic with the client id and secret."""

    client_id: str
    """The introspecting client's id at the issuer."""

    client_secret: str | SecretStr = attrs.field(repr=False)
    """The client secret."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if not self.client_id.strip() or not _reveal(self.client_secret):
            raise exc.configuration("BasicClientAuth requires a client_id and a client_secret")

    # ....................... #

    def request_parts(self) -> tuple[dict[str, str], dict[str, str]]:
        """Form fields and headers that authenticate the introspection request."""

        # RFC 6749 §2.3.1: each part is form-urlencoded before the Basic encoding.
        pair = f"{quote_plus(self.client_id)}:{quote_plus(_reveal(self.client_secret))}"

        return {}, {"Authorization": "Basic " + base64.b64encode(pair.encode()).decode()}


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class PrivateKeyJwtAuth:
    """``private_key_jwt`` (RFC 7523 §2.2): a short-lived JWT signed with the client's key.

    Zitadel recommends it for API introspection. The assertion carries ``iss`` = ``sub`` =
    the client id, ``aud`` = :attr:`audience`, a 60-second ``exp`` and a random ``jti``.
    """

    client_id: str
    """The introspecting client's id at the issuer."""

    private_key: str | SecretStr = attrs.field(repr=False)
    """PEM-encoded private key."""

    audience: str
    """``aud`` of the assertion — the issuer URL for Zitadel and Keycloak."""

    key_id: str | None = None
    """``kid`` header, when the issuer selects the key by id (Zitadel's key file ``keyId``)."""

    algorithm: str = "RS256"
    """Asymmetric JWS algorithm matching the key."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if not self.client_id.strip() or not self.audience.strip():
            raise exc.configuration("PrivateKeyJwtAuth requires a client_id and an audience")

        if self.algorithm not in _ASYMMETRIC_ALGORITHMS:
            raise exc.configuration(
                f"PrivateKeyJwtAuth algorithm must be one of {sorted(_ASYMMETRIC_ALGORITHMS)}",
            )

        try:
            self.assertion()

        except (jwt.PyJWTError, ValueError, TypeError):
            # Not chained: a key-loading error could quote key material.
            raise exc.configuration(
                "PrivateKeyJwtAuth private_key does not sign with the configured algorithm",
            ) from None

    # ....................... #

    def assertion(self) -> str:
        """A fresh signed client assertion."""

        now = utcnow()

        return jwt.encode(  # pyright: ignore[reportUnknownMemberType]
            {
                "iss": self.client_id,
                "sub": self.client_id,
                "aud": self.audience,
                "iat": int(now.timestamp()),
                "exp": int((now + _ASSERTION_LIFETIME).timestamp()),
                "jti": secure_token_urlsafe(16),
            },
            _reveal(self.private_key),
            algorithm=self.algorithm,
            headers={"kid": self.key_id} if self.key_id is not None else None,
        )

    # ....................... #

    def request_parts(self) -> tuple[dict[str, str], dict[str, str]]:
        """Form fields and headers that authenticate the introspection request."""

        return {
            "client_assertion_type": _JWT_BEARER_ASSERTION,
            "client_assertion": self.assertion(),
        }, {}


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class IntrospectionConfig:
    """Where and how to introspect, and what an active token must carry."""

    endpoint: str
    """The issuer's introspection endpoint (``OidcProviderMetadata.introspection_endpoint``)."""

    issuer: str
    """The issuer the tokens belong to. Recorded on every assertion, and a response whose
    ``iss`` differs is refused. Required: RFC 7662 makes ``iss`` optional in the response,
    and the principal resolver keys on it."""

    client_auth: BasicClientAuth | PrivateKeyJwtAuth
    """How this resource server authenticates to the introspection endpoint."""

    audience: str | Sequence[str] | None = None
    """Required ``aud`` value(s); the response's ``aud`` must contain one of them."""

    require_audience: bool = True
    """When ``True`` (default), :attr:`audience` must be set. Turn off only for an issuer that
    mints tokens without an audience (a Hydra client that requests none) — any active token
    of that issuer is then accepted, whichever client it was minted for."""

    leeway: timedelta = timedelta(seconds=10)
    """Clock-skew leeway for the response's ``exp`` / ``nbf``."""

    cache_ttl: timedelta = timedelta(seconds=30)
    """How long an active verdict is reused; clamped by the token's ``exp``, at most five
    minutes, zero to disable. A revoked token keeps working until its entry expires."""

    cache_max_entries: int = 1024
    """Cached verdicts kept per verifier."""

    timeout: float = 10.0
    """Request timeout in seconds."""

    claim_mapper: OidcClaimMapper = attrs.field(factory=OidcClaimMapper)
    """Maps the response — a claims map — onto the assertion, ``_forze`` extraction included."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        require_https(self.endpoint, what="Introspection endpoint")

        if not self.issuer.strip():
            raise exc.configuration("Introspection issuer must be non-empty")

        if self.require_audience and self.audience is None:
            raise exc.configuration(
                "Introspection requires an audience; set require_audience=False only for an "
                "issuer that mints tokens without one",
            )

        if self.audience is not None:
            configured = (self.audience,) if isinstance(self.audience, str) else self.audience

            if not configured or any(not isinstance(a, str) or not a.strip() for a in configured):
                raise exc.configuration(
                    "Introspection audience must be a non-empty string or a non-empty sequence "
                    "of them",
                )

        if self.claim_mapper.issuer_claim != "iss":
            # The issuer is checked as `iss`; a mapper reading another claim could record an
            # issuer other than the configured one on the assertion.
            raise exc.configuration("Introspection claim_mapper must read the issuer from 'iss'")

        if self.leeway < timedelta(0):
            raise exc.configuration("Leeway must not be negative")

        require_timeout(self.timeout, what="Introspection timeout")


# ....................... #


def _is_timestamp(value: object) -> bool:
    """A finite JSON number: ``NaN`` compares false both ways and would slip past a bound."""

    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return False

    try:
        return math.isfinite(value)

    except OverflowError:  # an integer too large for a float
        return False


# ....................... #


def _invalid() -> Exception:
    # One shape for every rejected token: an inactive, revoked, expired or foreign token must
    # not be distinguishable by the caller.
    return exc.authentication("Invalid OIDC token", code="invalid_oidc_token")


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class IntrospectionTokenVerifier(TokenVerifierPort):
    """Verify an opaque access token by introspecting it at the issuer (RFC 7662).

    Fails closed: ``active`` other than ``true``, an ``error`` in a 2xx body, a foreign
    ``iss``, a missing ``sub``, a missing, foreign or malformed ``aud`` (when required), a
    time claim that is not a finite number, a claim the mapper requires that is missing, or an
    expired / not-yet-valid token is an ``authentication`` failure (``invalid_oidc_token``). A
    transport failure, timeout, non-2xx status, or a body that is not a JSON object is an
    ``infrastructure`` failure — those say nothing about the token, so they are not reported
    as one. No path emits an assertion without an active verdict.

    Active verdicts are cached per token digest (see
    :class:`~forze_identity.oidc._cache.TokenCache`). Build it once and reuse it — the
    :class:`ConfigurableIntrospectionVerifier` factory does — or the cache never hits.
    The token and the client credentials are never logged.
    """

    config: IntrospectionConfig
    """Endpoint, client authentication and acceptance policy."""

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
    """Active verdicts by token digest. One per verifier: the key is the token alone, so a cache
    shared with another verifier would serve that verifier's verdicts here."""

    # ....................... #

    async def verify_token(
        self,
        credentials: AccessTokenCredentials,
    ) -> VerifiedAssertion:
        cached = self.cache.get(credentials.token)

        if cached is not None:
            return cached

        began = self.cache.begin()
        payload = await self._introspect(credentials.token)
        assertion = self._accept(payload)
        # _accept has checked exp is a finite number or absent.
        self.cache.put(credentials.token, assertion, began=began, expires_at=payload.get("exp"))

        return assertion

    # ....................... #

    async def _introspect(self, token: str) -> dict[str, Any]:
        form, headers = self.config.client_auth.request_parts()
        form["token"] = token
        headers["Accept"] = "application/json"

        # shortcut: a client per call, as the VK verifier and the code-exchange helpers do, so
        # a cache miss pays a TLS handshake; a lifecycle-closed client if miss latency matters.
        try:
            async with httpx.AsyncClient(
                timeout=self.config.timeout,
                transport=self.transport,
            ) as client:
                response = await client.post(self.config.endpoint, data=form, headers=headers)

        except httpx.HTTPError as e:
            raise exc.infrastructure(
                "OIDC token introspection request failed",
                code="oidc_introspection_unavailable",
            ) from e

        if not response.is_success:
            # RFC 7662 answers an unknown or revoked token with 200 + active:false; a non-2xx
            # is about this client (401: bad client credentials) or the issuer, not the token.
            raise exc.infrastructure(
                "OIDC token introspection was refused",
                code="oidc_introspection_unavailable",
                details={"status": response.status_code},
            )

        try:
            payload = response.json()

        except ValueError as e:
            raise exc.infrastructure(
                "OIDC token introspection returned a non-JSON response",
                code="oidc_introspection_invalid_response",
            ) from e

        if not isinstance(payload, dict):
            raise exc.infrastructure(
                "OIDC token introspection response must be a JSON object",
                code="oidc_introspection_invalid_response",
            )

        return cast(dict[str, Any], payload)

    # ....................... #

    def _accept(self, payload: dict[str, Any]) -> VerifiedAssertion:
        cfg = self.config

        if payload.get("active") is not True or "error" in payload:
            raise _invalid()

        iss = payload.setdefault("iss", cfg.issuer)
        sub = payload.get("sub")

        if iss != cfg.issuer or not isinstance(sub, str) or not sub.strip():
            raise _invalid()

        now = utcnow().timestamp()
        leeway = cfg.leeway.total_seconds()
        exp, nbf = payload.get("exp"), payload.get("nbf")

        if any(v is not None and not _is_timestamp(v) for v in (exp, nbf, payload.get("iat"))):
            raise _invalid()

        if (exp is not None and exp <= now - leeway) or (nbf is not None and nbf > now + leeway):
            raise _invalid()

        validated_audience = self._validated_audience(payload.get("aud"))

        if cfg.audience is not None and validated_audience is None:
            raise _invalid()

        try:
            return cfg.claim_mapper.map(payload, validated_audience=validated_audience)

        except ValueError as e:  # a claim the mapper is configured to require is missing
            raise _invalid() from e

    # ....................... #

    def _validated_audience(self, token_aud: object) -> str | None:
        """The configured audience the response presented, or ``None``.

        :raises CoreException: ``authentication`` when ``aud`` is a list with a non-string
            member — a malformed claim, refused rather than read around.
        """

        audience = self.config.audience

        if audience is None:
            return None

        configured = (audience,) if isinstance(audience, str) else tuple(audience)

        if isinstance(token_aud, str):
            presented: set[str] = {token_aud}

        elif isinstance(token_aud, list):
            members = cast(list[object], token_aud)

            if not all(isinstance(aud, str) for aud in members):
                raise _invalid()

            presented = set(cast(list[str], members))

        else:
            presented = set()

        return next((aud for aud in configured if aud in presented), None)


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ConfigurableIntrospectionVerifier:
    """Build :class:`IntrospectionTokenVerifier` once and return it for every call.

    Built once so its verdict cache spans requests; wire this as the route's
    ``TokenVerifierPort`` factory and do not reconstruct it per request.
    """

    config: IntrospectionConfig
    """Endpoint, client authentication and acceptance policy."""

    _verifier: IntrospectionTokenVerifier = attrs.field(
        init=False,
        repr=False,
        default=attrs.Factory(
            lambda self: IntrospectionTokenVerifier(config=self.config),
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
