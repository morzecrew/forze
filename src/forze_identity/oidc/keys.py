from ._compat import require_oidc

require_oidc()

# ....................... #

from datetime import timedelta
from typing import Any, Protocol, final

import attrs
from jwt import InvalidTokenError, PyJWKClient, PyJWKClientError

from forze.base.exceptions import exc

# ----------------------- #


class SigningKeyProviderPort(Protocol):
    """Resolve a signing key for a JWT, optionally keyed by ``kid`` from the header.

    Allows the verifier to be unit-tested with an in-memory key without touching the
    network. Production deployments wire :class:`JwksKeyProvider` for IdPs that publish a
    standard ``jwks_uri`` (Firebase, Casdoor, generic OIDC).
    """

    def get_signing_key(self, token: str) -> Any: ...  # pragma: no cover


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class StaticKeyProvider(SigningKeyProviderPort):
    """Return the same key for every token (handy for HS256 tests / single-tenant setups)."""

    key: Any
    """The verification key (bytes for HMAC, PEM for RSA/EC)."""

    # ....................... #

    def get_signing_key(self, token: str) -> Any:
        _ = token
        return self.key


# ....................... #


@final
@attrs.define(slots=True, kw_only=True)
class JwksKeyProvider(SigningKeyProviderPort):
    """Fetch and cache JWKS from a URL using :class:`jwt.PyJWKClient`.

    Reads the ``kid`` from the JWT header to pick the right key. Caches on the underlying
    client; instantiate once per IdP issuer.
    """

    jwks_uri: str
    """The JWKS URI to fetch the signing keys from."""

    cache_keys: bool = True
    """Whether to cache the signing keys."""

    cache_ttl: timedelta = attrs.field(default=timedelta(seconds=300))
    """JWKS signing key cache TTL."""

    timeout: timedelta = attrs.field(default=timedelta(seconds=10))
    """HTTP timeout for JWKS fetches."""

    # Non-init field to lazily create the client.
    _client: PyJWKClient | None = attrs.field(default=None, init=False)

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.cache_ttl.total_seconds() <= 0:
            raise exc.configuration("Cache TTL must be positive")

        if self.timeout.total_seconds() <= 0:
            raise exc.configuration("Timeout must be positive")

    # ....................... #

    def _require_client(self) -> PyJWKClient:
        if self._client is not None:
            return self._client

        self._client = PyJWKClient(
            uri=self.jwks_uri,
            cache_keys=self.cache_keys,
            lifespan=int(self.cache_ttl.total_seconds()),
            timeout=int(self.timeout.total_seconds()),
        )

        return self._client

    # ....................... #

    def get_signing_key(self, token: str) -> Any:
        """Get the signing key for the given token."""

        c = self._require_client()

        try:
            return c.get_signing_key_from_jwt(token).key

        except (PyJWKClientError, InvalidTokenError) as e:
            raise exc.authentication(
                "Could not resolve OIDC signing key",
                code="invalid_oidc_signing_key",
            ) from e
