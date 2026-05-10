from forze_oidc._compat import require_oidc

require_oidc()

# ....................... #

from typing import Any, Protocol, final

import attrs
import jwt
from jwt import PyJWKClient, PyJWKClientError

from forze.base.errors import AuthenticationError

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

    cache_keys: bool = True
    cache_ttl_seconds: int = 300
    timeout: int = 10

    # Non-init field to lazily create the client.
    _client: PyJWKClient | None = attrs.field(default=None, init=False)

    # ....................... #

    def _require_client(self) -> PyJWKClient:
        if self._client is not None:
            return self._client

        self._client = PyJWKClient(
            uri=self.jwks_uri,
            cache_keys=self.cache_keys,
            lifespan=self.cache_ttl_seconds,
            timeout=self.timeout,
        )

        return self._client

    # ....................... #

    def get_signing_key(self, token: str) -> Any:
        try:
            return self._require_client().get_signing_key_from_jwt(token).key

        except (PyJWKClientError, jwt.InvalidTokenError) as exc:
            raise AuthenticationError(
                "Could not resolve OIDC signing key",
                code="invalid_oidc_signing_key",
            ) from exc
