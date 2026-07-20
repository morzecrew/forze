"""Structured secrets for tenant-routed served-model clients."""

from pydantic import BaseModel, Field, SecretStr

from forze.base.primitives.fingerprint import build_routing_fingerprint

# ----------------------- #


class InferenceHttpRoutingCredentials(BaseModel):
    """JSON shape stored in secrets for :class:`RoutedInferenceHttpClient`.

    Explicit per-tenant connection material is the point: ``dedicated`` isolation means a
    tenant's features never reach another tenant's model server, so each tenant carries its
    own endpoint (and its own authorization for it).
    """

    base_url: str = Field(..., min_length=1)
    """Model-serving endpoint for this tenant."""

    headers: dict[str, str] | None = Field(default=None, repr=False)
    """Optional default headers (e.g. authorization). Redacted from ``repr``."""

    bearer_token: SecretStr | None = None
    """Optional bearer token merged into ``Authorization`` when headers omit it."""


# ....................... #


def routing_fingerprint(creds: InferenceHttpRoutingCredentials) -> str:
    """Stable fingerprint for tenant credential rotation.

    Header values can carry credentials (``Authorization``), so they go through the one-way
    secret KDF rather than the fast public hash.
    """

    header_items = [f"{k}:{v}" for k, v in sorted((creds.headers or {}).items())]

    return build_routing_fingerprint(
        public=[creds.base_url],
        secret=[creds.bearer_token, *header_items],
    )


# ....................... #


def credential_headers(creds: InferenceHttpRoutingCredentials) -> dict[str, str]:
    """Default headers for a tenant's client — explicit headers win over the token."""

    headers = dict(creds.headers or {})

    if creds.bearer_token is not None and "Authorization" not in headers:
        headers["Authorization"] = f"Bearer {creds.bearer_token.get_secret_value()}"

    return headers
