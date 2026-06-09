"""Structured secrets for tenant-routed HTTP clients."""

from pydantic import BaseModel, Field, SecretStr

from forze.base.primitives.fingerprint import build_routing_fingerprint

# ----------------------- #


class HttpRoutingCredentials(BaseModel):
    """JSON shape stored in secrets for :class:`~forze_http.kernel.client.RoutedHttpxClient`.

    Use with :func:`~forze.application.contracts.secrets.resolve_structured_for_tenant`.
    """

    base_url: str = Field(..., min_length=1)
    """Service base URL for the tenant."""

    headers: dict[str, str] | None = Field(default=None, repr=False)
    """Optional default headers (e.g. authorization). Redacted from ``repr``."""

    bearer_token: SecretStr | None = None
    """Optional bearer token merged into ``Authorization`` when headers omit it."""


# ....................... #


def routing_fingerprint(creds: HttpRoutingCredentials) -> str:
    """Stable fingerprint for tenant HTTP credential rotation.

    Header values can carry credentials (e.g. ``Authorization``), so they are
    routed through the one-way secret KDF rather than the fast public hash.
    """

    header_items = [f"{k}:{v}" for k, v in sorted((creds.headers or {}).items())]

    return build_routing_fingerprint(
        public=[creds.base_url],
        secret=[creds.bearer_token, *header_items],
    )
