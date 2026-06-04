"""Structured secrets for tenant-routed HTTP clients."""

from pydantic import BaseModel, Field, SecretStr

from forze.base.primitives.fingerprint import (
    combine_fingerprint,
    secret_dedup_fingerprint,
    stable_fingerprint,
)

# ----------------------- #


class HttpRoutingCredentials(BaseModel):
    """JSON shape stored in secrets for :class:`~forze_http.kernel.client.RoutedHttpxClient`.

    Use with :func:`~forze.application.contracts.secrets.resolve_structured_for_tenant`.
    """

    base_url: str = Field(..., min_length=1)
    """Service base URL for the tenant."""

    headers: dict[str, str] | None = None
    """Optional default headers (e.g. authorization)."""

    bearer_token: SecretStr | None = None
    """Optional bearer token merged into ``Authorization`` when headers omit it."""


# ....................... #


def routing_fingerprint(creds: HttpRoutingCredentials) -> str:
    """Stable fingerprint for tenant HTTP credential rotation."""

    header_fp = stable_fingerprint(
        *[f"{k}:{v}" for k, v in sorted((creds.headers or {}).items())],
    )

    return combine_fingerprint(
        stable_fingerprint(creds.base_url, header_fp),
        secret_dedup_fingerprint(creds.bearer_token),
    )
