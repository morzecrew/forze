"""Structured secrets for tenant-routed Meilisearch clients."""

from pydantic import BaseModel, Field, SecretStr

# ----------------------- #


class MeilisearchRoutingCredentials(BaseModel):
    """JSON shape stored in secrets for :class:`~forze_meilisearch.kernel.client.RoutedMeilisearchClient`.

    Use with :func:`~forze.application.contracts.secrets.resolve_structured`.
    """

    url: str = Field(..., min_length=1)
    api_key: str | SecretStr | None = None
