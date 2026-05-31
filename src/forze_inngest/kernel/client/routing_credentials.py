"""Structured secrets for tenant-routed Inngest clients."""

from datetime import timedelta

from pydantic import BaseModel, Field, SecretStr

# ----------------------- #


class InngestRoutingCredentials(BaseModel):
    """JSON shape stored in secrets for :class:`~forze_inngest.kernel.client.RoutedInngestClient`.

    Use with :func:`~forze.application.contracts.secrets.resolve_structured`.
    """

    app_id: str = Field(..., min_length=1)
    event_key: str | SecretStr | None = None
    signing_key: str | SecretStr | None = None
    is_production: bool | None = None
    request_timeout: timedelta | None = None
