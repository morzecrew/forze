"""Structured secrets for tenant-routed Inngest clients."""

from datetime import timedelta

from pydantic import BaseModel, Field, SecretStr, model_validator

from forze.base.exceptions import exc

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

    # ....................... #

    @model_validator(mode="after")
    def _validate_request_timeout(self) -> "InngestRoutingCredentials":
        if (
            self.request_timeout is not None
            and self.request_timeout.total_seconds() <= 0
        ):
            raise exc.configuration("Request timeout must be positive")

        return self
