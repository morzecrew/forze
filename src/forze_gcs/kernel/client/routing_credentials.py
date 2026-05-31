"""Structured secrets for tenant-routed GCS clients."""

from pydantic import BaseModel, Field, model_validator

# ----------------------- #


class GCSRoutingCredentials(BaseModel):
    """JSON shape stored in secrets for :class:`~forze_gcs.kernel.client.RoutedGCSClient`.

    Use with :func:`~forze.application.contracts.secrets.resolve_structured`.
    """

    project_id: str = Field(..., min_length=1)
    service_file: str | None = None
    service_account_json: str | None = None

    @model_validator(mode="after")
    def _one_credential_source(self) -> "GCSRoutingCredentials":
        if self.service_file is not None and self.service_account_json is not None:
            raise ValueError(
                "Specify either service_file or service_account_json, not both.",
            )

        return self
