"""Structured secrets for tenant-routed BigQuery clients."""

from pydantic import BaseModel, Field, model_validator

# ----------------------- #


class BigQueryRoutingCredentials(BaseModel):
    """JSON shape stored in secrets for :class:`~forze_bigquery.kernel.client.RoutedBigQueryClient`.

    Use with :func:`~forze.application.contracts.secrets.resolve_structured`.
    """

    project_id: str = Field(..., min_length=1)
    service_file: str | None = None
    """Path to a service account JSON key file."""

    service_account_json: str | None = None
    """Inline service account JSON (mutually exclusive with ``service_file``)."""

    @model_validator(mode="after")
    def _one_credential_source(self) -> "BigQueryRoutingCredentials":
        if self.service_file is not None and self.service_account_json is not None:
            raise ValueError(
                "Specify either service_file or service_account_json, not both.",
            )

        return self
