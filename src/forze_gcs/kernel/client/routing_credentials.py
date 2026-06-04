"""Structured secrets for tenant-routed GCS clients."""

from pydantic import BaseModel, Field, model_validator

from forze.base.primitives.fingerprint import (
    combine_fingerprint,
    secret_dedup_fingerprint,
    stable_fingerprint,
)
from forze.base.primitives.owned_temp_path import OwnedTempPath

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


# ....................... #


def routing_credential_dedup_tag(
    *,
    key_file: str | None = None,
    inline_key_json: str | None = None,
) -> str:
    """Return a dedup tag for routed credential sources (never embeds raw JSON)."""

    if key_file:
        return f"file:{key_file}"

    if inline_key_json:
        return f"inline:{secret_dedup_fingerprint(inline_key_json)}"

    return "default-credentials"


def routing_fingerprint(creds: GCSRoutingCredentials) -> str:
    """Stable fingerprint for LRU deduplication."""

    return combine_fingerprint(
        stable_fingerprint(creds.project_id),
        routing_credential_dedup_tag(
            key_file=creds.service_file,
            inline_key_json=creds.service_account_json,
        ),
    )


def credential_file_for_init(
    creds: GCSRoutingCredentials,
    *,
    prefix: str,
) -> OwnedTempPath:
    """Materialize inline key JSON to a temp file when the client needs a path."""

    if creds.service_file:
        return OwnedTempPath.unowned(creds.service_file)

    if not creds.service_account_json:
        return OwnedTempPath.empty()

    return OwnedTempPath.materialize_text(
        creds.service_account_json,
        prefix=prefix,
    )
