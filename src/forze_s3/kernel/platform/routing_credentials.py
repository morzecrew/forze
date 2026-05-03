"""Structured secret payload for per-tenant S3-compatible storage credentials."""

from pydantic import BaseModel, Field

# ----------------------- #


class S3RoutingCredentials(BaseModel):
    """JSON shape stored in secrets for :class:`~forze_s3.kernel.platform.RoutedS3Client`.

    Use with :func:`~forze.application.contracts.secrets.resolve_structured`.
    """

    endpoint: str = Field(..., min_length=1)
    access_key_id: str = Field(..., min_length=1)
    secret_access_key: str = Field(..., min_length=1)
