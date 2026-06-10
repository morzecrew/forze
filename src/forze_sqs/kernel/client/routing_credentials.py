"""Structured secret payload for per-tenant SQS-compatible queue credentials."""

from __future__ import annotations

from pydantic import BaseModel, Field

# ----------------------- #


class SQSRoutingCredentials(BaseModel):
    """JSON shape stored in secrets for :class:`~forze_sqs.kernel.client.RoutedSQSClient`.

    Use with :func:`~forze.application.contracts.secrets.resolve_structured`.

    Explicit static credentials are **required** here (unlike
    :meth:`~forze_sqs.kernel.client.SQSClient.initialize`, which falls back
    to botocore's default credential chain): per-tenant routing isolates
    tenants by credentials, so the process-ambient chain identity would
    defeat the purpose.
    """

    endpoint: str = Field(..., min_length=1)
    region_name: str = Field(..., min_length=1)
    access_key_id: str = Field(..., min_length=1)
    secret_access_key: str = Field(..., min_length=1)
