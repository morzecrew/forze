"""Structured secret payload for per-tenant SQS-compatible queue credentials."""

from __future__ import annotations

from pydantic import BaseModel, Field

# ----------------------- #


class SQSRoutingCredentials(BaseModel):
    """JSON shape stored in secrets for :class:`~forze_sqs.kernel.platform.RoutedSQSClient`.

    Use with :func:`~forze.application.contracts.secrets.resolve_structured`.
    """

    endpoint: str = Field(..., min_length=1)
    region_name: str = Field(..., min_length=1)
    access_key_id: str = Field(..., min_length=1)
    secret_access_key: str = Field(..., min_length=1)
