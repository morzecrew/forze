"""Structured secret payload for per-tenant SageMaker runtime credentials."""

from pydantic import BaseModel, Field, SecretStr

from forze.base.primitives.fingerprint import build_routing_fingerprint

# ----------------------- #


class SageMakerRoutingCredentials(BaseModel):
    """JSON shape stored in secrets for :class:`RoutedSageMakerRuntimeClient`.

    Explicit static credentials are **required** here (unlike
    :meth:`~forze_inference.sagemaker.kernel.client.SageMakerRuntimeClient.initialize`, which
    falls back to botocore's default chain): per-tenant routing isolates tenants *by AWS
    identity*, so a process-ambient chain would defeat the purpose — every tenant would end
    up invoking under the same principal.
    """

    region_name: str = Field(..., min_length=1)
    access_key_id: str = Field(..., min_length=1)
    secret_access_key: SecretStr = Field(..., min_length=1)
    endpoint_url: str | None = None
    """Override URL (VPC endpoints, emulators); ``None`` = the real service."""


# ....................... #


def routing_fingerprint(creds: SageMakerRoutingCredentials) -> str:
    """Stable fingerprint for tenant credential rotation."""

    return build_routing_fingerprint(
        public=[creds.region_name, creds.endpoint_url or "", creds.access_key_id],
        secret=[creds.secret_access_key],
    )
