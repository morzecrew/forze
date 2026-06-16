"""S3 storage dep factories."""

from typing import final

import attrs

from forze.application.contracts.crypto import KeyringDepKey
from forze.application.contracts.storage import (
    StorageCommandDepPort,
    StorageQueryDepPort,
    StorageSpec,
    StorageUploadSessionDepPort,
)
from forze.application.execution import ExecutionContext
from forze.application.integrations.storage.client import ObjectStorageSSE

from ....adapters import S3StorageAdapter
from ..configs import S3ServerSideEncryption, S3StorageConfig
from ..keys import S3ClientDepKey

# ----------------------- #


def _build_sse(config: S3ServerSideEncryption) -> ObjectStorageSSE | None:
    """Translate the route's S3 SSE config to the neutral client descriptor.

    Returns ``None`` when SSE is off (``mode="none"``) so the client sends no
    SSE params (unchanged behavior).
    """

    if config.mode == "none":
        return None

    return ObjectStorageSSE(mode=config.mode, key_id=config.kms_key_id)


# ....................... #


def _build_adapter(
    ctx: ExecutionContext,
    config: S3StorageConfig,
) -> S3StorageAdapter:
    client = ctx.deps.provide(S3ClientDepKey)
    cipher = ctx.deps.provide(KeyringDepKey) if config.encrypt else None

    return S3StorageAdapter(
        client=client,
        bucket_spec=config.bucket,
        tenant_aware=config.tenant_aware,
        tenant_provider=ctx.inv_ctx.get_tenant,
        cipher=cipher,
        sse=_build_sse(config.sse),
    )


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurableS3StorageQuery(StorageQueryDepPort):
    """Configurable S3 storage query adapter."""

    config: S3StorageConfig = attrs.field(
        validator=attrs.validators.instance_of(S3StorageConfig),
    )
    """Configuration for the storage."""

    # ....................... #

    def __call__(self, ctx: ExecutionContext, spec: StorageSpec) -> S3StorageAdapter:
        return _build_adapter(ctx, self.config)


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurableS3StorageCommand(StorageCommandDepPort):
    """Configurable S3 storage command adapter."""

    config: S3StorageConfig = attrs.field(
        validator=attrs.validators.instance_of(S3StorageConfig),
    )
    """Configuration for the storage."""

    # ....................... #

    def __call__(self, ctx: ExecutionContext, spec: StorageSpec) -> S3StorageAdapter:
        return _build_adapter(ctx, self.config)


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurableS3StorageUploads(StorageUploadSessionDepPort):
    """Configurable S3 storage multipart upload-session adapter."""

    config: S3StorageConfig = attrs.field(
        validator=attrs.validators.instance_of(S3StorageConfig),
    )
    """Configuration for the storage."""

    # ....................... #

    def __call__(self, ctx: ExecutionContext, spec: StorageSpec) -> S3StorageAdapter:
        return _build_adapter(ctx, self.config)
