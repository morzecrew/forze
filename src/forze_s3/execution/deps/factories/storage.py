"""S3 storage dep factories."""

from typing import final

import attrs

from forze.application.contracts.crypto import KeyringDepKey
from forze.application.contracts.storage import (
    StorageCommandDepPort,
    StorageQueryDepPort,
    StorageSpec,
)
from forze.application.execution import ExecutionContext

from ....adapters import S3StorageAdapter
from ..configs import S3StorageConfig
from ..keys import S3ClientDepKey

# ----------------------- #


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
