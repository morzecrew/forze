"""GCS storage dep factories."""

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

from ....adapters import GCSStorageAdapter
from ..configs import GCSStorageConfig
from ..keys import GCSClientDepKey

# ----------------------- #


def _build_adapter(
    ctx: ExecutionContext,
    config: GCSStorageConfig,
) -> GCSStorageAdapter:
    client = ctx.deps.provide(GCSClientDepKey)
    cipher = ctx.deps.provide(KeyringDepKey) if config.encrypt else None

    return GCSStorageAdapter(
        client=client,
        bucket_spec=config.bucket,
        tenant_aware=config.tenant_aware,
        tenant_provider=ctx.inv_ctx.get_tenant,
        cipher=cipher,
    )


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurableGCSStorageQuery(StorageQueryDepPort):
    """Configurable GCS storage query adapter factory."""

    config: GCSStorageConfig = attrs.field(
        validator=attrs.validators.instance_of(GCSStorageConfig),
    )
    """Configuration for the storage route."""

    # ....................... #

    def __call__(self, ctx: ExecutionContext, spec: StorageSpec) -> GCSStorageAdapter:
        return _build_adapter(ctx, self.config)


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurableGCSStorageCommand(StorageCommandDepPort):
    """Configurable GCS storage command adapter factory."""

    config: GCSStorageConfig = attrs.field(
        validator=attrs.validators.instance_of(GCSStorageConfig),
    )
    """Configuration for the storage route."""

    # ....................... #

    def __call__(self, ctx: ExecutionContext, spec: StorageSpec) -> GCSStorageAdapter:
        return _build_adapter(ctx, self.config)


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurableGCSStorageUploads(StorageUploadSessionDepPort):
    """Configurable GCS storage multipart upload-session adapter factory."""

    config: GCSStorageConfig = attrs.field(
        validator=attrs.validators.instance_of(GCSStorageConfig),
    )
    """Configuration for the storage route."""

    # ....................... #

    def __call__(self, ctx: ExecutionContext, spec: StorageSpec) -> GCSStorageAdapter:
        return _build_adapter(ctx, self.config)
