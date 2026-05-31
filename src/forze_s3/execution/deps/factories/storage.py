"""S3 storage dep factory."""

from typing import final

import attrs

from forze.application.contracts.storage import StorageDepPort, StorageSpec
from forze.application.execution import ExecutionContext

from ....adapters import S3StorageAdapter
from ..configs import S3StorageConfig
from ..keys import S3ClientDepKey

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurableS3Storage(StorageDepPort):
    """Configurable S3 storage adapter."""

    config: S3StorageConfig = attrs.field(
        validator=attrs.validators.instance_of(S3StorageConfig),
    )
    """Configuration for the storage."""

    # ....................... #

    def __call__(self, ctx: ExecutionContext, spec: StorageSpec) -> S3StorageAdapter:
        client = ctx.deps.provide(S3ClientDepKey)

        return S3StorageAdapter(
            client=client,
            bucket_spec=self.config.bucket,
            tenant_aware=self.config.tenant_aware,
            tenant_provider=ctx.inv_ctx.get_tenant,
        )
