from typing import final

import attrs

from forze.application.contracts.storage import StorageDepPort, StorageSpec
from forze.application.execution import ExecutionContext

from ...adapters import GCSStorageAdapter
from .configs import GCSStorageConfig
from .keys import GCSClientDepKey

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurableGCSStorage(StorageDepPort):
    """Configurable GCS storage adapter factory."""

    config: GCSStorageConfig
    """Configuration for the storage route."""

    # ....................... #

    def __call__(self, ctx: ExecutionContext, spec: StorageSpec) -> GCSStorageAdapter:
        client = ctx.deps.provide(GCSClientDepKey)

        return GCSStorageAdapter(
            client=client,
            bucket=self.config.bucket,
            tenant_aware=self.config.tenant_aware,
            tenant_provider=ctx.inv_ctx.get_tenant,
        )
