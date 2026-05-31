"""Redis search result snapshot dep factory."""

from typing import final

import attrs

from forze.application.contracts.search import (
    SearchResultSnapshotPort,
    SearchResultSnapshotSpec,
)
from forze.application.execution import ExecutionContext

from ....adapters import RedisSearchResultSnapshotAdapter
from ..configs import RedisSearchResultSnapshotConfig, RedisUniversalConfig
from ..keys import RedisClientDepKey

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ConfigurableRedisSearchResultSnapshot:
    """Build :class:`RedisSearchResultSnapshotAdapter` from execution context and store spec."""

    config: RedisSearchResultSnapshotConfig | RedisUniversalConfig = attrs.field(
        validator=attrs.validators.instance_of(RedisUniversalConfig),
    )
    """Configuration (namespace, optional tenant)."""

    # ....................... #

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: SearchResultSnapshotSpec,
    ) -> SearchResultSnapshotPort:
        client = ctx.deps.provide(RedisClientDepKey)

        return RedisSearchResultSnapshotAdapter(
            client=client,
            namespace=self.config.namespace,
            default_ttl=spec.ttl,
            default_max_ids=spec.max_ids,
            default_chunk_size=spec.chunk_size,
            tenant_aware=self.config.tenant_aware,
            tenant_provider=ctx.inv_ctx.get_tenant,
        )
