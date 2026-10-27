"""DuckDB governed dynamic-read dep factory."""

from typing import TYPE_CHECKING, cast, final

import attrs

from ....adapters.dynamic_read import DuckDbDynamicReadAdapter
from ....kernel.client import DuckDbDynamicReadClientPort
from ..configs import DuckDbDynamicReadConfig
from ..keys import DuckDbClientDepKey

if TYPE_CHECKING:
    from forze.application.contracts.dynamic_read import DynamicReadSpec
    from forze.application.execution.context import ExecutionContext

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurableDuckDbDynamicRead:
    """Build a :class:`DuckDbDynamicReadAdapter` for a dynamic-read route."""

    config: DuckDbDynamicReadConfig
    """DuckDB-specific configuration for the route."""

    # ....................... #

    def __call__(
        self,
        ctx: "ExecutionContext",
        spec: "DynamicReadSpec",
    ) -> DuckDbDynamicReadAdapter:
        # The module wires dynamic reads on a RoutedDuckDbClient only (the origin floor).
        client = cast(DuckDbDynamicReadClientPort, ctx.deps.provide(DuckDbClientDepKey))

        return DuckDbDynamicReadAdapter(
            client=client,
            spec=spec,
            config=self.config,
            statement_timeout=self.config.statement_timeout,
            tenant_aware=self.config.tenant_aware,
            tenant_provider=ctx.inv_ctx.get_tenant,
        )
