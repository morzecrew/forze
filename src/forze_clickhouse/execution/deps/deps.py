from typing import Any, final

import attrs

from forze.application.contracts.analytics import AnalyticsSpec
from forze.application.execution import ExecutionContext
from ...adapters import ClickHouseAnalyticsAdapter
from .configs import ClickHouseAnalyticsConfig
from .keys import ClickHouseClientDepKey

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurableClickHouseAnalytics:
    """Build a :class:`ClickHouseAnalyticsAdapter` for an analytics spec route."""

    config: ClickHouseAnalyticsConfig
    """ClickHouse-specific configuration for the route."""

    # ....................... #

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: AnalyticsSpec[Any, Any],
    ) -> ClickHouseAnalyticsAdapter[Any, Any]:
        self.config.validate_against_spec(spec)
        client = ctx.deps.provide(ClickHouseClientDepKey)
        return ClickHouseAnalyticsAdapter(
            client=client,
            spec=spec,
            config=self.config,
            tenant_provider=ctx.inv_ctx.get_tenant,
        )
