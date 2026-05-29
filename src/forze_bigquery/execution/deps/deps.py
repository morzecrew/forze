from typing import Any, final

import attrs

from forze.application.contracts.analytics import AnalyticsSpec
from forze.application.execution import ExecutionContext
from ...adapters import BigQueryAnalyticsAdapter
from .configs import BigQueryAnalyticsConfig
from .keys import BigQueryClientDepKey

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurableBigQueryAnalytics:
    """Build a :class:`BigQueryAnalyticsAdapter` for an analytics spec route."""

    config: BigQueryAnalyticsConfig
    """BigQuery-specific configuration for the route."""

    # ....................... #

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: AnalyticsSpec[Any, Any],
    ) -> BigQueryAnalyticsAdapter[Any, Any]:
        self.config.validate_against_spec(spec)
        client = ctx.deps.provide(BigQueryClientDepKey)

        return BigQueryAnalyticsAdapter(
            client=client,
            spec=spec,
            config=self.config,
            tenant_provider=ctx.inv_ctx.get_tenant,
        )
