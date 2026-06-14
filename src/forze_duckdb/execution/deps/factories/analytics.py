"""DuckDB analytics dep factory."""

from typing import Any, final

import attrs

from forze.application.contracts.analytics import AnalyticsSpec
from forze.application.execution import ExecutionContext

from ....adapters import DuckDbAnalyticsAdapter
from ..configs import DuckDbAnalyticsConfig
from ..keys import DuckDbClientDepKey

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurableDuckDbAnalytics:
    """Build a :class:`DuckDbAnalyticsAdapter` for an analytics spec route."""

    config: DuckDbAnalyticsConfig = attrs.field(
        validator=attrs.validators.instance_of(DuckDbAnalyticsConfig),
    )
    """DuckDB-specific configuration for the route."""

    # ....................... #

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: AnalyticsSpec[Any, Any],
    ) -> DuckDbAnalyticsAdapter[Any]:
        self.config.validate_against_spec(spec)
        client = ctx.deps.provide(DuckDbClientDepKey)

        return DuckDbAnalyticsAdapter(
            client=client,
            spec=spec,
            config=self.config,
            tenant_provider=ctx.inv_ctx.get_tenant,
        )
