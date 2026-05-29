"""Postgres analytics dep factory."""

from typing import TYPE_CHECKING, Any, final

import attrs

from ....adapters.analytics import PostgresAnalyticsAdapter
from ..configs import PostgresAnalyticsConfig
from ..keys import PostgresClientDepKey

if TYPE_CHECKING:
    from forze.application.contracts.analytics import AnalyticsSpec
    from forze.application.execution.context import ExecutionContext

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurablePostgresAnalytics:
    """Build a :class:`PostgresAnalyticsAdapter` for an analytics spec route."""

    config: PostgresAnalyticsConfig
    """Postgres-specific configuration for the route."""

    # ....................... #

    def __call__(
        self,
        ctx: "ExecutionContext",
        spec: "AnalyticsSpec[Any, Any]",
    ) -> PostgresAnalyticsAdapter[Any, Any]:
        self.config.validate_against_spec(spec)
        client = ctx.deps.provide(PostgresClientDepKey)
        return PostgresAnalyticsAdapter(
            client=client,
            spec=spec,
            config=self.config,
            tenant_provider=ctx.inv_ctx.get_tenant,
        )
