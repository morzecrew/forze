from typing import Any, final

import attrs

from forze.application.contracts.analytics import AnalyticsSpec
from forze.application.execution import ExecutionContext
from forze.base.errors import CoreError

from ...adapters import ClickHouseAnalyticsAdapter
from .configs import ClickHouseAnalyticsConfig
from .keys import ClickHouseClientDepKey

# ----------------------- #


def validate_clickhouse_analytics_config(
    spec: AnalyticsSpec[Any, Any],
    config: ClickHouseAnalyticsConfig,
) -> None:
    """Ensure integration config aligns with the kernel :class:`AnalyticsSpec`."""

    spec_keys = set(spec.queries.keys())
    config_keys = set(config["queries"].keys())

    missing = spec_keys - config_keys
    if missing:
        raise CoreError(
            f"ClickHouse analytics config for route {spec.name!r} is missing query keys: "
            f"{sorted(missing)!r}."
        )

    extra = config_keys - spec_keys
    if extra:
        raise CoreError(
            f"ClickHouse analytics config for route {spec.name!r} has unknown query keys: "
            f"{sorted(extra)!r}."
        )

    if spec.ingest is not None and not config.get("ingest_table"):
        raise CoreError(
            f"ClickHouse analytics config for route {spec.name!r} requires ingest_table "
            "when AnalyticsSpec.ingest is set."
        )


# ....................... #


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
        validate_clickhouse_analytics_config(spec, self.config)
        client = ctx.deps.provide(ClickHouseClientDepKey)
        return ClickHouseAnalyticsAdapter(
            client=client,
            spec=spec,
            config=self.config,
        )
