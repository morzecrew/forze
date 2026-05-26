from typing import Any, final

import attrs

from forze.application.contracts.analytics import AnalyticsSpec
from forze.application.execution import ExecutionContext
from forze.base.exceptions import exc

from ...adapters import BigQueryAnalyticsAdapter
from .configs import BigQueryAnalyticsConfig
from .keys import BigQueryClientDepKey

# ----------------------- #


def validate_bigquery_analytics_config(
    spec: AnalyticsSpec[Any, Any],
    config: BigQueryAnalyticsConfig,
) -> None:
    """Ensure integration config aligns with the kernel :class:`AnalyticsSpec`."""

    spec_keys = set(spec.queries.keys())
    config_keys = set(config["queries"].keys())

    missing = spec_keys - config_keys

    if missing:
        raise exc.configuration(
            f"BigQuery analytics config for route {spec.name!r} is missing query keys: "
            f"{sorted(missing)!r}."
        )

    extra = config_keys - spec_keys

    if extra:
        raise exc.configuration(
            f"BigQuery analytics config for route {spec.name!r} has unknown query keys: "
            f"{sorted(extra)!r}."
        )

    if spec.ingest is not None and not config.get("ingest_table"):
        raise exc.configuration(
            f"BigQuery analytics config for route {spec.name!r} requires ingest_table "
            "when AnalyticsSpec.ingest is set."
        )


# ....................... #


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
        validate_bigquery_analytics_config(spec, self.config)
        client = ctx.deps.provide(BigQueryClientDepKey)

        return BigQueryAnalyticsAdapter(
            client=client,
            spec=spec,
            config=self.config,
        )
