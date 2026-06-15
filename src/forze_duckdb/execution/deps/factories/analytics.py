"""DuckDB analytics dep factory."""

from typing import Any, final

import attrs

from forze.application.contracts.analytics import AnalyticsSpec
from forze.application.contracts.crypto import (
    DeterministicCipherDepKey,
    KeyringDepKey,
)
from forze.application.execution import ExecutionContext
from forze.application.integrations.analytics import resolve_analytics_codecs_spec

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

        spec = resolve_analytics_codecs_spec(
            spec,
            keyring=(
                ctx.deps.provide(KeyringDepKey)
                if ctx.deps.exists(KeyringDepKey)
                else None
            ),
            deterministic=(
                ctx.deps.provide(DeterministicCipherDepKey)
                if ctx.deps.exists(DeterministicCipherDepKey)
                else None
            ),
            tenant_provider=ctx.inv_ctx.get_tenant,
        )

        return DuckDbAnalyticsAdapter(
            client=client,
            spec=spec,
            config=self.config,
            tenant_provider=ctx.inv_ctx.get_tenant,
        )
