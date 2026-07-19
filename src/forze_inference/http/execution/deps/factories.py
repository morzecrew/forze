"""Configurable factory building the served-model inference adapter per spec."""

from typing import TYPE_CHECKING, Any, final

import attrs

from forze.application.contracts.inference import InferenceSpec

from ...adapters.inference import HttpInferenceAdapter
from .configs import HttpInferenceConfig
from .keys import InferenceHttpClientDepKey

if TYPE_CHECKING:
    from forze.application.execution import ExecutionContext

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ConfigurableHttpInference:
    """Build an :class:`HttpInferenceAdapter` for a given spec (one factory per route)."""

    config: HttpInferenceConfig

    # ....................... #

    def __call__(
        self,
        ctx: "ExecutionContext",
        spec: InferenceSpec[Any, Any],
    ) -> HttpInferenceAdapter[Any, Any]:
        self.config.validate_against_spec(spec)

        return HttpInferenceAdapter(
            spec=spec,
            client=ctx.deps.provide(InferenceHttpClientDepKey),
            config=self.config,
            protocol=self.config.wire_protocol(),
            tenant_aware=self.config.tenant_aware,
            tenant_provider=ctx.inv_ctx.get_tenant,
        )
