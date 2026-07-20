"""Configurable factory building the SageMaker inference adapter per spec."""

from typing import TYPE_CHECKING, Any, final

import attrs

from forze.application.contracts.inference import InferenceSpec

from ...adapters.inference import SageMakerInferenceAdapter
from .configs import SageMakerInferenceConfig
from .keys import SageMakerRuntimeClientDepKey

if TYPE_CHECKING:
    from forze.application.execution import ExecutionContext

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ConfigurableSageMakerInference:
    """Build a :class:`SageMakerInferenceAdapter` for a given spec (one per route)."""

    config: SageMakerInferenceConfig

    # ....................... #

    def __call__(
        self,
        ctx: "ExecutionContext",
        spec: InferenceSpec[Any, Any],
    ) -> SageMakerInferenceAdapter[Any, Any]:
        return SageMakerInferenceAdapter(
            spec=spec,
            client=ctx.deps.provide(SageMakerRuntimeClientDepKey),
            config=self.config,
            tenant_aware=self.config.tenant_aware,
            tenant_provider=ctx.inv_ctx.get_tenant,
        )
