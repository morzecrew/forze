"""Deps module registering SageMaker inference routes over one runtime client."""

from typing import final

import attrs

from forze.application.contracts.deps import (
    Deps,
    DepsModule,
    merge_deps,
    routed_from_mapping,
)
from forze.application.contracts.inference import InferenceDepKey
from forze.application.contracts.tenancy import (
    TenancyRouteGroup,
    TenantIsolationMode,
    validate_module_tenancy,
)
from forze.base.primitives import MappingConverter, StrKeyMapping

from ...kernel import RoutedSageMakerRuntimeClient, SageMakerRuntimeClientPort
from .configs import SageMakerInferenceConfig
from .factories import ConfigurableSageMakerInference
from .keys import SageMakerRuntimeClientDepKey

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class SageMakerInferenceDepsModule(DepsModule):
    """Register SageMaker inference routes: one :class:`SageMakerInferenceConfig` per route.

    The pre-constructed client is initialized via
    :func:`~forze_inference.sagemaker.execution.lifecycle.sagemaker_inference_lifecycle_step`.
    """

    client: SageMakerRuntimeClientPort
    """Pre-constructed runtime client shared by every route."""

    models: StrKeyMapping[SageMakerInferenceConfig] = attrs.field(
        converter=MappingConverter.to_str_key_frozen,  # type: ignore[misc]
    )
    """Per-route endpoint configs, keyed by spec name."""

    required_tenant_isolation: TenantIsolationMode | None = attrs.field(default=None)
    """Minimum tenant isolation this deployment accepts; wiring fails closed below it.

    ``none`` (one shared endpoint), ``tagged`` (a bound tenant is required, still one shared
    endpoint), ``namespace`` (a per-tenant ``endpoint_name`` resolver — an endpoint per
    tenant under one AWS identity) and ``dedicated`` (a
    :class:`~forze_inference.sagemaker.RoutedSageMakerRuntimeClient` — each tenant invokes
    under its own AWS credentials, so access is enforced by IAM, not just by endpoint name)."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        validate_module_tenancy(
            integration="SageMakerInference",
            client_is_routed=isinstance(self.client, RoutedSageMakerRuntimeClient),
            groups=[
                TenancyRouteGroup(
                    kind="inference",
                    configs=self.models,
                    tenant_aware=lambda cfg: cfg.tenant_aware,
                    namespace_resolver=lambda cfg: cfg.endpoint_name,
                )
            ],
            required_isolation=self.required_tenant_isolation,
            max_supported_isolation="dedicated",
            validation_failed_code="sagemaker_inference_tenancy_validation_failed",
        )

    # ....................... #

    def __call__(self) -> Deps:
        return merge_deps(
            routed_from_mapping(
                self.models,
                bindings=[(InferenceDepKey, ConfigurableSageMakerInference)],
            ),
            plain={SageMakerRuntimeClientDepKey: self.client},
        )
