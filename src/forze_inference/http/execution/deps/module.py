"""Deps module registering served-model inference routes over one endpoint client."""

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

from ...kernel import InferenceHttpClientPort, RoutedInferenceHttpClient
from .configs import HttpInferenceConfig
from .factories import ConfigurableHttpInference
from .keys import InferenceHttpClientDepKey

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class HttpInferenceDepsModule(DepsModule):
    """Register served-model inference routes: one :class:`HttpInferenceConfig` per route.

    The pre-constructed client is initialized via
    :func:`~forze_inference.http.execution.lifecycle.inference_http_lifecycle_step`.
    """

    client: InferenceHttpClientPort
    """Pre-constructed endpoint client shared by every route."""

    models: StrKeyMapping[HttpInferenceConfig] = attrs.field(
        converter=MappingConverter.to_str_key_frozen,  # type: ignore[misc]
    )
    """Per-route served-model configs, keyed by spec name."""

    required_tenant_isolation: TenantIsolationMode | None = attrs.field(default=None)
    """Minimum tenant isolation this deployment accepts; wiring fails closed below it.

    ``none`` (every tenant scored by one shared model), ``tagged`` (a bound tenant is
    required, still one shared model), ``namespace`` (a per-tenant ``model_name`` resolver —
    a model per tenant behind one endpoint) and ``dedicated`` (a
    :class:`~forze_inference.http.RoutedInferenceHttpClient` — each tenant's own endpoint,
    resolved from its own secret, so features never reach another tenant's server)."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        validate_module_tenancy(
            integration="InferenceHttp",
            client_is_routed=isinstance(self.client, RoutedInferenceHttpClient),
            groups=[
                TenancyRouteGroup(
                    kind="inference",
                    configs=self.models,
                    tenant_aware=lambda cfg: cfg.tenant_aware,
                    namespace_resolver=lambda cfg: cfg.model_name,
                )
            ],
            required_isolation=self.required_tenant_isolation,
            max_supported_isolation="dedicated",
            validation_failed_code="inference_http_tenancy_validation_failed",
        )

    # ....................... #

    def __call__(self) -> Deps:
        return merge_deps(
            routed_from_mapping(
                self.models,
                bindings=[(InferenceDepKey, ConfigurableHttpInference)],
            ),
            plain={InferenceHttpClientDepKey: self.client},
        )
