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
from forze.base.primitives import MappingConverter, StrKeyMapping

from ...kernel import InferenceHttpClientPort
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

    # ....................... #

    def __call__(self) -> Deps:
        return merge_deps(
            routed_from_mapping(
                self.models,
                bindings=[(InferenceDepKey, ConfigurableHttpInference)],
            ),
            plain={InferenceHttpClientDepKey: self.client},
        )
