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
from forze.base.primitives import MappingConverter, StrKeyMapping

from ...kernel import SageMakerRuntimeClientPort
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

    # ....................... #

    def __call__(self) -> Deps:
        return merge_deps(
            routed_from_mapping(
                self.models,
                bindings=[(InferenceDepKey, ConfigurableSageMakerInference)],
            ),
            plain={SageMakerRuntimeClientDepKey: self.client},
        )
